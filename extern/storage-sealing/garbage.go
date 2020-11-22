package sealing

import (
	"context"
	"golang.org/x/xerrors"
	"github.com/filecoin-project/go-state-types/abi"
	gr "github.com/filecoin-project/sector-storage/go-redis"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/gogo/protobuf/sortkeys"
	"gopkg.in/fatih/set.v0"
	"strings"
	"time"
)

func (m *Sealing) pledgeSector(ctx context.Context, sectorID storage.SectorRef, existingPieceSizes []abi.UnpaddedPieceSize, sizes ...abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
	// 说明
	// 当len(existingPieceSizes)>0 && len(sizes)>0时表明是deal发单后需要填充sector剩余空间。否则是纯垃圾sector
	// 原因参考pledgeSector调用即可明白
	if len(sizes) == 0 {
		return nil, nil
	}

	log.Infof("Pledge %d, contains %+v", sectorID, existingPieceSizes)

	out := make([]abi.PieceInfo, len(sizes))
	//for i, size := range sizes {
	//	ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, NewNullReader(size),"")
	//	if err != nil {
	//		return nil, xerrors.Errorf("add piece: %w", err)
	//	}
	//
	//	existingPieceSizes = append(existingPieceSizes, size)
	//
	//	out[i] = ppi
	//}

	if len(existingPieceSizes) > 0 && len(sizes) > 0 {
		log.Infof("fil deal sector(%+v) with pledge, contains %+v", sectorID, existingPieceSizes)
		for i, size := range sizes {
			log.Infof("========== Range AddPiece %d, sizes %+v, size %+v", sectorID, len(sizes), size)
			//ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, NewNullReader(0), "_filPledgeToDealSector")
			ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, "", "_filPledgeToDealSector", "")
			if err != nil {
				return nil, xerrors.Errorf("add piece: %w", err)
			}

			existingPieceSizes = append(existingPieceSizes, size)

			out[i] = ppi
		}
	} else {
		log.Infof("pure pledge sector(%+v)", sectorID)
		for i, size := range sizes {
			//ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, NewNullReader(0), "_pledgeSector")
			ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, "", "_pledgeSector", "")
			if err != nil {
				return nil, xerrors.Errorf("add piece: %w", err)
			}

			existingPieceSizes = append(existingPieceSizes, size)

			out[i] = ppi
		}
	}

	go func() {
		log.Infof("====== send turnOnCh pledge, sectorID %+v", sectorID.ID.Number)
		m.turnOnCh <- gr.SplicingBackupPubAndParamsField(sectorID.ID.Number, sealtasks.TTAddPiecePl, 0)
	}()

	return out, nil
}

func (m *Sealing) PledgeSector() error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() > cfg.MaxSealingSectors {
			return xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	// 有机会就去尝试消化积压的sector
	m.RecoverPledgeSector()

	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		spt, err := m.currentSealProof(ctx)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		size, err := spt.SectorSize()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		// 恢复garbage sid
		var sid abi.SectorNumber
		m.recoverLk.Lock()
		recoverLen := len(m.recoverSectorNumbers)
		log.Infof("===== PledgeSector need recoverSectorNumber length:%d\n", recoverLen)
		if recoverLen > 0 {
			// 按sectornumber排序,释放sector
			var keys []uint64
			for key, _ := range m.recoverSectorNumbers {
				keys = append(keys, uint64(key))
			}
			sortkeys.Uint64s(keys)
			sid = abi.SectorNumber(keys[0])
			delete(m.recoverSectorNumbers, abi.SectorNumber(keys[0]))

			//for number := range m.recoverSectorNumbers {
			//	delete(m.recoverSectorNumbers, number)
			//	break
			//}
			log.Infof("===== PledgeSector use recoverSectorNumber(%d),after recoverSectorNumber length:%d\n", sid, len(m.recoverSectorNumbers))
		}
		m.recoverLk.Unlock()

		if recoverLen <= 0 {
			sid, err = m.sc.Next()
			if err != nil {
				log.Errorf("%+v", err)
				return
			}
			log.Infof("===== PledgeSector use m.sc.Next (%d),after recoverSectorNumber length:%d\n", sid, len(m.recoverSectorNumbers))
		}

		sectorID := m.minerSector(spt, sid)
		err = m.sealer.NewSector(ctx, sectorID)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		pieces, err := m.pledgeSector(ctx, sectorID, []abi.UnpaddedPieceSize{}, abi.PaddedPieceSize(size).Unpadded())
		if err != nil {
			// 保存pledge garbage的sector
			m.recoverLk.Lock()
			m.recoverSectorNumbers[sid] = struct{}{}
			log.Infof("===== PledgeSector failed collect sectorNumber(%d),after recoverSectorNumber length:%d\n", sid, len(m.recoverSectorNumbers))
			m.recoverLk.Unlock()
			log.Errorf("%+v", err)
			return
		}

		ps := make([]Piece, len(pieces))
		for idx := range ps {
			ps[idx] = Piece{
				Piece:    pieces[idx],
				DealInfo: nil,
			}
		}

		if err := m.newSectorCC(ctx, sid, ps); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}

// 优先恢复recover 从reids中收集到的sector
func (m *Sealing) RecoverPledgeSector() error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() > cfg.MaxSealingSectors {
			return xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	m.recoverPledgeLK.Lock()
	recoverLen := len(m.recoverPledgeSectors)
	m.recoverPledgeLK.Unlock()

	log.Infof("===== RecoverPledgeSector need recoverPledgeSectorNumber length:%d\n", recoverLen)
	if recoverLen > 0 {
		// 循环发送，前期应该会造成不少浪费，但是目的是抓紧消化完所有的这种存量sector
		m.recoverPledgeLK.Lock()
		var keys []abi.SectorNumber
		for key, _ := range m.recoverPledgeSectors {
			keys = append(keys, key)
		}
		m.recoverPledgeLK.Unlock()

		for _, sid := range keys {
			// 循环发送
			go func() {
				ctx := context.TODO() // we can't use the context from command which invokes
				// this, as we run everything here async, and it's cancelled when the
				// command exits
				spt, err := m.currentSealProof(ctx)
				if err != nil {
					log.Errorf("%+v", err)
					return
				}

				size, err := spt.SectorSize()
				if err != nil {
					log.Errorf("%+v", err)
					return
				}

				//size := abi.PaddedPieceSize(m.sealer.SectorSize()).Unpadded()

				pieces, err := m.pledgeSector(ctx, m.minerSector(abi.RegisteredSealProof_StackedDrg32GiBV1, sid), []abi.UnpaddedPieceSize{}, abi.PaddedPieceSize(size).Unpadded())
				if err != nil {
					//log.Infof("===== PledgeSector failed collect sectorNumber(%d),after recoverSectorNumber length:%d\n", sid, len(m.recoverSectorNumbers))
					//log.Warnf("%+v", err)
					return
				}

				ps := make([]Piece, len(pieces))
				for idx := range ps {
					ps[idx] = Piece{
						Piece:    pieces[idx],
						DealInfo: nil,
					}
				}

				// 释放数量
				m.recoverPledgeLK.Lock()
				delete(m.recoverPledgeSectors, sid)
				m.recoverPledgeLK.Unlock()

				if err := m.newSectorCC(ctx, sid, ps); err != nil {
					log.Errorf("%+v", err)
					return
				}
				log.Infof("===== try to recoverPledgeSectorNumber(%d)\n", sid)
			}()
			//log.Infof("===== try to recoverPledgeSectorNumber(%d)\n", sid)
		}
	}

	return nil
}

func (m *Sealing) initRecoverSectorNumber(ctx context.Context) {
	start := time.Now()
	sidMax, err := m.sc.Next()
	if err != nil {
		log.Errorf("===initRecoverSectorNumber get m.sc.Next() sid failed,error:%+v", err)
		return
	}

	sidStart := abi.SectorNumber(0)
	if m.maddr.String() == "f02420" {
		// 因为我们的sectornumber太大了
		log.Infof("miner %s start recoverySectorNumber from 1500000", m.maddr.String())
		sidStart = abi.SectorNumber(1500000)
	}
	sectorsAll := set.New(set.ThreadSafe)
	for i := sidStart; i <= sidMax; i++ {
		sectorsAll.Add(i)
	}

	listSectors, err := m.ListSectors()
	if err != nil {
		log.Errorf("===initRecoverSectorNumber get m.ListSectors() list error:%+v", err)
		return
	}
	sectorsList := set.New(set.ThreadSafe)
	for _, sinfo := range listSectors {
		sectorsList.Add(sinfo.SectorNumber)
	}

	tok, _, err := m.api.ChainHead(ctx)
	if err != nil {
		log.Errorf("===initRecoverSectorNumber-- handlePreCommit1: api error, not proceeding: %+v", err)
		return
	}

	// 属于sectorAll 但是不属于 sectorList的编号
	recover := set.Difference(sectorsAll, sectorsList)
	m.recoverLk.Lock()
	for _, sid := range recover.List() {
		// 过滤sectorNumber是否在链上存在
		_, err := m.api.StateSectorPreCommitInfo(ctx, m.maddr, sid.(abi.SectorNumber), tok)
		if err != nil {
			//log.Warnf("check precommit info: %w ,skip %+v", err, sid.(abi.SectorNumber))
			continue
		}

		// 过滤RcoverPledge中的sector
		_, ok := m.recoverPledgeSectors[sid.(abi.SectorNumber)]
		if ok {
			continue
		}
		m.recoverSectorNumbers[sid.(abi.SectorNumber)] = struct{}{}
	}
	log.Infof("==== recover sector number size %d,cost time %s\n", len(m.recoverSectorNumbers), time.Now().Sub(start))
	log.Infof("==== recover sector number : %+v\n", m.recoverSectorNumbers)
	m.recoverLk.Unlock()

}

func (m *Sealing) SearchRecoveryPledge() map[abi.SectorNumber]struct{} {
	pledgeKeys := make([]gr.RedisField, 0)
	//sealKeys := make([]gr.RedisField, 0)

	allKeyList, _ := m.rc.HKeys(gr.PUB_NAME)
	for _, key := range allKeyList {
		res := strings.Split(string(key), "_")
		switch res[1] {
		case "pledge":
			pledgeKeys = append(pledgeKeys, key)
			//case "seal":
			//	sealKeys = append(sealKeys, key)
		}
	}

	i := 0
	for j := 0; j < len(pledgeKeys); j++ {
		p1k := gr.TypeToType(pledgeKeys[j], gr.FIELDP1)
		p1Exist, err := m.rc.HExist(gr.PUB_NAME, p1k)
		if err != nil {
			log.Errorf("rd search recovery pledge, filter1 err:%+v", err)
		}

		if !p1Exist {
			pledgeKeys[i] = pledgeKeys[j]
			i++
		}
	}
	pledgeKeys = pledgeKeys[:i]

	//i = 0
	//for j := 0; j < len(pledgeKeys); j++ {
	//	hostName := ""
	//	err := m.rc.HGet(gr.PUB_NAME, pledgeKeys[j], &hostName)
	//	if err != nil {
	//		log.Errorf("rd search recovery pledge, filter2 err:%+v", err)
	//	}
	//	ex, _ := m.rc.HExist(gr.SplicingTaskCounntKey(hostName), pledgeKeys[j])
	//
	//	if !ex {
	//		pledgeKeys[i] = pledgeKeys[j]
	//		i++
	//	}
	//}
	//pledgeKeys = pledgeKeys[:i]

	//i = 0
	//for j := 0; j < len(pledgeKeys); j++ {
	//	pledgeRes := &gr.ParamsResAp{}
	//	ex, err := m.rc.HExist(gr.PARAMS_RES_NAME, pledgeKeys[j])
	//	if err != nil {
	//		log.Errorf("rd search recovery pledge, filter3 err:%+v", err)
	//	}
	//
	//	if !ex {
	//		pledgeKeys[i] = pledgeKeys[j]
	//		i++
	//		continue
	//	}
	//
	//	err = m.rc.HGet(gr.PARAMS_RES_NAME, pledgeKeys[j], pledgeRes)
	//	if err != nil {
	//		log.Errorf("rd search recovery pledge, filter4 err:%+v", err)
	//	}
	//
	//	if pledgeRes.Err != "" {
	//		pledgeKeys[i] = pledgeKeys[j]
	//		i++
	//	}
	//}
	//pledgeKeys = pledgeKeys[:i]

	log.Infof("===== rd search recovery pledge, len %d pledgeKeys :%+v", len(pledgeKeys), pledgeKeys)

	sectorList := make(map[abi.SectorNumber]struct{}, 0)
	for _, key := range pledgeKeys {
		sid, _, _, _ := key.TailoredPubAndParamsfield()
		sectorList[sid] = struct{}{}
	}

	return sectorList
}
