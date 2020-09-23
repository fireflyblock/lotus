package sealing

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
)

func (m *Sealing) pledgeSector(ctx context.Context, sectorID abi.SectorID, existingPieceSizes []abi.UnpaddedPieceSize, sizes ...abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
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

	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		size := abi.PaddedPieceSize(m.sealer.SectorSize()).Unpadded()

		sid, err := m.sc.Next()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}
		err = m.sealer.NewSector(ctx, m.minerSector(sid))
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		pieces, err := m.pledgeSector(ctx, m.minerSector(sid), []abi.UnpaddedPieceSize{}, size)
		if err != nil {
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

		if err := m.newSectorCC(sid, ps); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}
