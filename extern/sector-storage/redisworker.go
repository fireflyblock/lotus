package sectorstorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/sector-storage/fsutil"
	gr "github.com/filecoin-project/sector-storage/go-redis"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/sector-storage/transfordata"
	storage2 "github.com/filecoin-project/specs-storage/storage"
	"github.com/go-redis/redis/v8"
	"golang.org/x/xerrors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

type RedisWorker struct {
	sealer     *ffiwrapper.Sealer
	redisCli   *gr.RedisClient
	hostName   string
	workerPath string
}

func NewSealer(sectorSizeInt int64, path string) (*RedisWorker, error) {
	// sector size
	sectorSize := abi.SectorSize(sectorSizeInt)

	spt, err := ffiwrapper.SealProofTypeFromSectorSize(sectorSize)
	if err != nil {
		return nil, err
	}

	cfg := &ffiwrapper.Config{
		SealProofType: spt,
	}

	sbfs := &basicfs.Provider{
		Root: path,
	}

	sb, err := ffiwrapper.New(sbfs, cfg)

	//init redis data
	var rurl string
	var pw string
	conf, err := InitRequestConfig("conf.json")
	if err != nil {
		log.Errorf("===== read conf.json err:", err)
	}

	if conf.RedisUrl == "" {
		rurl = DefaultRedisURL
	} else {
		rurl = conf.RedisUrl
	}

	if conf.PassWord == "" {
		pw = DefaultRedisPassWord
	} else {
		pw = conf.PassWord
	}
	ctx, _ := context.WithCancel(context.Background())
	rc := gr.NewRedisClusterCLi(ctx, rurl, pw)
	if rc == nil {
		return nil, errors.New("new redis cluster client err")
	}
	hn, err := os.Hostname()
	if err != nil {
		log.Errorf("===== get hostname err:", err)
	}

	redisWorker := &RedisWorker{
		sealer:     sb,
		redisCli:   rc,
		hostName:   hn,
		workerPath: path,
	}
	return redisWorker, err
}

//(rw *RedisWorker)
func (rw *RedisWorker) RegisterWorker(ctx context.Context, path string) (err error) {
	tc := rw.InitWorkerConfig(path)

	tcfKey := gr.SplicingTaskConfigKey(rw.hostName)
	err = rw.redisCli.HSet(tcfKey, gr.FIELDPAP, uint64(tc.AddPieceSize))
	if err != nil {
		return
	}
	err = rw.redisCli.HSet(tcfKey, gr.FIELDP1, uint64(tc.Pre1CommitSize))
	if err != nil {
		return
	}
	err = rw.redisCli.HSet(tcfKey, gr.FIELDP2, uint64(tc.Pre2CommitSize))
	if err != nil {
		return
	}
	err = rw.redisCli.HSet(tcfKey, gr.FIELDC1, uint64(tc.Commit1))
	if err != nil {
		return
	}

	return nil
}

func (rw *RedisWorker) RecoveryTask(ctx context.Context) error {
	//range
	keys, err := rw.redisCli.HKeys(gr.RedisKey(rw.hostName))
	if err != nil {
		log.Errorf("===== rd RecoveryTask, hkeys err:", err)
		return err
	}
	log.Infof("===== rd recovery worker, get all keys hostName %s, keys %+v", rw.hostName, keys)
	for _, key := range keys {
		//check task
		err := rw.CheckRecovery()
		if err != nil {

		}
		var res gr.RedisField
		err = rw.redisCli.HGet(gr.RedisKey(rw.hostName), key, &res)
		if err != nil {
			log.Errorf("===== rd RecoveryTask, hget err:", err)
		}

		sid, err := strconv.Atoi(string(key))
		if err != nil {
			log.Error(err)
		}

		switch res {
		case gr.FIELDSEAL:
			name := fmt.Sprintf("%s%d", gr.SEALAPID, sid)
			var id int64
			err = rw.redisCli.Get(gr.RedisKey(name), &id)
			if err != nil {
				log.Errorf("===== rd RecoveryTask, get err:", err)
			}

			sealField := gr.SplicingBackupPubAndParamsField(abi.SectorNumber(sid), res.ToOfficalTaskType(), uint64(id))
			exist, err := rw.redisCli.HExist(gr.PARAMS_RES_NAME, sealField)
			if err != nil {
				log.Errorf("===== rd RecoveryTask, hexist err:", err)
			}
			if exist {
				continue
			}

			log.Infof("===== rd recovery worker, check seal hostName %s, sectorID %s, taskType %s, sealField %s, exist %+v", rw.hostName, key, res, sealField, exist)
			pubMsg := gr.SplicingPubMessage(abi.SectorNumber(sid), res.ToOfficalTaskType(), rw.hostName, 0)
			go rw.DealSeal(ctx, sealField, pubMsg)

		case gr.FIELDPLEDGEP:
			pledgeField := gr.SplicingBackupPubAndParamsField(abi.SectorNumber(sid), res.ToOfficalTaskType(), 0)
			exist, err := rw.redisCli.HExist(gr.PARAMS_RES_NAME, pledgeField)
			if err != nil {
				log.Errorf("===== rd RecoveryTask, hexist err:", err)
			}

			if exist {
				continue
			}

			log.Infof("===== rd recovery worker, check pledge hostName %s, sectorID %s, taskType %s, exist %+v", rw.hostName, key, res, exist)
			pubMsg := gr.SplicingPubMessage(abi.SectorNumber(sid), res.ToOfficalTaskType(), rw.hostName, 0)
			go rw.DealPledge(ctx, pledgeField, pubMsg)

		case gr.FIELDP1:
			p1Field := gr.SplicingBackupPubAndParamsField(abi.SectorNumber(sid), res.ToOfficalTaskType(), 0)
			exist, err := rw.redisCli.HExist(gr.PARAMS_RES_NAME, p1Field)
			if err != nil {
				log.Errorf("===== rd RecoveryTask, hexist err:", err)
			}
			if exist {
				continue
			}

			log.Infof("===== rd recovery worker, check p1 hostName %s, sectorID %s, taskType %s, exist %+v", rw.hostName, key, res, exist)
			pubMsg := gr.SplicingPubMessage(abi.SectorNumber(sid), res.ToOfficalTaskType(), rw.hostName, 0)
			go rw.DealP1(ctx, p1Field, pubMsg)

		case gr.FIELDP2:
			p2Field := gr.SplicingBackupPubAndParamsField(abi.SectorNumber(sid), res.ToOfficalTaskType(), 0)
			exist, err := rw.redisCli.HExist(gr.PARAMS_RES_NAME, p2Field)
			if err != nil {
				log.Errorf("===== rd RecoveryTask, hexist err:", err)
			}
			if exist {
				continue
			}

			log.Infof("===== rd recovery worker, check p2 hostName %s, sectorID %s, taskType %s, exist %+v", rw.hostName, key, res, exist)
			pubMsg := gr.SplicingPubMessage(abi.SectorNumber(sid), res.ToOfficalTaskType(), rw.hostName, 0)
			go rw.DealP2(ctx, p2Field, pubMsg)

		case gr.FIELDC1:
			c1Field := gr.SplicingBackupPubAndParamsField(abi.SectorNumber(sid), res.ToOfficalTaskType(), 0)
			exist, err := rw.redisCli.HExist(gr.PARAMS_RES_NAME, c1Field)
			if err != nil {
				log.Errorf("===== rd RecoveryTask, hexist err:", err)
			}
			if exist {
				continue
			}

			log.Infof("===== rd recovery worker, check c1 hostName %s, sectorID %s, taskType %s, exist %+v", rw.hostName, key, res, exist)
			pubMsg := gr.SplicingPubMessage(abi.SectorNumber(sid), res.ToOfficalTaskType(), rw.hostName, 0)
			go rw.DealC1(ctx, c1Field, pubMsg)
		}
	}
	return nil
}

func (rw *RedisWorker) CheckRecovery() error {
	//range
	keys, err := rw.redisCli.HKeys(gr.RedisKey(rw.hostName))
	if err != nil {
		log.Errorf("===== rd RecoveryTask, hkeys err:", err)
		return err
	}
	log.Infof("===== rd check recovery worker, get all keys hostName %s, keys %+v", rw.hostName, keys)
	for _, key := range keys {
		//check task
		var taskType gr.RedisField
		err = rw.redisCli.HGet(gr.RedisKey(rw.hostName), key, &taskType)
		if err != nil {
			log.Errorf("===== rd RecoveryTask, hget err:", err)
			return err
		}

		sid, err := strconv.Atoi(string(key))
		if err != nil {
			log.Error(err)
		}

		var id int64
		if taskType == gr.FIELDSEAL {
			name := fmt.Sprintf("%s%d", gr.SEALAPID, sid)
			err = rw.redisCli.Get(gr.RedisKey(name), &id)
			if err != nil {
				log.Errorf("===== rd RecoveryTask, get err:", err)
			}
		}

		field := gr.SplicingBackupPubAndParamsField(abi.SectorNumber(sid), taskType.ToOfficalTaskType(), uint64(id))
		exist, err := rw.redisCli.HExist(gr.PUB_NAME, field)
		if err != nil {
			log.Errorf("===== rd RecoveryTask, hexist err:", err)
			return err
		}

		if !exist {
			rw.redisCli.HDel(gr.RedisKey(rw.hostName), field)
			return nil
		}
	}
	return nil
}

func (rw *RedisWorker) UpdateStatus(sectorID abi.SectorNumber, taskType sealtasks.TaskType) error {
	return rw.redisCli.HSet(gr.RedisKey(rw.hostName), gr.RedisField(strconv.Itoa(int(sectorID))), gr.ToFieldTaskType(taskType))
}

func (rw *RedisWorker) StartWorker(ctx context.Context, sw *sync.WaitGroup) (err error) {
	//sub task
	subChaAp, err := rw.redisCli.Subscribe(gr.PUBLISHCHANNELAP)
	if err != nil {
		return err
	}
	subChaP1, err := rw.redisCli.Subscribe(gr.PUBLISHCHANNELP1)
	if err != nil {
		return err
	}
	subChaP2, err := rw.redisCli.Subscribe(gr.PUBLISHCHANNELP2)
	if err != nil {
		return err
	}
	subChaC1, err := rw.redisCli.Subscribe(gr.PUBLISHCHANNELC1)
	if err != nil {
		return err
	}

	go rw.SubscribeResult(ctx, subChaAp, subChaP1, subChaP2, subChaC1, sw)

	return
}

func (rw *RedisWorker) SubscribeResult(ctx context.Context, subChaAp, subChaP1, subChaP2, subChaC1 <-chan *redis.Message, sw *sync.WaitGroup) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("====== SubscribeResult err", err)
			s := debug.Stack()
			log.Error(string(s))
			sw.Done()
		}
	}()

	for {
		select {
		case msg := <-subChaAp:
			log.Infof("===== Cha %+v 接收到 ap msg %+v", msg.Channel, msg.Payload)
			pl := gr.RedisField(msg.Payload)
			sid, tt, hostName, sealApId, err := pl.TailoredSubMessage()
			if err != nil {
				log.Errorf("sub tailored err:", err)
			}

			if rw.hostName == hostName {
				log.Infof("===== rd subscribe task, Cha %+v msg %+v sectorID %+v taskType %+v", msg.Channel, msg.Payload, sid, tt)
				//get params res
				resField := gr.SplicingBackupPubAndParamsField(sid, tt.ToOfficalTaskType(), sealApId)
				pubMsg := gr.SplicingPubMessage(sid, tt.ToOfficalTaskType(), hostName, sealApId)
				switch tt.ToOfficalTaskType() {
				case sealtasks.TTAddPiecePl:
					err = rw.UpdateStatus(sid, tt.ToOfficalTaskType())
					if err != nil {
						log.Errorf("===== rd update status err", err)
					}
					go rw.DealPledge(ctx, resField, pubMsg)
				case sealtasks.TTAddPieceSe:
					err = rw.UpdateStatus(sid, tt.ToOfficalTaskType())
					if err != nil {
						log.Errorf("===== rd update status err", err)
					}
					go rw.DealSeal(ctx, resField, pubMsg)
				}
			} else {
				continue
			}
		case msg := <-subChaP1:
			log.Infof("===== Cha %+v 接收到 p1 msg %+v", msg.Channel, msg.Payload)
			pl := gr.RedisField(msg.Payload)
			sid, tt, hostName, sealApId, err := pl.TailoredSubMessage()
			if err != nil {
				log.Errorf("sub tailored err:", err)
			}

			if rw.hostName == hostName {
				log.Infof("===== rd subscribe task, Cha %+v msg %+v sectorID %+v taskType %+v", msg.Channel, msg.Payload, sid, tt)
				//get params res
				resField := gr.SplicingBackupPubAndParamsField(sid, tt.ToOfficalTaskType(), sealApId)
				pubMsg := gr.SplicingPubMessage(sid, tt.ToOfficalTaskType(), hostName, sealApId)
				err = rw.UpdateStatus(sid, tt.ToOfficalTaskType())
				if err != nil {
					log.Errorf("===== rd update status err", err)
				}
				go rw.DealP1(ctx, resField, pubMsg)
			} else {
				continue
			}
		case msg := <-subChaP2:
			log.Infof("===== Cha %+v 接收到 p2 msg %+v", msg.Channel, msg.Payload)
			pl := gr.RedisField(msg.Payload)
			sid, tt, hostName, sealApId, err := pl.TailoredSubMessage()
			if err != nil {
				log.Errorf("sub tailored err:", err)
			}

			if rw.hostName == hostName {
				log.Infof("===== rd subscribe task, Cha %+v msg %+v sectorID %+v taskType %+v", msg.Channel, msg.Payload, sid, tt)
				//get params res
				resField := gr.SplicingBackupPubAndParamsField(sid, tt.ToOfficalTaskType(), sealApId)
				pubMsg := gr.SplicingPubMessage(sid, tt.ToOfficalTaskType(), hostName, sealApId)
				err = rw.UpdateStatus(sid, tt.ToOfficalTaskType())
				if err != nil {
					log.Errorf("===== rd update status err", err)
				}
				go rw.DealP2(ctx, resField, pubMsg)
			} else {
				continue
			}

		case msg := <-subChaC1:
			log.Infof("===== Cha %+v 接收到 c1 msg %+v", msg.Channel, msg.Payload)
			pl := gr.RedisField(msg.Payload)
			sid, tt, hostName, sealApId, err := pl.TailoredSubMessage()
			if err != nil {
				log.Errorf("sub tailored err:", err)
			}

			if rw.hostName == hostName {
				log.Infof("===== rd subscribe task, Cha %+v msg %+v sectorID %+v taskType %+v", msg.Channel, msg.Payload, sid, tt)
				//get params res
				resField := gr.SplicingBackupPubAndParamsField(sid, tt.ToOfficalTaskType(), sealApId)
				pubMsg := gr.SplicingPubMessage(sid, tt.ToOfficalTaskType(), hostName, sealApId)
				err = rw.UpdateStatus(sid, tt.ToOfficalTaskType())
				if err != nil {
					log.Errorf("===== rd update status err", err)
				}
				go rw.DealC1(ctx, resField, pubMsg)
			} else {
				continue
			}
		}
	}
}

func (rw *RedisWorker) DealPledge(ctx context.Context, pubField, pubMessage gr.RedisField) {
	paramsRes := &gr.ParamsResAp{}
	params := &gr.ParamsAp{}
	//get params
	err := rw.redisCli.HGet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		log.Errorf("===== hget pledge params err:%+v", err)
		paramsRes = &gr.ParamsResAp{
			PieceInfo: abi.PieceInfo{},
			Err:       err.Error(),
		}
		goto RESRETURN
	}
	//do task
	{
		//data := rw.pledgeReader(params.NewPieceSize)
		//pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, data, params.ApType)
		pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, params.FilePath, params.FileName, params.ApType)
		log.Infof("===== rd pledge, sectorID %+v err %+v", params.Sector.Number, err)
		if err != nil {
			paramsRes = &gr.ParamsResAp{
				PieceInfo: pi,
				Err:       err.Error(),
			}
		} else {
			paramsRes = &gr.ParamsResAp{
				PieceInfo: pi,
				Err:       "",
			}
		}
	}

RESRETURN:
	//back params-res
	err = rw.redisCli.HSet(gr.PARAMS_RES_NAME, pubField, paramsRes)
	if err != nil {
		log.Errorf("===== hset ap params_res err:%+v", err)
	}

	//back pub-res
	err = rw.redisCli.HSet(gr.PUB_RES_NAME, pubField, rw.hostName)
	if err != nil {
		log.Errorf("===== hset ap pub_res err:%+v", err)
	}

	//publish res
	_, err = rw.redisCli.Publish(gr.SUBSCRIBECHANNEL, pubMessage)
	log.Infof("===== rd publish task, Cha %+v msg %+v\n", gr.SUBSCRIBECHANNEL, pubMessage)
	if err != nil {
		log.Errorf("===== pub ap res err:%+v", err)
	}
}

func (rw *RedisWorker) DealSeal(ctx context.Context, pubField, pubMessage gr.RedisField) {
	paramsRes := &gr.ParamsResAp{}
	params := &gr.ParamsAp{}
	err := rw.redisCli.HGet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		log.Errorf("===== hget seal params err:%+v", err)
		paramsRes = &gr.ParamsResAp{
			PieceInfo: abi.PieceInfo{},
			Err:       err.Error(),
		}
		goto RESRETURN
	}

	{
		//todo addpiece seal transfer file (io.reader)
		//pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, params.PieceData, params.ApType)
		pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, params.FilePath, params.FileName, params.ApType)
		log.Infof("===== rd seal, sectorID %+v err %+v", params.Sector.Number, err)
		if err != nil {
			paramsRes = &gr.ParamsResAp{
				PieceInfo: pi,
				Err:       err.Error(),
			}
		} else {
			paramsRes = &gr.ParamsResAp{
				PieceInfo: pi,
				Err:       "",
			}
		}
	}

RESRETURN:
	//back params-res
	err = rw.redisCli.HSet(gr.PARAMS_RES_NAME, pubField, paramsRes)
	if err != nil {
		log.Errorf("===== hget seal params err:%+v", err)
	}

	//back pub-res
	err = rw.redisCli.HSet(gr.PUB_RES_NAME, pubField, rw.hostName)
	if err != nil {
		log.Errorf("===== hset seal pub_res err:%+v", err)
	}

	//publish res
	_, err = rw.redisCli.Publish(gr.SUBSCRIBECHANNEL, pubMessage)
	log.Infof("===== rd publish task, Cha %+v msg %+v\n", gr.SUBSCRIBECHANNEL, pubMessage)
	if err != nil {
		log.Errorf("===== pub seal res err:%+v", err)
	}
}

func (rw *RedisWorker) DealP1(ctx context.Context, pubField, pubMessage gr.RedisField) {
	paramsRes := &gr.ParamsResP1{}
	params := &gr.ParamsP1{}
	err := rw.redisCli.HGet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		log.Errorf("===== hget p1 params err:%+v", err)
		paramsRes = &gr.ParamsResP1{
			Out: nil,
			Err: err.Error(),
		}
		goto RESRETURN
	}

	{
		out, err := rw.sealer.SealPreCommit1(ctx, params.Sector, params.Ticket, params.Pieces, params.Recover)
		log.Infof("===== rd p1, sectorID %+v err %+v", params.Sector.Number, err)
		if err != nil {
			paramsRes = &gr.ParamsResP1{
				Out: out,
				Err: err.Error(),
			}
		} else {
			paramsRes = &gr.ParamsResP1{
				Out: out,
				Err: "",
			}
		}
	}

RESRETURN:
	//back params-res
	err = rw.redisCli.HSet(gr.PARAMS_RES_NAME, pubField, paramsRes)
	if err != nil {
		log.Errorf("===== hset p1 params_res err:%+v", err)
	}

	//back pub-res
	err = rw.redisCli.HSet(gr.PUB_RES_NAME, pubField, rw.hostName)
	if err != nil {
		log.Errorf("===== hset p1 pub_res err:%+v", err)
	}

	//publish res
	_, err = rw.redisCli.Publish(gr.SUBSCRIBECHANNEL, pubMessage)
	log.Infof("===== rd publish task, Cha %+v msg %+v\n", gr.SUBSCRIBECHANNEL, pubMessage)
	if err != nil {
		log.Errorf("===== pub p1 res err:%+v", err)
	}
}

func (rw *RedisWorker) DealP2(ctx context.Context, pubField, pubMessage gr.RedisField) {
	paramsRes := &gr.ParamsResP2{}
	params := &gr.ParamsP2{}
	err := rw.redisCli.HGet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		log.Errorf("===== hget p2 params err:%+v", err)
		paramsRes = &gr.ParamsResP2{
			Out: storage2.SectorCids{},
			Err: err.Error(),
		}
		goto RESRETURN
	}

	{
		out, err := rw.sealer.SealPreCommit2(ctx, params.Sector, params.Pc1o)
		log.Infof("===== rd p2, sectorID %+v err %+v", params.Sector.Number, err)
		if err != nil {
			paramsRes = &gr.ParamsResP2{
				Out: out,
				Err: err.Error(),
			}
		} else {
			paramsRes = &gr.ParamsResP2{
				Out: out,
				Err: "",
			}
		}
	}

RESRETURN:
	//back params-res
	err = rw.redisCli.HSet(gr.PARAMS_RES_NAME, pubField, paramsRes)
	if err != nil {
		log.Errorf("===== hset p2 params_res err:%+v", err)
	}

	//back pub-res
	err = rw.redisCli.HSet(gr.PUB_RES_NAME, pubField, rw.hostName)
	if err != nil {
		log.Errorf("===== hset p2 pub_res err:%+v", err)
	}

	//publish res
	_, err = rw.redisCli.Publish(gr.SUBSCRIBECHANNEL, pubMessage)
	log.Infof("===== rd publish task, Cha %+v msg %+v\n", gr.SUBSCRIBECHANNEL, pubMessage)
	if err != nil {
		log.Errorf("===== pub p2 res err:%+v", err)
	}
}

func (rw *RedisWorker) DealC1(ctx context.Context, pubField, pubMessage gr.RedisField) {
	paramsRes := &gr.ParamsResC1{}
	params := &gr.ParamsC1{}
	err := rw.redisCli.HGet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		log.Errorf("===== hget c1 params err:%+v", err)
		paramsRes = &gr.ParamsResC1{
			Out: nil,
			Err: err.Error(),
		}
		goto RESRETURN
	}

	{
		out, err := rw.sealer.SealCommit1(ctx, params.Sector, params.Ticket, params.Seed, params.Pieces, params.Cids)
		log.Infof("===== rd c1, sectorID %+v err %+v", params.Sector.Number, err)
		if err != nil {
			paramsRes = &gr.ParamsResC1{
				Out: out,
				Err: err.Error(),
			}
		} else {
			paramsRes = &gr.ParamsResC1{
				Out: out,
				Err: "",
			}
		}
	}

	// 传输文件
	{
		// 判断是否是deal 的sector，如果是，则存储unseal的文件，否则不存unsealed文件
		exist, err := rw.redisCli.HExist(gr.PUB_NAME, gr.SplicingBackupPubAndParamsField(params.Sector.Number, sealtasks.TTAddPieceSe, 1))
		if err != nil {
			log.Errorf("===== sector(%+v) c1 finished , check sector is deal or not err:%+v", params.Sector, err)
			paramsRes.StoragePath = ""
			goto RESRETURN
		}

		isTransforSuccess := false
		// TODO 不传输完成不罢休,
		for {
			if isTransforSuccess {
				break
			}
			if exist {
				// deal sector 传输unseal文件
				for _, dest := range params.PathList {
					err := rw.TransforDataToStorageServer(ctx, params.Sector, dest, false)
					if err != nil {
						log.Warnf("sector(%+v) c1 transfor data to %s failed", params.Sector, dest)
						continue
					}
					paramsRes.StoragePath = dest
					isTransforSuccess = true
					break
				}
			} else {
				// not deal sector 不传输unseal文件
				for _, dest := range params.PathList {
					err := rw.TransforDataToStorageServer(ctx, params.Sector, dest, true)
					if err != nil {
						log.Warnf("sector(%+v) c1 transfor data to %s failed", dest)
						continue
					}
					paramsRes.StoragePath = dest
					isTransforSuccess = true
					break
				}
			}
			// 遍历一轮还没有传输成功，则sleep 5 min
			time.Sleep(time.Minute * 5)
		}
	}

RESRETURN:
	//back params-res
	err = rw.redisCli.HSet(gr.PARAMS_RES_NAME, pubField, paramsRes)
	if err != nil {
		log.Errorf("===== hset c1 params_res err:%+v", err)
	}

	//back pub-res
	err = rw.redisCli.HSet(gr.PUB_RES_NAME, pubField, rw.hostName)
	if err != nil {
		log.Errorf("===== hset c1 pub_res err:%+v", err)
	}

	//publish res
	_, err = rw.redisCli.Publish(gr.SUBSCRIBECHANNEL, pubMessage)
	log.Infof("===== rd publish task, Cha %+v msg %+v\n", gr.SUBSCRIBECHANNEL, pubMessage)
	if err != nil {
		log.Errorf("===== pub c1 res err:%+v", err)
	}
}

func (rw *RedisWorker) TransforDataToStorageServer(ctx context.Context, sector abi.SectorID, dest string, removeUnseal bool) error {

	// 尝试 开始发送 通过解析p.local来获取NFS ip
	ip, destPath := transfordata.PareseDestFromePath(dest)
	if ip == "" || destPath == "" {

		log.Infof("===== try to send sector [%v], but ip(%s),destPath(%s) is empty", sector, ip, destPath)
		return xerrors.Errorf("")
	}

	// 删除数据,完成传输文件
	log.Infof("===== after finished SealCommit1 for sector [%v], delete local layer,tree-c,tree-d files...", sector)
	rw.RemoveLayersAndTreeCAndD(ctx, sector, removeUnseal)

	start := time.Now()
	// send FTSealed
	srcSealedPath := filepath.Join(rw.workerPath, stores.FTSealed.String()) + "/"
	src := stores.SectorName(sector)
	sealedPath := filepath.Join(destPath, stores.FTSealed.String()) + "/"
	log.Infof("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v)", sector, srcSealedPath, src, ip, sealedPath)
	code, err := transfordata.SendFile(srcSealedPath, src, sealedPath, ip)
	if err != nil {
		if code == 300 {
			log.Infof("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v),failed target ip had too many task", sector, srcSealedPath, src, ip, sealedPath)
		}
		return err
	}

	// send FTCache
	srcCachePath := filepath.Join(rw.workerPath, stores.FTCache.String()) + "/"
	cachePath := filepath.Join(destPath, stores.FTCache.String()) + "/"
	//src:=SectorName(sector)
	log.Infof("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v)", sector, srcCachePath, src, ip, cachePath)
	err = transfordata.SendZipFile(srcCachePath, src, cachePath, ip)
	if err != nil {
		log.Errorf("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v),error:%+v", sector, srcCachePath, src, ip, cachePath, err)
		return err
	}
	log.Infof("===== transfor sector(%+v) to Storage(%+v) cost time %s", sector, destPath, time.Now().Sub(start))

	if !removeUnseal {
		// send FTUnseal
		srcUnsealPath := filepath.Join(rw.workerPath, stores.FTUnsealed.String()) + "/"
		unsealPath := filepath.Join(destPath, stores.FTUnsealed.String()) + "/"
		//src:=SectorName(sector)
		log.Infof("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v)", sector, srcUnsealPath, src, ip, unsealPath)
		code, err = transfordata.SendFile(srcUnsealPath, src, unsealPath, ip)
		if err != nil {
			if code == 300 {
				log.Infow("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v),failed target ip had too many task????", sector, srcSealedPath, src, ip, sealedPath)
			}
			log.Errorf("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v),error:%+v", sector, srcUnsealPath, src, ip, unsealPath, err)
			return err
		}

		// 删除unsealed文件
		err = os.RemoveAll(srcUnsealPath + src)
		if err != nil {
			log.Warnf("===== transfor sector(%+v) to Storage(%+v) success, but remove %s error", sector, unsealPath, srcUnsealPath)
		}
	}

	// 删除sealed文件
	err = os.Remove(srcSealedPath + src)
	if err != nil {
		log.Warnf("===== transfor sector(%+v) to Storage(%+v) success, but remove %s error", sector, sealedPath, srcSealedPath)
		//panic(err)
	}

	// 删除cache文件
	err = os.RemoveAll(srcCachePath + src)
	if err != nil {
		log.Warnf("===== transfor sector(%+v) to Storage(%+v) success, but remove %s error", sector, cachePath, srcCachePath)
		//panic(err)
	}

	return nil
}

func getLayersAndTreeCAndTreeDFiles(url string, proofType abi.RegisteredSealProof) []string {
	layerLabel := "/sc-02-data-layer-"
	treeCLabel := "/sc-02-data-tree-c"
	treeD := "/sc-02-data-tree-d.dat"
	tailLabel := ".dat"
	var files []string

	if proofType == abi.RegisteredSealProof_StackedDrg2KiBV1 || proofType == abi.RegisteredSealProof_StackedDrg512MiBV1 {
		files = append(files, url+layerLabel+"1"+tailLabel)
		files = append(files, url+layerLabel+"2"+tailLabel)
		files = append(files, url+treeD)
		files = append(files, url+treeCLabel+tailLabel)
	} else if proofType == abi.RegisteredSealProof_StackedDrg32GiBV1 {
		for i := 0; i < 12; i++ {
			files = append(files, url+layerLabel+strconv.Itoa(i)+tailLabel)
		}
		for j := 0; j < 8; j++ {
			files = append(files, url+treeCLabel+"-"+strconv.Itoa(j)+tailLabel)
		}
		files = append(files, url+treeD)
	}
	return files
}

// 删除cache中的临时文件
func (rw *RedisWorker) RemoveLayersAndTreeCAndD(ctx context.Context, sector abi.SectorID, removeUnseal bool) {
	log.Infof("===== try to remove sector(%+v) from worker", sector)
	spath := filepath.Join(rw.workerPath, stores.FTCache.String(), stores.SectorName(sector))

	files := getLayersAndTreeCAndTreeDFiles(spath, rw.sealer.SealProofType())
	log.Infof("===== try to remove sector [%+v] from worker--------->delete files:[%+v]", sector, files)
	for _, file := range files {
		log.Infof("remove %s", file)

		if err := os.RemoveAll(file); err != nil {
			log.Errorf("removing sector (%v) from %s: %+v", sector, spath, err)
		}
	}

	// 是否删除Unseal文件
	if removeUnseal {
		// delete unseal
		spath = filepath.Join(rw.workerPath, stores.FTUnsealed.String(), stores.SectorName(sector))
		log.Infof("remove %s", spath)
		if err := os.RemoveAll(spath); err != nil {
			log.Errorf("removing sector (%v) from %s: %+v", sector, spath, err)
		}
	}
}

func (rw *RedisWorker) InitWorkerConfig(path string) (tc *TaskConfig) {
	fsst, err := fsutil.Statfs(path)
	if err != nil {
		log.Errorf("GetWorkerTaskCount get disk info err:%+v", err)
		return
	}

	diskSize := fsst.Capacity

	info, err := info(context.TODO())
	if err != nil {
		log.Errorf("GetWorkerTaskCount get source info err:%+v", err)
		return
	}
	return CalculateResources(info.Resources, diskSize)
}

type Reader struct{}

func (Reader) Read(out []byte) (int, error) {
	for i := range out {
		out[i] = 0
	}
	return len(out), nil
}

func (rw *RedisWorker) pledgeReader(size abi.UnpaddedPieceSize) io.Reader {
	return io.LimitReader(Reader{}, int64(size))
}
