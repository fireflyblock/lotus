package sectorstorage

import (
	"context"
	"errors"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/sector-storage/fsutil"
	gr "github.com/filecoin-project/sector-storage/go-redis"
	"github.com/filecoin-project/sector-storage/sealtasks"
	storage2 "github.com/filecoin-project/specs-storage/storage"
	"github.com/go-redis/redis/v8"
	"os"
	"runtime/debug"
	"sync"
)

type RedisWorker struct {
	sealer   *ffiwrapper.Sealer
	redisCli *gr.RedisClient
	hostName string
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
	var rurl = []string{}
	var pw string
	conf, err := InitRequestConfig("conf.json")
	if err != nil {
		log.Errorf("===== read conf.json err:", err)
	}

	if conf.RecordUrl == "" {
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
		sealer:   sb,
		redisCli: rc,
		hostName: hn,
	}
	return redisWorker, err
}

//(rw *RedisWorker)
func (rw *RedisWorker) RegisterWorker(ctx context.Context, path string) (err error) {
	tc := rw.InitWorkerConfig(path)

	tcfKey := gr.SplicingTaskConfigKey(rw.hostName)
	tctKey := gr.SplicingTaskCounntKey(rw.hostName)

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

	count, err := rw.redisCli.Exist(gr.RedisField(tctKey.ToString()))
	if count == 0 {
		rw.redisCli.HSet(tctKey, gr.FIELDPLEDGEP, 0)

		rw.redisCli.HSet(tctKey, gr.FIELDSEAL, 0)

		rw.redisCli.HSet(tctKey, gr.FIELDP1, 0)

		rw.redisCli.HSet(tctKey, gr.FIELDP2, 0)

		rw.redisCli.HSet(tctKey, gr.FIELDC1, 0)
	}

	return nil
}

func (rw *RedisWorker) StartWorker(ctx context.Context, sw *sync.WaitGroup) (err error) {
	//sub task
	subCha, err := rw.redisCli.Subscribe(gr.PUBLISHCHANNEL)
	if err != nil {
		return err
	}

	go rw.SubscribeResult(ctx, subCha, sw)

	return
}

func (rw *RedisWorker) SubscribeResult(ctx context.Context, subCha <-chan *redis.Message, sw *sync.WaitGroup) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("====== SubscribeResult err", err)
			s := debug.Stack()
			log.Error(string(s))
			sw.Done()
		}
	}()

	for msg := range subCha {
		log.Infof("===== Cha %+v 接收到 msg %+v", msg.Channel, msg.Payload)
		pl := gr.RedisField(msg.Payload)
		sid, tt, hostName, sealApId, err := pl.TailoredSubMessage()
		if err != nil {
			log.Errorf("sub tailored err:", err)
		}

		if rw.hostName == hostName {
			//get params res
			resField := gr.SplicingBackupPubAndParamsField(sid, tt.ToOfficalTaskType(), sealApId)
			pubMsg := gr.SplicingPubMessage(sid, tt.ToOfficalTaskType(), hostName, sealApId)

			switch tt.ToOfficalTaskType() {
			case sealtasks.TTAddPiecePl:
				go rw.DealPledge(ctx, resField, pubMsg)
			case sealtasks.TTAddPieceSe:
				go rw.DealSeal(ctx, resField, pubMsg)
			case sealtasks.TTPreCommit1:
				go rw.DealP1(ctx, resField, pubMsg)
			case sealtasks.TTPreCommit2:
				go rw.DealP2(ctx, resField, pubMsg)
			case sealtasks.TTCommit1:
				go rw.DealC1(ctx, resField, pubMsg)
			}
		} else {
			continue
		}
	}
	return
}

func (rw *RedisWorker) DealPledge(ctx context.Context, pubField, pubMessage gr.RedisField) {
	paramsRes := &gr.ParamsResAp{}
	params := &gr.ParamsAp{}
	//get params
	err := rw.redisCli.HGet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		log.Errorf("===== hget ap params err:%+v", err)
		paramsRes = &gr.ParamsResAp{
			PieceInfo: abi.PieceInfo{},
			Err:       err,
		}
		goto RESRETURN
	}
	//do task
	{
		//pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, params.PieceData, params.ApType)
		pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, params.FilePath, params.FileName, params.ApType)
		paramsRes = &gr.ParamsResAp{
			PieceInfo: pi,
			Err:       err,
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
	if err != nil {
		log.Errorf("===== pub ap res err:%+v", err)
	}
}

func (rw *RedisWorker) DealSeal(ctx context.Context, pubField, pubMessage gr.RedisField) {
	paramsRes := &gr.ParamsResAp{}
	params := &gr.ParamsAp{}
	err := rw.redisCli.HGet(gr.PARAMS_NAME, pubField, params)
	if err != nil {
		log.Errorf("===== hget ap params err:%+v", err)
		paramsRes = &gr.ParamsResAp{
			PieceInfo: abi.PieceInfo{},
			Err:       err,
		}
		goto RESRETURN
	}

	{
		//pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, params.PieceData, params.ApType)
		pi, err := rw.sealer.AddPiece(ctx, params.Sector, params.PieceSizes, params.NewPieceSize, params.FilePath, params.FileName, params.ApType)
		paramsRes = &gr.ParamsResAp{
			PieceInfo: pi,
			Err:       err,
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
			Err: err,
		}
		goto RESRETURN
	}

	{
		out, err := rw.sealer.SealPreCommit1(ctx, params.Sector, params.Ticket, params.Pieces, params.Recover)
		paramsRes = &gr.ParamsResP1{
			Out: out,
			Err: err,
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
			Err: err,
		}
		goto RESRETURN
	}

	{
		out, err := rw.sealer.SealPreCommit2(ctx, params.Sector, params.Pc1o)
		paramsRes = &gr.ParamsResP2{
			Out: out,
			Err: err,
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
			Err: err,
		}
		goto RESRETURN
	}

	{
		out, err := rw.sealer.SealCommit1(ctx, params.Sector, params.Ticket, params.Seed, params.Pieces, params.Cids)
		paramsRes = &gr.ParamsResC1{
			Out: out,
			Err: err,
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
	if err != nil {
		log.Errorf("===== pub c1 res err:%+v", err)
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
