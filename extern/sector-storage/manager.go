package sectorstorage

import (
	"context"
	"errors"
	"fmt"
	minertype "github.com/filecoin-project/lotus/firefly/miner-type"
	gr "github.com/filecoin-project/sector-storage/go-redis"
	"github.com/filecoin-project/sector-storage/transfordata"
	"io"
	"net/http"
	"sync"
	"time"

	logrus "github.com/filecoin-project/sector-storage/log"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/fsutil"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/sector-storage/storiface"
)

var log = logging.Logger("advmgr")

var ErrNoWorkers = errors.New("no suitable workers found")

type URLs []string

type Worker interface {
	ffiwrapper.StorageSealer

	MoveStorage(ctx context.Context, sector storage.SectorRef, types storiface.SectorFileType) error

	// FetchRealData get real useful data
	//FetchRealData(ctx context.Context, id storage.SectorRef) error

	Fetch(ctx context.Context, s storage.SectorRef, ft storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) error
	UnsealPiece(context.Context, storage.SectorRef, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error
	ReadPiece(context.Context, io.Writer, storage.SectorRef, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize) (bool, error)

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

	// Returns paths accessible to the worker
	Paths(context.Context) ([]stores.StoragePath, error)

	Info(context.Context) (storiface.WorkerInfo, error)

	// returns channel signalling worker shutdown
	Closing(context.Context) (<-chan struct{}, error)

	// worker push sealed data to Storage
	//PushDataToStorage(ctx context.Context, sid abi.SectorID, dest string) error

	// init get worker bind sectors
	//GetBindSectors(ctx context.Context) ([]abi.SectorID, error)

	Close() error
}

type SectorManager interface {
	ReadPiece(context.Context, io.Writer, storage.SectorRef, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error

	ffiwrapper.StorageSealer
	storage.Prover
	FaultTracker
}

type WorkerID uint64

type Manager struct {
	//scfg *ffiwrapper.Config

	ls         stores.LocalStorage
	storage    *stores.Remote
	localStore *stores.Local
	remoteHnd  *stores.FetchHandler
	index      stores.SectorIndex

	redisCli        *gr.RedisClient
	isExistSealTask bool
	sched           *scheduler

	storage.Prover

	// 存储ip -> 传输数量映射
	transIP2Count sync.Map
	transLK       sync.Mutex
	maxTransCount int
}

type SealerConfig struct {
	ParallelFetchLimit int

	// Local worker config
	AllowAddPiece   bool
	AllowPreCommit1 bool
	AllowPreCommit2 bool
	AllowCommit     bool
	AllowUnseal     bool
}

type StorageAuth http.Header

func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, sc SealerConfig, urls URLs, sa StorageAuth) (*Manager, error) {
	logrus.Init()
	//sectorSize, _ := cfg.SealProofType.SectorSize()
	lstor, err := stores.NewLocal(ctx, ls, si, urls)
	if err != nil {
		return nil, err
	}

	prover, err := ffiwrapper.New(&readonlyProvider{stor: lstor, index: si})
	if err != nil {
		return nil, xerrors.Errorf("creating prover instance: %w", err)
	}

	stor := stores.NewRemote(lstor, si, http.Header(sa), sc.ParallelFetchLimit)

	m := &Manager{
		ls:              ls,
		storage:         stor,
		localStore:      lstor,
		remoteHnd:       &stores.FetchHandler{Local: lstor},
		index:           si,
		isExistSealTask: true,
		sched:           newScheduler(),

		Prover: prover,

		transLK:       sync.Mutex{},
		maxTransCount: 7, // 控制最大传输并发量
	}

	//err = m.sched.StartLoad()
	//if err != nil {
	//	return  nil,xerrors.Errorf("===== start load task record err: %w", err)
	//}

	//init redis data
	var rurl string
	var pw string
	conf, err := InitRequestConfig("conf.json")
	if err != nil {
		logrus.SchedLogger.Errorf("===== read conf.json err:", err)
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

	rc := gr.NewRedisClusterCLi(ctx, rurl, pw)
	if rc == nil {
		return nil, errors.New("new redis cluster client err")
	}
	m.redisCli = rc

	if minertype.CanDoSched() {
		go m.sched.runSched()
	} else {
		log.Info("Do not do sched!!!!!!")
	}

	localTasks := []sealtasks.TaskType{
		sealtasks.TTCommit1, sealtasks.TTFinalize, sealtasks.TTFetch, sealtasks.TTReadUnsealed,
	}
	if sc.AllowAddPiece {
		localTasks = append(localTasks, sealtasks.TTAddPiece)
	}
	if sc.AllowPreCommit1 {
		localTasks = append(localTasks, sealtasks.TTPreCommit1)
	}
	if sc.AllowPreCommit2 {
		localTasks = append(localTasks, sealtasks.TTPreCommit2)
	}
	if sc.AllowCommit {
		localTasks = append(localTasks, sealtasks.TTCommit2)
	}
	if sc.AllowUnseal {
		localTasks = append(localTasks, sealtasks.TTUnseal)
	}

	err = m.AddWorker(ctx, NewLocalWorker(WorkerConfig{
		//SealProof: cfg.SealProofType,
		TaskTypes: localTasks,
	}, stor, lstor, si))
	if err != nil {
		return nil, xerrors.Errorf("adding local worker: %w", err)
	}

	return m, nil
}

func (m *Manager) AddLocalStorage(ctx context.Context, path string) error {
	path, err := homedir.Expand(path)
	if err != nil {
		return xerrors.Errorf("expanding local path: %w", err)
	}

	if err := m.localStore.OpenPath(ctx, path); err != nil {
		return xerrors.Errorf("opening local path: %w", err)
	}

	if err := m.ls.SetStorage(func(sc *stores.StorageConfig) {
		sc.StoragePaths = append(sc.StoragePaths, stores.LocalPath{Path: path})
	}); err != nil {
		return xerrors.Errorf("get storage config: %w", err)
	}
	return nil
}

func (m *Manager) AddWorker(ctx context.Context, w Worker) error {
	info, err := w.Info(ctx)
	if err != nil {
		return xerrors.Errorf("getting worker info: %w", err)
	}
	scope, err := GetWorkScope(ctx, w)
	if err != nil {
		return err
	}

	path, err := w.Paths(ctx)
	fsS, err := m.storage.FsStat(ctx, path[0].ID)
	if err != nil {
		return err
	}

	//计算worker配置信息
	//tc := CalculateResources(info.Resources, fsS.Available)
	// 修改为按容量计算
	tc := CalculateResources(info.Resources, fsS.Capacity)
	logrus.SchedLogger.Infof("===== CalculateResources worker[%s],taskConfig:[%+v],availableDisk:%d", info.Hostname, tc, fsS.Available)
	if scope == PRIORITYCOMMIT2 {
		tc.Commit2 = 10000
		logrus.SchedLogger.Infof("===== c2 worker[%s],Resources:[%+v],WorkScope:[%+v]", info.Hostname, info.Resources, scope)
	}

	m.sched.newWorkers <- &workerHandle{
		w: w,
		wt: &workTracker{
			running: map[uint64]storiface.WorkerJob{},
		},
		info:      info,
		preparing: &activeResources{},
		active:    &activeResources{},
		taskConf:  tc,
		WorkScope: scope,
	}
	m.sched.workScopeRecorder.append(scope, info.Hostname)
	logrus.SchedLogger.Infof("===== ADD worker[%s],Resources:[%+v],WorkScope:[%+v],workScopeRecorder:[%+v]", info.Hostname, info.Resources, scope, m.sched.workScopeRecorder)
	return nil
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.remoteHnd.ServeHTTP(w, r)
}

//func (m *Manager) SectorSize() abi.SectorSize {
//	sz, _ := m.scfg.SealProofType.SectorSize()
//	return sz
//}

func schedNop(context.Context, Worker) error {
	return nil
}

func schedFetch(sector storage.SectorRef, ft storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) func(context.Context, Worker) error {
	return func(ctx context.Context, worker Worker) error {
		return worker.Fetch(ctx, sector, ft, ptype, am)
	}
}

func (m *Manager) tryReadUnsealedPiece(ctx context.Context, sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (foundUnsealed bool, readOk bool, selector WorkerSelector, returnErr error) {

	// acquire a lock purely for reading unsealed sectors
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTNone); err != nil {
		returnErr = xerrors.Errorf("acquiring read sector lock: %w", err)
		return
	}

	// passing 0 spt because we only need it when allowFetch is true
	best, err := m.index.StorageFindSector(ctx, sector.ID, storiface.FTUnsealed, 0, false)
	if err != nil {
		returnErr = xerrors.Errorf("read piece: checking for already existing unsealed sector: %w", err)
		return
	}

	foundUnsealed = len(best) > 0
	if foundUnsealed { // append to existing
		// There is unsealed sector, see if we can read from it

		selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)

		err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
			readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
			return err
		})
		if err != nil {
			returnErr = xerrors.Errorf("reading piece from sealed sector: %w", err)
		}
	} else {
		selector = newAllocSelector(m.index, storiface.FTUnsealed, storiface.PathSealing)
	}
	return
}

func (m *Manager) ReadPiece(ctx context.Context, sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) error {
	foundUnsealed, readOk, selector, err := m.tryReadUnsealedPiece(ctx, sink, sector, offset, size)
	if err != nil {
		return err
	}
	if readOk {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTUnsealed); err != nil {
		return xerrors.Errorf("acquiring unseal sector lock: %w", err)
	}

	unsealFetch := func(ctx context.Context, worker Worker) error {
		if err := worker.Fetch(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.PathSealing, storiface.AcquireCopy); err != nil {
			return xerrors.Errorf("copy sealed/cache sector data: %w", err)
		}

		if foundUnsealed {
			if err := worker.Fetch(ctx, sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove); err != nil {
				return xerrors.Errorf("copy unsealed sector data: %w", err)
			}
		}
		return nil
	}

	if unsealed == cid.Undef {
		return xerrors.Errorf("cannot unseal piece (sector: %d, offset: %d size: %d) - unsealed cid is undefined", sector, offset, size)
	}
	err = m.sched.Schedule(ctx, sector, sealtasks.TTUnseal, selector, unsealFetch, func(ctx context.Context, w Worker) error {
		return w.UnsealPiece(ctx, sector, offset, size, ticket, unsealed)
	})
	if err != nil {
		return err
	}

	selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
		return err
	})
	if err != nil {
		return xerrors.Errorf("reading piece from sealed sector: %w", err)
	}

	if !readOk {
		return xerrors.Errorf("failed to read unsealed piece")
	}

	return nil
}

func (m *Manager) NewSector(ctx context.Context, sector storage.SectorRef) error {
	logrus.SchedLogger.Warnf("stub NewSector")
	return nil
}

func (m *Manager) AddPiece(ctx context.Context, sector storage.SectorRef, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, filePath string, fileName string, apType string) (abi.PieceInfo, error) {
	//func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader, apType string) (abi.PieceInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	var out abi.PieceInfo
	apInfo, err := gr.Serialization(gr.ParamsAp{
		Sector:       sector,
		PieceSizes:   existingPieces,
		NewPieceSize: sz,
		FilePath:     filePath,
		FileName:     fileName,
		ApType:       apType,
	})
	if err != nil {
		return out, err
	}

	var tt sealtasks.TaskType
	switch fileName {
	default:
		//"_seal", "_filPledgeToDealSector":
		tt = sealtasks.TTAddPieceSe
		//1.publish
		currentSealApId, err := m.PublishTask(sector, tt, apInfo, 1)
		if err != nil {
			return out, err
		}
		return m.SubscribeResult(sector, tt, currentSealApId)

	case "_pledgeSector":
		tt = sealtasks.TTAddPiecePl
		//checkout
		pubField := gr.SplicingBackupPubAndParamsField(sector.ID.Number, tt, 0)
		res := m.RecoveryPledge(sector.ID.Number, pubField)
		if res != nil {
			if res.Err == "" {
				logrus.SchedLogger.Infof("===== rd recovery miner ok, hostName %d, taskType %+v", sector.ID.Number, tt)
				m.WhetherToDeleteRetryCount(pubField, "")
				return res.PieceInfo, nil
			}
			m.DeleteCurrentParamsRes(sector.ID.Number, tt)
			logrus.SchedLogger.Infof("===== rd recovery miner and find err %+v, hostName %d, taskType %+v", res.Err, sector.ID.Number, tt)
		}

		//1.publish
		_, err = m.PublishTask(sector, tt, apInfo, 1)
		if err != nil {
			return out, err
		}
		return m.SubscribeResult(sector, tt, 0)
	}
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo, recover bool) (out storage.PreCommit1Out, err error) {
	ticketEpoch := ctx.Value("p1RecoverDate")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTSealed|storiface.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	pp1Info, err := gr.Serialization(gr.ParamsP1{
		Sector:  sector,
		Ticket:  ticket,
		Pieces:  pieces,
		Recover: recover,
	})
	if err != nil {
		return nil, err
	}

	//recovery
	p1Field := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTPreCommit1, 0)

	if rd, ok := ticketEpoch.(gr.PreCommit1RD); ok {
		logrus.SchedLogger.Infof("===== rd PreCommit1RD sectorID %d rd %+v", rd, sector.ID.Number)
		res := m.RecoveryP1(sector.ID.Number, p1Field, rd.TicketEpoch)
		if res != nil {
			if res.Err == "" {
				logrus.SchedLogger.Infof("===== rd recovery miner ok, sectorID %d, taskType %+v", sector.ID.Number, sealtasks.TTPreCommit1)
				m.WhetherToDeleteRetryCount(p1Field, "")
				return res.Out, nil
			}
			logrus.SchedLogger.Infof("===== rd recovery miner err %+v, hostName %d, taskType %+v", res.Err, sector.ID.Number, sealtasks.TTPreCommit1)
		}
		err = m.redisCli.HSet(gr.RecoverName, p1Field, rd)
		if err != nil {
			logrus.SchedLogger.Errorf("===== rd recovery hset p1RecoverDate %+v  sectorID %+v, p1Field %+v\n", rd, sector.ID.Number, p1Field)
		}
		m.DeleteCurrentParamsRes(sector.ID.Number, sealtasks.TTPreCommit1)
	}

	//1.publish
	//logrus.SchedLogger.Infof("===== rd publish task, sectorID %+v taskType %+v", sector.ID.Number, sealtasks.TTPreCommit1)
	_, err = m.PublishTask(sector, sealtasks.TTPreCommit1, pp1Info, 1)
	if err != nil {
		return nil, err
	}

	//2.Subscribe res
	start := time.Now()
	tick := &time.Ticker{}
	switch sector.ProofType {
	case abi.RegisteredSealProof_StackedDrg2KiBV1, abi.RegisteredSealProof_StackedDrg2KiBV1_1:
		tick = time.NewTicker(time.Second)

	case abi.RegisteredSealProof_StackedDrg512MiBV1, abi.RegisteredSealProof_StackedDrg512MiBV1_1:
		tick = time.NewTicker(time.Second * 10)

	case abi.RegisteredSealProof_StackedDrg32GiBV1, abi.RegisteredSealProof_StackedDrg32GiBV1_1:
		tick = time.NewTicker(CHECK_RES_GAP)
		time.Sleep(time.Hour * 3)
	default:
		tick = time.NewTicker(CHECK_RES_GAP)
	}
	defer tick.Stop()

	resField := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTPreCommit1, 0)
	timer := time.NewTimer(P1WaitTime)
	defer timer.Stop()

	for {
		select {
		case <-timer.C: //12600
			m.StoreWaitTime(P1WaitTime, resField)
			continue

		case <-tick.C:
			usedTime := time.Now().Sub(start)
			if usedTime > P1WaitTime {
				m.StoreWaitTime(usedTime, resField)
			}

			hostName := ""
			//check params
			exist, err := m.redisCli.HExist(gr.ParamsResName, resField)
			if err != nil {
				logrus.SchedLogger.Errorf("===== HExist p1 res params err %+v sectorID %+v, p1Field %+v\n", err, sector.ID.Number, p1Field)
				continue
			}

			if !exist {
				continue
			}

			//get res
			err = m.redisCli.HGet(gr.PubName, resField, &hostName)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget p1 pub err %+v sectorID %+v, p1Field %+v\n", err, sector, p1Field)
				continue
			}

			//update taskCount (need lock)
			defer func() {
				err = m.FreeTaskCount(hostName, sector.ID.Number, sealtasks.TTPreCommit1, 0)
				if err != nil {
					logrus.SchedLogger.Errorf("===== sector %+v p1 finished , update %s taskCount err:%+v", sector.ID.Number, hostName, err)
				}
			}()

			logrus.SchedLogger.Infof("===== rd ticker check task, sectorID %+v taskType %+v worker %+v\n", sector.ID.Number, sealtasks.TTPreCommit1, hostName)
			//get params res
			pubRes := ""
			err = m.redisCli.HGet(gr.PubResName, resField, &pubRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget p1 res pub err %+v sectorID %+v, p1Field %+v\n", err, sector.ID.Number, p1Field)
				continue
			}

			if pubRes != gr.PubResSucceed {
				logrus.SchedLogger.Errorf("===== sector(%+v) p1 computing err:%+v", sector, pubRes)
				return out, errors.New(fmt.Sprintf("%d p1 res err: %s", sector.ID.Number, pubRes))
			}

			paramsRes := &gr.ParamsResP1{}
			err = m.redisCli.HGet(gr.ParamsResName, resField, paramsRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget p1 res params err %+v sectorID %+v, p1Field %+v\n", err, sector.ID.Number, p1Field)
				continue
			}

			return paramsRes.Out, nil
		}
	}
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	pp2Info, err := gr.Serialization(gr.ParamsP2{
		Sector: sector,
		Pc1o:   phase1Out,
	})
	if err != nil {
		return out, err
	}

	p2Field := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTPreCommit2, 0)
	res := m.RecoveryP2(sector.ID.Number, p2Field)
	if res != nil {
		if res.Err == "" {
			logrus.SchedLogger.Infof("===== rd recovery miner ok, sectorID %d, taskType %+v", sector.ID.Number, sealtasks.TTPreCommit2)
			m.WhetherToDeleteRetryCount(p2Field, "")
			return res.Out, nil
		}
		m.DeleteCurrentParamsRes(sector.ID.Number, sealtasks.TTPreCommit2)
		logrus.SchedLogger.Infof("===== rd recovery miner err %+v, hostName %d, taskType %+v", res.Err, sector.ID.Number, sealtasks.TTPreCommit2)
	}

	//1.publish
	//logrus.SchedLogger.Infof("===== rd publish task, sectorID %+v taskType %+v", sector.ID.Number, sealtasks.TTPreCommit2)
	_, err = m.PublishTask(sector, sealtasks.TTPreCommit2, pp2Info, 1)
	if err != nil {
		return out, err
	}
	//2.Subscribe res
	start := time.Now()
	tick := &time.Ticker{}
	switch sector.ProofType {
	case abi.RegisteredSealProof_StackedDrg2KiBV1, abi.RegisteredSealProof_StackedDrg2KiBV1_1:
		tick = time.NewTicker(time.Second)

	case abi.RegisteredSealProof_StackedDrg512MiBV1, abi.RegisteredSealProof_StackedDrg512MiBV1_1:
		tick = time.NewTicker(time.Second * 10)

	case abi.RegisteredSealProof_StackedDrg32GiBV1, abi.RegisteredSealProof_StackedDrg32GiBV1_1:
		tick = time.NewTicker(CHECK_RES_GAP)
		time.Sleep(time.Minute * 20)

	default:
		tick = time.NewTicker(CHECK_RES_GAP)
	}
	defer tick.Stop()

	resField := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTPreCommit2, 0)
	timer := time.NewTimer(P2WaitTime)
	defer timer.Stop()

	for {
		select {
		case <-timer.C: //12600
			m.StoreWaitTime(P2WaitTime, resField)
			continue

		case <-tick.C:
			usedTime := time.Now().Sub(start)
			if usedTime > P2WaitTime {
				m.StoreWaitTime(usedTime, resField)
			}
			hostName := ""
			//check params
			exist, err := m.redisCli.HExist(gr.ParamsResName, resField)
			if err != nil {
				logrus.SchedLogger.Errorf("===== HExist p2 res params err %+v sectorID %+v, p2Field %+v\n", err, sector.ID.Number, p2Field)
				continue
			}

			if !exist {
				continue
			}

			//get res
			err = m.redisCli.HGet(gr.PubName, resField, &hostName)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget p2 pub err %+v sectorID %+v, p2Field %+v\n", err, sector.ID.Number, p2Field)
				continue
			}

			//update taskCount (need lock)
			defer func() {
				err = m.FreeTaskCount(hostName, sector.ID.Number, sealtasks.TTPreCommit2, 0)
				if err != nil {
					logrus.SchedLogger.Errorf("===== sector %+v p2 finished , update %s taskCount err:%+v", sector.ID.Number, hostName, err)
				}
			}()

			logrus.SchedLogger.Infof("===== rd ticker check task, sectorID %+v taskType %+v worker %+v\n", sector.ID.Number, sealtasks.TTPreCommit2, hostName)
			//get params res
			pubRes := ""
			err = m.redisCli.HGet(gr.PubResName, resField, &pubRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget p2 res pub err %+v sectorID %+v, p2Field %+v\n", err, sector.ID.Number, p2Field)
				continue
			}

			if pubRes != gr.PubResSucceed {
				logrus.SchedLogger.Errorf("===== sector(%+v) p2 computing err:%+v", sector, pubRes)
				return out, errors.New(fmt.Sprintf("%d p2 res err: %s", sector.ID.Number, pubRes))
			}

			paramsRes := &gr.ParamsResP2{}
			err = m.redisCli.HGet(gr.ParamsResName, resField, paramsRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget p2 res params err %+v sectorID %+v, p2Field %+v\n", err, sector.ID.Number, p2Field)
				continue
			}

			return paramsRes.Out, nil
		}
	}
}

func (m *Manager) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	storagePaths, path2sid := m.GetStoragePathList(ctx, sector.ID)
	if len(storagePaths) == 0 || len(path2sid) == 0 {
		logrus.SchedLogger.Errorf("try to doing sector(%+v) c1 task, but storagePaths = 0", sector)
	}

	pc1Info, err := gr.Serialization(gr.ParamsC1{
		Sector:   sector,
		Ticket:   ticket,
		Seed:     seed,
		Pieces:   pieces,
		Cids:     cids,
		PathList: storagePaths,
	})
	if err != nil {
		return out, err
	}

	c1Field := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTCommit1, 0)
	res := m.RecoveryC1(sector.ID.Number, c1Field)
	if res != nil {
		if res.Err == "" {
			logrus.SchedLogger.Infof("===== rd recovery miner ok, sectorID %d, taskType %+v", sector.ID.Number, sealtasks.TTCommit1)
			hostName := ""
			resField := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTCommit1, 0)
			err = m.redisCli.HGet(gr.PubResName, resField, &hostName)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget c1 res err %+v sectorID %+v, c1Field %+v\n", err, sector.ID.Number, c1Field)
			}
			//free p1 counter
			p1Field := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTPreCommit1, 0)
			cp1 := gr.SplicingCounterP1Key(hostName)
			cp1Exist, err := m.redisCli.HExist(cp1, p1Field)
			if err == nil && cp1Exist {
				m.FreeP1Count(hostName, sector.ID.Number, sealtasks.TTPreCommit1, 0)
			}
			m.WhetherToDeleteRetryCount(c1Field, "")

			return res.Out, nil
		}
		//delete res
		m.DeleteCurrentParamsRes(sector.ID.Number, sealtasks.TTCommit1)
		logrus.SchedLogger.Infof("===== rd recovery miner err %+v, hostName %d, taskType %+v", res.Err, sector.ID.Number, sealtasks.TTCommit1)
	}

	//1.publish
	//logrus.SchedLogger.Infof("===== rd publish task, sectorID %+v taskType %+v", sector.ID.Number, sealtasks.TTCommit1)
	_, err = m.PublishTask(sector, sealtasks.TTCommit1, pc1Info, 1)
	if err != nil {
		return out, err
	}

	//2.Subscribe res
	//subCha, err := m.redisCli.Subscribe(gr.SUBSCRIBECHANNEL)
	//if err != nil {
	//	return out, err
	//}

	tick := &time.Ticker{}
	switch sector.ProofType {
	case abi.RegisteredSealProof_StackedDrg2KiBV1, abi.RegisteredSealProof_StackedDrg2KiBV1_1:
		tick = time.NewTicker(time.Second)

	case abi.RegisteredSealProof_StackedDrg512MiBV1, abi.RegisteredSealProof_StackedDrg512MiBV1_1:
		tick = time.NewTicker(time.Second * 10)

	case abi.RegisteredSealProof_StackedDrg32GiBV1, abi.RegisteredSealProof_StackedDrg32GiBV1_1:
		tick = time.NewTicker(CHECK_RES_GAP)
	default:
		tick = time.NewTicker(CHECK_RES_GAP)
	}
	defer tick.Stop()

	resField := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTCommit1, 0)
	timer := time.NewTimer(C1WaitTime)
	defer timer.Stop()
	start := time.Now()

	for {
		select {
		case <-timer.C: //12600
			m.StoreWaitTime(C1WaitTime, resField)
			continue

		case <-tick.C:
			usedTime := time.Now().Sub(start)
			if usedTime > C1WaitTime {
				m.StoreWaitTime(usedTime, resField)
			}
			hostName := ""
			//check params
			exist, err := m.redisCli.HExist(gr.ParamsResName, resField)
			if err != nil {
				logrus.SchedLogger.Errorf("===== HExist c1 res params err %+v sectorID %+v, c1Field %+v\n", err, sector.ID.Number, c1Field)
				continue
			}

			if !exist {
				continue
			}

			//get res
			err = m.redisCli.HGet(gr.PubName, resField, &hostName)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget c1 pub err %+v sectorID %+v, c1Field %+v\n", err, sector.ID.Number, c1Field)
				continue
			}

			//update taskCount (need lock)
			defer func() {
				err = m.FreeTaskCount(hostName, sector.ID.Number, sealtasks.TTCommit1, 0)
				if err != nil {
					logrus.SchedLogger.Errorf("===== sector %+v c1 finished , update %s taskCount err:%+v", sector.ID.Number, hostName, err)
				}
			}()

			logrus.SchedLogger.Infof("===== rd ticker check task, sectorID %+v taskType %+v worker %+v\n", sector.ID.Number, sealtasks.TTCommit1, hostName)
			//get params res
			pubRes := ""
			err = m.redisCli.HGet(gr.PubResName, resField, &pubRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget c1 res pub err %+v sectorID %+v, c1Field %+v\n", err, sector.ID.Number, c1Field)
				continue
			}

			//collect failed transfer path
			m.CollectFailedPath(hostName)

			if pubRes != gr.PubResSucceed {
				logrus.SchedLogger.Errorf("===== sector(%+v) c1 computing err:%+v", sector, pubRes)
				return out, errors.New(fmt.Sprintf("%d c1 res err: %s", sector.ID.Number, pubRes))
			}

			paramsRes := &gr.ParamsResC1{}
			err = m.redisCli.HGet(gr.ParamsResName, resField, paramsRes)
			if err != nil {
				logrus.SchedLogger.Errorf("===== hget c1 res params err %+vsectorID %+v, c1Field %+v\n", err, sector.ID.Number, c1Field)
				continue
			}

			if paramsRes.StoragePath == "" {
				logrus.SchedLogger.Errorf("===== sector(%+v) c1 transfor path is nil!!!!", sector)
				return out, xerrors.Errorf("===== sector(%+v) c1 transfor path is nil!!!!, sector")
			}

			_, ok := path2sid[paramsRes.StoragePath]
			if !ok {
				logrus.SchedLogger.Errorf("===== sector(%+v) c1 transfor error,not find sid, path is :%s, ", sector, paramsRes.StoragePath)
				return out, xerrors.Errorf("===== sector(%+v) c1 transfor error,not find sid, path is :%s, ", sector, paramsRes.StoragePath)
			}

			// declareSector  申明新的存储路径
			// 判断是否是deal 的sector，如果是，则存储unseal的文件，否则不存unsealed文件
			exist, err = m.redisCli.HExist(gr.PubName, gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTAddPieceSe, 1))
			if err != nil {
				logrus.SchedLogger.Errorf("===== sector(%+v) c1 finished , check sector is deal or not err:%+v", sector, err)
				return out, err
			}

			if exist {
				// deal sector 存储unseal文件
				err = m.index.StorageDeclareSector(ctx, stores.ID(path2sid[paramsRes.StoragePath]), sector.ID, storiface.FTSealed|storiface.FTCache|storiface.FTUnsealed, true)
			} else {
				// not deal sector 不存储unseal文件
				err = m.index.StorageDeclareSector(ctx, stores.ID(path2sid[paramsRes.StoragePath]), sector.ID, storiface.FTSealed|storiface.FTCache, true)
			}

			if err != nil {
				// 如果失败则删除任务记录，无法恢复
				m.sched.taskRecorder.Delete(sector)
				logrus.SchedLogger.Errorf("===== after sector(%+v) finished Commit1 and transfor data to destPath(%+v),but  failed to StorageDeclareSector. err:%+v", sector, paramsRes.StoragePath, err)
				return out, xerrors.Errorf("===== after sector(%+v) finished Commit1 and transfor data to destPath(%+v),but  failed to StorageDeclareSector. err:%+v", sector, paramsRes.StoragePath, err)
			}

			//free p1 counter
			p1Field := gr.SplicingBackupPubAndParamsField(sector.ID.Number, sealtasks.TTPreCommit1, 0)
			cp1 := gr.SplicingCounterP1Key(hostName)
			cp1Exist, err := m.redisCli.HExist(cp1, p1Field)
			if err == nil && cp1Exist {
				m.FreeP1Count(hostName, sector.ID.Number, sealtasks.TTPreCommit1, 0)
			}

			return paramsRes.Out, nil
		}
	}
}

// 获取存储路径列表
func (m *Manager) GetStoragePathList(ctx context.Context, sector abi.SectorID) ([]string, map[string]string) {
	// 筛选存储机器
	ssize, err := abi.RegisteredSealProof_StackedDrg32GiBV1.SectorSize()
	if err != nil {
		return []string{}, map[string]string{}
	}
	sis, err := m.index.StorageBestAlloc(ctx, storiface.FTSealed, ssize, storiface.PathStorage)
	//m.index.StorageList
	if err != nil {
		logrus.SchedLogger.Errorf("doing sector(%+v) c1 get storage paths error : %w", sector, err)
		return []string{}, map[string]string{}
	}
	storagePaths, err := m.StorageLocal(ctx)
	if err != nil {
		logrus.SchedLogger.Errorf("doing  sector(%+v) c1 get storage local paths error : %w", sector, err)
		return []string{}, map[string]string{}
	}

	paths := make([]string, 0, len(sis))
	path2sid := make(map[string]string, 0)
	for _, si := range sis {
		p, ok := storagePaths[si.ID]
		if !ok {
			logrus.SchedLogger.Errorf("doing  sector(%+v) c1 get %s storage local paths error : %w", sector, si.ID, err)
			continue
		}

		if p == "" { // TODO: can that even be the case?
			logrus.SchedLogger.Errorf("doing  sector(%+v) c1 get %s storage local paths is nil !!!", sector, si.ID)
			continue
		}

		// 尝试 开始发送 通过解析p.local来获取NFS ip
		ip, destPath := transfordata.PareseDestFromePath(p)
		if ip == "" {
			logrus.SchedLogger.Errorf("doing  sector(%+v) c1 parse path(%+v) error,not find dest ip", sector, p)
		} else if destPath == "" {
			logrus.SchedLogger.Errorf("doing  sector(%+v) c1 parse path(%+v) error,not find dest dest path", sector, p)
		} else {
			paths = append(paths, p)
			path2sid[p] = string(si.ID)
		}
	}

	return paths, path2sid
}

//func (m *Manager) FindBestStoragePathToPushData(ctx context.Context, sector abi.SectorID, w Worker) (string, stores.ID) {
//	// 筛选存储机器
//	ssize, err := abi.RegisteredSealProof_StackedDrg32GiBV1.SectorSize()
//	if err != nil {
//		return "", ""
//	}
//	sis, err := m.index.StorageBestAlloc(ctx, storiface.FTSealed, ssize, storiface.PathStorage)
//	//m.index.StorageList
//	if err != nil {
//		logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server,finding best storage error : %w", sector, err)
//		return "", ""
//	}
//
//	storagePaths, err := m.StorageLocal(ctx)
//	if err != nil {
//		logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server,finding storagePaths error : %w", sector, err)
//		return "", ""
//	}
//
//	var hasNFS bool
//	var transErrCount int
//RetryFindStorage:
//	//var bestSi stores.StorageInfo
//	for _, si := range sis {
//		p, ok := storagePaths[si.ID]
//		if !ok {
//			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, but not found storage ID(%+v)", sector, si.ID)
//			continue
//		}
//
//		if p == "" { // TODO: can that even be the case?
//			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, but not found storage path is null", sector)
//			continue
//		}
//
//		// 尝试 开始发送 通过解析p.local来获取NFS ip
//		ip, destPath := transfordata.PareseDestFromePath(p)
//		if ip == "" {
//			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, Parse path(%+v) error,not find dest ip", sector, p)
//		} else if destPath == "" {
//			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, Parse path(%+v) error,not find dest path", sector, p)
//		} else {
//			logrus.SchedLogger.Infof("===== find best destPath(%+v) ip(%+v)", destPath, ip)
//			hasNFS = true
//			if w == nil {
//				return p, si.ID
//			}
//
//			// 检测连通行
//			ok, err := transfordata.ConnectTest(destPath+"/firefly-miner", ip)
//			if !ok || err != nil {
//				continue
//			}
//
//			// 绑定传输，设置传输数量最多同时为8
//			m.transLK.Lock()
//			_, ok = m.transIP2Count.Load(ip)
//			if !ok {
//				m.transIP2Count.Store(ip, 0)
//			}
//			v, _ := m.transIP2Count.Load(ip)
//			if v.(int) >= m.maxTransCount {
//				logrus.SchedLogger.Infof("===== 当前有超过%d个传输任务向IP(%+v)传输数据,配置最大值为%d", v.(int), ip, m.maxTransCount)
//				m.transLK.Unlock()
//				continue
//			}
//			m.transIP2Count.Store(ip, v.(int)+1)
//			//v, _ = m.transIP2Count.Load(ip)
//			logrus.SchedLogger.Infof("ip(%s) transfor task count is %d,sector(%+v) try to it", ip, v.(int)+1, sector)
//			m.transLK.Unlock()
//
//			// 开始传输数据
//			err = w.PushDataToStorage(ctx, sector, p)
//
//			m.transLK.Lock()
//			v, _ = m.transIP2Count.Load(ip)
//			m.transIP2Count.Store(ip, v.(int)-1)
//			m.transLK.Unlock()
//
//			if err != nil {
//				// 如果发送错误次数超过10，则放弃，可能数据有问题
//				transErrCount += 1
//				logrus.SchedLogger.Errorf("===== after sector(%+v) finished Commit1,but push data to Storage(%+v) failed. err:%+v", sector, p, err)
//			} else {
//				// 传输成功，返回
//				//logrus.SchedLogger.Infof("ip(%s) transfor task count is %d,sector(%+v) success send to it", ip, v.(int)-1, sector)
//				logrus.SchedLogger.Infof("===== finished sector(%+v) Commit1 transfored to destPath(%+v) success!!", sector, p)
//				logrus.SchedLogger.Infof("===== finished sector(%+v) Commit1 transfored to destPath(%+v) success!!", sector, p)
//				return p, si.ID
//			}
//		}
//	}
//	//if bestSi.ID == "" {
//	if hasNFS {
//		logrus.SchedLogger.Warnf("all storage transfor task count is more then %d, please check it", m.maxTransCount)
//		if transErrCount < 10 {
//			time.Sleep(time.Minute)
//			goto RetryFindStorage
//		} else {
//			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, error more then %d times", sector, 10)
//			return "", ""
//		}
//	}
//	logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, can not find any storage Server", sector)
//	//}
//	return "", ""
//}

func (m *Manager) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		wInfo, _ := w.Info(ctx)
		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		taskRd.taskStatus = COMMIT2_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is computing Commit2 in sectorID[%+v]", wInfo.Hostname, sector)
		//go m.sched.StartStore(sector.ID.Number, sealtasks.TTCommit2, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}
		out = p
		//go m.sched.StartStore(sector.ID.Number, sealtasks.TTCommit2, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = TRANSFOR_FINISHED
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is TRANSFOR_FINISHED in sectorID[%+v]", wInfo.Hostname, sector)
		return nil
	})

	return out, err
}

func (m *Manager) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	//if err := m.index.StorageLock(ctx, sector, storiface.FTNone, storiface.FTSealed|storiface.FTUnsealed|storiface.FTCache); err != nil {
	//	return xerrors.Errorf("acquiring sector lock: %w", err)
	//}
	//
	//unsealed := storiface.FTUnsealed
	//{
	//	unsealedStores, err := m.index.StorageFindSector(ctx, sector, storiface.FTUnsealed, 0, false)
	//	if err != nil {
	//		return xerrors.Errorf("finding unsealed sector: %w", err)
	//	}
	//
	//	if len(unsealedStores) == 0 { // Is some edge-cases unsealed sector may not exist already, that's fine
	//		unsealed = storiface.FTNone
	//	}
	//}

	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, false)

	err := m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		//schedFetch(sector, storiface.FTCache|stores.FTSealed|unsealed, storiface.PathSealing, storiface.AcquireMove),
		schedNop,
		func(ctx context.Context, w Worker) error {
			wInfo, _ := w.Info(ctx)
			logrus.SchedLogger.Infof("===== worker %s start do %s for sector %d mod keepUnseald 0", wInfo.Hostname, sealtasks.TTFinalize, sector)
			m.sched.taskRecorder.Delete(sector)
			// 找不到合适的路径让commit1执行失败并且不要删除数据
			//destPath, _ := m.FindBestStoragePathToPushData(ctx, sector, nil)
			//if destPath == "" {
			//	// if not mount nfs disk then miner fetch form worker
			//	return w.FinalizeSector(ctx, sector, []storage.Range{})
			//}
			return nil
			//return w.FinalizeSector(ctx, sector, keepUnsealed)
			//return w.FinalizeSector(ctx, sector, []storage.Range{})
		})
	if err != nil {
		return err
	}
	logrus.SchedLogger.Infof("===== rd start delete data, sectorID %+v", sector.ID.Number)
	m.DeleteDataForSid(sector.ID.Number)

	//fetchSel := newAllocSelector(m.index, storiface.FTCache|stores.FTSealed, storiface.PathStorage)
	//moveUnsealed := unsealed
	//{
	//	if len(keepUnsealed) == 0 {
	//		moveUnsealed = storiface.FTNone
	//	}
	//}

	// 禁止拉取数据,修复FinalizeFaild
	//err = m.sched.Schedule(ctx, sector, sealtasks.TTFetch, fetchSel,
	//	//schedFetch(sector, storiface.FTCache|stores.FTSealed|moveUnsealed, storiface.PathStorage, storiface.AcquireMove),
	//	schedNop,
	//	func(ctx context.Context, w Worker) error {
	//		err := w.MoveStorage(ctx, sector, storiface.FTCache|stores.FTSealed|moveUnsealed)
	//		if err != nil {
	//			return err
	//		}
	//		//delete after sector is executed
	//		m.sched.taskRecorder.Delete(sector)
	//		return nil
	//	})
	//if err != nil {
	//	return xerrors.Errorf("moving sector to storage: %w", err)
	//}

	return nil
}

func (m *Manager) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) error {
	logrus.SchedLogger.Warn("ReleaseUnsealed todo")
	return nil
}

func (m *Manager) Remove(ctx context.Context, sector abi.SectorID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, storiface.FTNone, storiface.FTSealed|storiface.FTUnsealed|storiface.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	var err error

	if rerr := m.storage.Remove(ctx, sector, storiface.FTSealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector, storiface.FTCache, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector, storiface.FTUnsealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (m *Manager) StorageLocal(ctx context.Context) (map[stores.ID]string, error) {
	l, err := m.localStore.Local(ctx)
	if err != nil {
		return nil, err
	}

	out := map[stores.ID]string{}
	for _, st := range l {
		out[st.ID] = st.LocalPath
	}

	return out, nil
}

func (m *Manager) FsStat(ctx context.Context, id stores.ID) (fsutil.FsStat, error) {
	return m.storage.FsStat(ctx, id)
}

func (m *Manager) SchedDiag(ctx context.Context, doSched bool) (interface{}, error) {
	return m.sched.Info(ctx)
}

func (m *Manager) Close(ctx context.Context) error {
	return m.sched.Close(ctx)
}

func (m *Manager) updateTaskRecordStatus(sector abi.SectorID, status CurrentTaskStatus) {

	tr := m.getTaskRecord(sector)
	taskRd := tr.(sectorTaskRecord)

	taskRd.taskStatus = status
	m.sched.taskRecorder.Store(sector, taskRd)
}

func (m *Manager) getTaskRecord(sector abi.SectorID) interface{} {
	_, ok := m.sched.taskRecorder.Load(sector)
	if !ok {
		m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
	}
	tr, _ := m.sched.taskRecorder.Load(sector)
	return tr
}

var _ SectorManager = &Manager{}
