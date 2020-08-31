package sectorstorage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/sector-storage/fsutil"
	"github.com/mitchellh/go-homedir"

	logrus "github.com/filecoin-project/sector-storage/log"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/sector-storage/storiface"
)

var log = logging.Logger("advmgr")

var ErrNoWorkers = errors.New("no suitable workers found")

type URLs []string

type Worker interface {
	ffiwrapper.StorageSealer

	MoveStorage(ctx context.Context, sector abi.SectorID) error

	// FetchRealData get real useful data
	FetchRealData(ctx context.Context, id abi.SectorID) error

	Fetch(ctx context.Context, s abi.SectorID, ft stores.SectorFileType, ptype stores.PathType, am stores.AcquireMode) error
	UnsealPiece(context.Context, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error
	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize) (bool, error)

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

	// Returns paths accessible to the worker
	Paths(context.Context) ([]stores.StoragePath, error)

	Info(context.Context) (storiface.WorkerInfo, error)

	// returns channel signalling worker shutdown
	Closing(context.Context) (<-chan struct{}, error)

	// worker push sealed data to Storage
	PushDataToStorage(ctx context.Context, sid abi.SectorID, dest string) error

	// init get worker bind sectors
	GetBindSectors(ctx context.Context) ([]abi.SectorID, error)

	Close() error
}

type SectorManager interface {
	SectorSize() abi.SectorSize

	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error

	ffiwrapper.StorageSealer
	storage.Prover
	FaultTracker
}

type WorkerID uint64

type Manager struct {
	scfg *ffiwrapper.Config

	ls         stores.LocalStorage
	storage    *stores.Remote
	localStore *stores.Local
	remoteHnd  *stores.FetchHandler
	index      stores.SectorIndex

	sched *scheduler

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

func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, cfg *ffiwrapper.Config, sc SealerConfig, urls URLs, sa StorageAuth) (*Manager, error) {
	logrus.Init()
	lstor, err := stores.NewLocal(ctx, ls, si, urls)
	if err != nil {
		return nil, err
	}

	prover, err := ffiwrapper.New(&readonlyProvider{stor: lstor, index: si}, cfg)
	if err != nil {
		return nil, xerrors.Errorf("creating prover instance: %w", err)
	}

	stor := stores.NewRemote(lstor, si, http.Header(sa), sc.ParallelFetchLimit)

	m := &Manager{
		scfg: cfg,

		ls:         ls,
		storage:    stor,
		localStore: lstor,
		remoteHnd:  &stores.FetchHandler{Local: lstor},
		index:      si,

		sched: newScheduler(cfg.SealProofType),

		Prover: prover,

		transLK:       sync.Mutex{},
		maxTransCount: 7, // 控制最大传输并发量
	}

	//err = m.sched.StartLoad()
	//if err != nil {
	//	return  nil,xerrors.Errorf("===== start load task record err: %w", err)
	//}
	go m.sched.runSched()

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
		SealProof: cfg.SealProofType,
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

	// 获取woeker做过的任务，并且建立绑定
	sectors, err := w.GetBindSectors(ctx)
	if err != nil {
		log.Errorf("init worker init Bind sectors err :%+v", err)
	}
	log.Infof("init worker %s,Bind sectors:%+v", info.Hostname, sectors)
	for _, sid := range sectors {
		// 随便给的一个状态不知道是否正确
		tr := m.getTaskRecord(sid)
		taskRd := tr.(sectorTaskRecord)

		if taskRd.taskStatus != 0 {
			log.Infof("sector(%+v) bind ralationship is exist....", sid)
			continue
		}
		taskRd.taskStatus = PRE1_WAITTING
		taskRd.workerFortask = info.Hostname
		m.sched.taskRecorder.Store(sid, taskRd)
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

func (m *Manager) SectorSize() abi.SectorSize {
	sz, _ := m.scfg.SealProofType.SectorSize()
	return sz
}

func schedNop(context.Context, Worker) error {
	return nil
}

func schedFetch(sector abi.SectorID, ft stores.SectorFileType, ptype stores.PathType, am stores.AcquireMode) func(context.Context, Worker) error {
	return func(ctx context.Context, worker Worker) error {
		return worker.Fetch(ctx, sector, ft, ptype, am)
	}
}

func (m *Manager) ReadPiece(ctx context.Context, sink io.Writer, sector abi.SectorID, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed|stores.FTCache, stores.FTUnsealed); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// passing 0 spt because we only need it when allowFetch is true
	best, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
	if err != nil {
		return xerrors.Errorf("read piece: checking for already existing unsealed sector: %w", err)
	}

	var selector WorkerSelector
	if len(best) == 0 { // new
		selector = newAllocSelector(m.index, stores.FTUnsealed, stores.PathSealing)
	} else { // append to existing
		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)
	}

	var readOk bool

	if len(best) > 0 {
		// There is unsealed sector, see if we can read from it

		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

		err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
			readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
			return err
		})
		if err != nil {
			return xerrors.Errorf("reading piece from sealed sector: %w", err)
		}

		if readOk {
			return nil
		}
	}

	unsealFetch := func(ctx context.Context, worker Worker) error {
		if err := worker.Fetch(ctx, sector, stores.FTSealed|stores.FTCache, stores.PathSealing, stores.AcquireCopy); err != nil {
			return xerrors.Errorf("copy sealed/cache sector data: %w", err)
		}

		if len(best) > 0 {
			if err := worker.Fetch(ctx, sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove); err != nil {
				return xerrors.Errorf("copy unsealed sector data: %w", err)
			}
		}
		return nil
	}

	err = m.sched.Schedule(ctx, sector, sealtasks.TTUnseal, selector, unsealFetch, func(ctx context.Context, w Worker) error {
		return w.UnsealPiece(ctx, sector, offset, size, ticket, unsealed)
	})
	if err != nil {
		return err
	}

	selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		readOk, err = w.ReadPiece(ctx, sink, sector, offset, size)
		return err
	})
	if err != nil {
		return xerrors.Errorf("reading piece from sealed sector: %w", err)
	}

	if readOk {
		return xerrors.Errorf("failed to read unsealed piece")
	}

	return nil
}

func (m *Manager) NewSector(ctx context.Context, sector abi.SectorID) error {
	logrus.SchedLogger.Warnf("stub NewSector")
	return nil
}

//func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, filePath string, fileName string) (abi.PieceInfo, error) {
func (m *Manager) AddPiece(ctx context.Context, sector abi.SectorID, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader, apType string) (abi.PieceInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		selector = newAllocSelector(m.index, stores.FTUnsealed, stores.PathSealing)
	} else { // use existing
		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)
	}

	var tt sealtasks.TaskType
	switch apType {
	case "_seal":
		tt = sealtasks.TTAddPiece
		logrus.SchedLogger.Infof("===== AddPiece comming, sectorID:%+v, apType:%+v", sector, apType)

	case "_filPledgeToDealSector":
		tt = sealtasks.TTAddPiece
		logrus.SchedLogger.Infof("===== AddPiece comming, sectorID:%+v, apType:%+v", sector, apType)

	case "_pledgeSector":
		tt = sealtasks.TTAddPiecePl
		logrus.SchedLogger.Infof("===== AddPiece comming, sectorID:%+v, apType:%+v", sector, apType)
	}
	//if apType == "_pledgeSector" {
	//	logrus.SchedLogger.Infof("===== AddPiece comming, sectorID:%+v, apType:%+v", sector, apType)
	//	tt = sealtasks.TTAddPiecePl
	//} else {
	//	tt = sealtasks.TTAddPiece
	//	logrus.SchedLogger.Infof("===== AddPiece comming, sectorID:%+v, apType:%+v", sector, apType)
	//}

	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, tt, selector, schedNop, func(ctx context.Context, w Worker) error {
		wInfo, _ := w.Info(ctx)
		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		defer func() {
			m.sched.isExistFreeWorker = true
			logrus.SchedLogger.Infof("===== AddPiece finish, worker[%s],sectorID[%+v],isExistFreeWorker[%+v], masterSwitch:[%+v]",
				wInfo.Hostname, sector, m.sched.isExistFreeWorker, m.sched.masterSwitch)
		}()
		taskRd.taskStatus = ADDPIECE_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is computing AddPiece in sectorID[%+v], apTYpe:%s", wInfo.Hostname, sector, apType)
		go m.sched.StartStore(sector.Number, sealtasks.TTAddPiece, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.AddPiece(ctx, sector, existingPieces, sz, r, apType)
		if err != nil {
			return err
		}
		out = p
		go m.sched.StartStore(sector.Number, sealtasks.TTAddPiece, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())
		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = PRE1_WAITTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is PRE1_WAITTING in sectorID[%+v]", wInfo.Hostname, sector)
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTUnsealed, stores.FTSealed|stores.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newAllocSelector(m.index, stores.FTCache|stores.FTSealed, stores.PathSealing)

	//err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector, schedNop, func(ctx context.Context, w Worker) error {
		wInfo, _ := w.Info(ctx)
		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		taskRd.taskStatus = PRE1_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is computing PreCommit1 in sectorID[%+v]", wInfo.Hostname, sector)
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit1, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealPreCommit1(ctx, sector, ticket, pieces)
		if err != nil {
			// 如果任务失败直接删除任务
			m.sched.taskRecorder.Delete(sector)
			return err
		}
		out = p
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit1, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = PRE2_WAITING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is PRE2_WAITING in sectorID[%+v]", wInfo.Hostname, sector)
		return nil
	})

	return out, err
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, true)

	//err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		wInfo, _ := w.Info(ctx)
		defer func() {
			m.sched.isExistFreeWorker = true
			logrus.SchedLogger.Infof("===== SealPreCommit2 finish, worker[%s],sectorID[%+v],isExistFreeWorker[%+v], masterSwitch:[%+v]",
				wInfo.Hostname, sector, m.sched.isExistFreeWorker, m.sched.masterSwitch)
		}()
		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		taskRd.taskStatus = PRE2_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is computing PreCommit2 in sectorID[%+v]", wInfo.Hostname, sector)
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit2, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealPreCommit2(ctx, sector, phase1Out)
		if err != nil {
			// 如果任务失败直接删除
			m.sched.taskRecorder.Delete(sector)
			return err
		}
		out = p
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit2, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = COMMIT1_WAITTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is COMMIT1_WAITTING in sectorID[%+v]", wInfo.Hostname, sector)
		return nil
	})

	return out, err
}

func (m *Manager) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTSealed, stores.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// NOTE: We set allowFetch to false in so that we always execute on a worker
	// with direct access to the data. We want to do that because this step is
	// generally very cheap / fast, and transferring data is not worth the effort
	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	//err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, schedFetch(sector, stores.FTCache|stores.FTSealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, schedNop, func(ctx context.Context, w Worker) error {
		wInfo, _ := w.Info(ctx)
		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		taskRd.taskStatus = COMMIT1_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is computing Commit1 in sectorID[%+v]", wInfo.Hostname, sector)
		go m.sched.StartStore(sector.Number, sealtasks.TTCommit1, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
		if err != nil {
			return err
		}
		out = p

		// 找不到合适的路径并且传输数据，否则使用miner的存储
		destPath, sID := m.FindBestStoragePathToPushData(ctx, sector, w)
		if destPath == "" {
			logrus.SchedLogger.Errorf("===== sector(%+v) finished Commit1 but not found a good storage path, replace destPath %s", sector, destPath)
			go m.sched.StartStore(sector.Number, sealtasks.TTCommit1, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())
			//任务结束，更改taskRecorder状态
			taskRd.taskStatus = COMMIT2_WAITTING
			m.sched.taskRecorder.Store(sector, taskRd)
			logrus.SchedLogger.Infof("===== worker %s is COMMIT2_WAITTING in sectorID[%+v]", wInfo.Hostname, sector)
			return nil
			//return xerrors.Errorf("sector(%+v) finished Commit1 but not found a good storage path", sector)
		}

		// 申明新的存储路径
		err = m.index.StorageDeclareSector(ctx, sID, sector, stores.FTSealed|stores.FTCache, true)
		if err != nil {
			// 如果失败则删除任务记录，无法恢复
			m.sched.taskRecorder.Delete(sector)
			log.Errorf("===== after sector(%+v) finished Commit1 and transfor data to destPath(%+v),but  failed to StorageDeclareSector. err:%+v", sector, destPath, err)
			return xerrors.Errorf("===== after sector(%+v) finished Commit1 and transfor data to destPath(%+v),but  failed to StorageDeclareSector. err:%+v", sector, destPath, err)
		}

		go m.sched.StartStore(sector.Number, sealtasks.TTCommit1, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = COMMIT2_WAITTING
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is COMMIT2_WAITTING in sectorID[%+v]", wInfo.Hostname, sector)
		return nil
	})

	return out, err
}

func (m *Manager) FindBestStoragePathToPushData(ctx context.Context, sector abi.SectorID, w Worker) (string, stores.ID) {
	// 筛选存储机器
	sis, err := m.index.StorageBestAlloc(ctx, stores.FTSealed, abi.RegisteredSealProof_StackedDrg32GiBV1, stores.PathStorage)
	//m.index.StorageList
	if err != nil {
		logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server,finding best storage error : %w", sector, err)
		return "", ""
	}

	storagePaths, err := m.StorageLocal(ctx)
	if err != nil {
		logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server,finding storagePaths error : %w", sector, err)
		return "", ""
	}

	var hasNFS bool
	var transErrCount int
RetryFindStorage:
	//var bestSi stores.StorageInfo
	for _, si := range sis {
		p, ok := storagePaths[si.ID]
		if !ok {
			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, but not found storage ID(%+v)", sector, si.ID)
			continue
		}

		if p == "" { // TODO: can that even be the case?
			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, but not found storage path is null", sector)
			continue
		}

		// 尝试 开始发送 通过解析p.local来获取NFS ip
		ip, destPath := stores.PareseDestFromePath(p)
		if ip == "" {
			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, Parse path(%+v) error,not find dest ip", sector, p)
		} else if destPath == "" {
			logrus.SchedLogger.Errorf("try to send sector (%+v) to storage Server, Parse path(%+v) error,not find dest path", sector, p)
		} else {
			logrus.SchedLogger.Infof("===== find best destPath(%+v) ip(%+v)", destPath, ip)
			hasNFS = true
			if w == nil {
				return p, si.ID
			}

			// 检测连通行
			ok, err := stores.ConnectTest(destPath+"/firefly-miner", ip)
			if !ok || err != nil {
				continue
			}

			// 绑定传输，设置传输数量最多同时为8
			m.transLK.Lock()
			_, ok = m.transIP2Count.Load(ip)
			if !ok {
				m.transIP2Count.Store(ip, 0)
			}
			v, _ := m.transIP2Count.Load(ip)
			if v.(int) >= m.maxTransCount {
				log.Infof("===== 当前有超过%d个传输任务向IP(%+v)传输数据,配置最大值为%d", v.(int), ip, m.maxTransCount)
				m.transLK.Unlock()
				continue
			}
			m.transIP2Count.Store(ip, v.(int)+1)
			//v, _ = m.transIP2Count.Load(ip)
			log.Infof("ip(%s) transfor task count is %d,sector(%+v) try to it", ip, v.(int)+1, sector)
			m.transLK.Unlock()

			// 开始传输数据
			err = w.PushDataToStorage(ctx, sector, p)

			m.transLK.Lock()
			v, _ = m.transIP2Count.Load(ip)
			m.transIP2Count.Store(ip, v.(int)-1)
			m.transLK.Unlock()

			if err != nil {
				// 如果发送错误次数超过10，则放弃，可能数据有问题
				transErrCount += 1
				log.Errorf("===== after sector(%+v) finished Commit1,but push data to Storage(%+v) failed. err:%+v", sector, p, err)
			} else {
				// 传输成功，返回
				//log.Infof("ip(%s) transfor task count is %d,sector(%+v) success send to it", ip, v.(int)-1, sector)
				logrus.SchedLogger.Infof("===== finished sector(%+v) Commit1 transfored to destPath(%+v) success!!", sector, p)
				log.Infof("===== finished sector(%+v) Commit1 transfored to destPath(%+v) success!!", sector, p)
				return p, si.ID
			}
		}
	}
	//if bestSi.ID == "" {
	if hasNFS {
		log.Warnf("all storage transfor task count is more then %d, please check it", m.maxTransCount)
		if transErrCount < 10 {
			time.Sleep(time.Minute)
			goto RetryFindStorage
		} else {
			log.Errorf("try to send sector (%+v) to storage Server, error more then %d times", sector, 10)
			return "", ""
		}
	}
	log.Errorf("try to send sector (%+v) to storage Server, can not find any storage Server", sector)
	//}
	return "", ""
}
func (m *Manager) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
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
		go m.sched.StartStore(sector.Number, sealtasks.TTCommit2, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}
		out = p
		go m.sched.StartStore(sector.Number, sealtasks.TTCommit2, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = TRANSFOR_FINISHED
		m.sched.taskRecorder.Store(sector, taskRd)
		logrus.SchedLogger.Infof("===== worker %s is TRANSFOR_FINISHED in sectorID[%+v]", wInfo.Hostname, sector)
		return nil
	})

	return out, err
}

func (m *Manager) FinalizeSector(ctx context.Context, sector abi.SectorID, keepUnsealed []storage.Range) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	//if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTSealed|stores.FTUnsealed|stores.FTCache); err != nil {
	//	return xerrors.Errorf("acquiring sector lock: %w", err)
	//}
	//
	//unsealed := stores.FTUnsealed
	//{
	//	unsealedStores, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
	//	if err != nil {
	//		return xerrors.Errorf("finding unsealed sector: %w", err)
	//	}
	//
	//	if len(unsealedStores) == 0 { // Is some edge-cases unsealed sector may not exist already, that's fine
	//		unsealed = stores.FTNone
	//	}
	//}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	err := m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		//schedFetch(sector, stores.FTCache|stores.FTSealed|unsealed, stores.PathSealing, stores.AcquireMove),
		schedNop,
		func(ctx context.Context, w Worker) error {
			wInfo, _ := w.Info(ctx)
			logrus.SchedLogger.Infof("===== worker %s start do %s for sector %d mod keepUnseald 0", wInfo.Hostname, sealtasks.TTFinalize, sector)
			m.sched.taskRecorder.Delete(sector)
			// 找不到合适的路径让commit1执行失败并且不要删除数据
			destPath, _ := m.FindBestStoragePathToPushData(ctx, sector, nil)
			if destPath == "" {
				// if not mount nfs disk then miner fetch form worker
				return w.FinalizeSector(ctx, sector, []storage.Range{})
			}
			return nil
			//return w.FinalizeSector(ctx, sector, keepUnsealed)
			//return w.FinalizeSector(ctx, sector, []storage.Range{})
		})
	if err != nil {
		return err
	}

	//fetchSel := newAllocSelector(m.index, stores.FTCache|stores.FTSealed, stores.PathStorage)
	//moveUnsealed := unsealed
	//{
	//	if len(keepUnsealed) == 0 {
	//		moveUnsealed = stores.FTNone
	//	}
	//}

	// 禁止拉取数据,修复FinalizeFaild
	//err = m.sched.Schedule(ctx, sector, sealtasks.TTFetch, fetchSel,
	//	//schedFetch(sector, stores.FTCache|stores.FTSealed|moveUnsealed, stores.PathStorage, stores.AcquireMove),
	//	schedNop,
	//	func(ctx context.Context, w Worker) error {
	//		err := w.MoveStorage(ctx, sector)
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

func (m *Manager) ReleaseUnsealed(ctx context.Context, sector abi.SectorID, safeToFree []storage.Range) error {
	logrus.SchedLogger.Warn("ReleaseUnsealed todo")
	return nil
}

func (m *Manager) Remove(ctx context.Context, sector abi.SectorID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector, stores.FTNone, stores.FTSealed|stores.FTUnsealed|stores.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	unsealed := stores.FTUnsealed
	{
		unsealedStores, err := m.index.StorageFindSector(ctx, sector, stores.FTUnsealed, 0, false)
		if err != nil {
			return xerrors.Errorf("finding unsealed sector: %w", err)
		}

		if len(unsealedStores) == 0 { // can be already removed
			unsealed = stores.FTNone
		}
	}

	selector := newExistingSelector(m.index, sector, stores.FTCache|stores.FTSealed, false)

	return m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		schedFetch(sector, stores.FTCache|stores.FTSealed|unsealed, stores.PathStorage, stores.AcquireMove),
		func(ctx context.Context, w Worker) error {
			return w.Remove(ctx, sector)
		})
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

func (m *Manager) SchedDiag(ctx context.Context) (interface{}, error) {
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
