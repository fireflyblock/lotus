package sectorstorage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/filecoin-project/sector-storage/fsutil"
	"github.com/mitchellh/go-homedir"

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
}

type SealerConfig struct {
	ParallelFetchLimit int

	// Local worker config
	AllowPreCommit1 bool
	AllowPreCommit2 bool
	AllowCommit     bool
	AllowUnseal     bool
}

type StorageAuth http.Header

func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, cfg *ffiwrapper.Config, sc SealerConfig, urls URLs, sa StorageAuth) (*Manager, error) {
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
	}

	//err = m.sched.StartLoad()
	//if err != nil {
	//	return  nil,xerrors.Errorf("========== start load task record err: %w", err)
	//}
	go m.sched.runSched()

	localTasks := []sealtasks.TaskType{
		sealtasks.TTAddPiece, sealtasks.TTCommit1, sealtasks.TTFinalize, sealtasks.TTFetch, sealtasks.TTReadUnsealed,
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
	tc := CalculateResources(info.Resources, fsS.Available)
	log.Debugf("================CalculateResources worker[%s],taskConfig:[%+v],availableDisk:%d", info.Hostname, tc, fsS.Available)
	if scope == PRIORITYCOMMIT2 {
		tc.Commit2 = 10000
		log.Debugf("================ c2 worker[%s],Resources:[%+v],WorkScope:[%+v]", info.Hostname, info.Resources, scope)
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
	log.Debugf("================ADD worker[%s],Resources:[%+v],WorkScope:[%+v],workScopeRecorder:[%+v]", info.Hostname, info.Resources, scope, m.sched.workScopeRecorder)
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
	log.Debugf("================ readpiece sector:%+v, best:%+v, selector:%+v\n",  sector, best, selector)

	if len(best) > 0 {
		// There is unsealed sector, see if we can read from it

		selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

		err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
			log.Debugf("================ readpiece TTReadUnsealed1  sector:%+v\n",  sector)
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
		log.Debugf("================ readpiece TTUnseal  sector:%+v\n",  sector)
		return w.UnsealPiece(ctx, sector, offset, size, ticket, unsealed)
	})
	if err != nil {
		return err
	}

	selector = newExistingSelector(m.index, sector, stores.FTUnsealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, schedFetch(sector, stores.FTUnsealed, stores.PathSealing, stores.AcquireMove), func(ctx context.Context, w Worker) error {
		log.Debugf("================ readpiece TTReadUnsealed2  sector:%+v\n",  sector)
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
	log.Warnf("stub NewSector")
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
	if apType == "_pledgeSector" {
		tt = sealtasks.TTAddPiecePl
	} else {
		tt = sealtasks.TTAddPiece
	}

	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, tt, selector, schedNop, func(ctx context.Context, w Worker) error {
		wInfo, _ := w.Info(ctx)
		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		taskRd.taskStatus = ADDPIECE_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		log.Debugf("================ worker %s is computing AddPiece in sectorID[%+v], apTYpe:%s", wInfo.Hostname, sector,apType)
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
		log.Debugf("================ worker %s is PRE1_WAITTING in sectorID[%+v]", wInfo.Hostname, sector)
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
		log.Debugf("================ worker %s is computing PreCommit1 in sectorID[%+v]", wInfo.Hostname, sector)
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit1, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealPreCommit1(ctx, sector, ticket, pieces)
		if err != nil {
			return err
		}
		out = p
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit1, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = PRE2_WAITING
		m.sched.taskRecorder.Store(sector, taskRd)
		log.Debugf("================ worker %s is PRE2_WAITING in sectorID[%+v]", wInfo.Hostname, sector)
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
			log.Debugf("================ SealPreCommit2 finish, worker[%s],sectorID[%+v],isExistFreeWorker[%+v]", wInfo.Hostname, sector, m.sched.isExistFreeWorker)
		}()
		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		taskRd.taskStatus = PRE2_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		log.Debugf("================ worker %s is computing PreCommit2 in sectorID[%+v]", wInfo.Hostname, sector)
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit2, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealPreCommit2(ctx, sector, phase1Out)
		if err != nil {
			return err
		}
		out = p
		go m.sched.StartStore(sector.Number, sealtasks.TTPreCommit2, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = COMMIT1_WAITTING
		m.sched.taskRecorder.Store(sector, taskRd)
		log.Debugf("================ worker %s is COMMIT1_WAITTING in sectorID[%+v]", wInfo.Hostname, sector)
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
		log.Debugf("================ worker %s is computing Commit1 in sectorID[%+v]", wInfo.Hostname, sector)
		go m.sched.StartStore(sector.Number, sealtasks.TTCommit1, wInfo.Hostname, sector.Miner, TS_COMPUTING, time.Now())
		p, err := w.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
		if err != nil {
			return err
		}
		out = p
		go m.sched.StartStore(sector.Number, sealtasks.TTCommit1, wInfo.Hostname, sector.Miner, TS_COMPLETE, time.Now())

		//任务结束，更改taskRecorder状态
		taskRd.taskStatus = COMMIT2_WAITTING
		m.sched.taskRecorder.Store(sector, taskRd)
		log.Debugf("================ worker %s is COMMIT2_WAITTING in sectorID[%+v]", wInfo.Hostname, sector)
		return nil
	})
	return out, err
}

func (m *Manager) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		wInfo, _ := w.Info(ctx)
		defer func() {
			m.sched.isExistFreeWorker = true
			log.Debugf("================ SealCommit2 finish, worker[%s],sectorID[%+v],isExistFreeWorker[%+v]", wInfo.Hostname, sector, m.sched.isExistFreeWorker)
		}()

		_, ok := m.sched.taskRecorder.Load(sector)
		if !ok {
			m.sched.taskRecorder.Store(sector, sectorTaskRecord{})
		}
		tr, _ := m.sched.taskRecorder.Load(sector)
		taskRd := tr.(sectorTaskRecord)

		taskRd.taskStatus = COMMIT2_COMPUTING
		m.sched.taskRecorder.Store(sector, taskRd)
		log.Debugf("================ worker %s is computing Commit2 in sectorID[%+v]", wInfo.Hostname, sector)
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
		log.Debugf("================ worker %s is TRANSFOR_FINISHED in sectorID[%+v]", wInfo.Hostname, sector)
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
			log.Debugf("================ worker %s start do %s for sector %d mod keepUnseald 0", wInfo.Hostname, sealtasks.TTFinalize, sector)
			//return w.FinalizeSector(ctx, sector, keepUnsealed)
			return w.FinalizeSector(ctx, sector, []storage.Range{})
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
	log.Warnw("ReleaseUnsealed todo")
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

var _ SectorManager = &Manager{}
