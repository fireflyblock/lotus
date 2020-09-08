package sectorstorage

import (
	"context"
	logrus "github.com/filecoin-project/sector-storage/log"
	"os"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0

//var SelectorTimeout = 5 * time.Second
//var InitWait = 3 * time.Second
//
//var (
//	SchedWindows = 2
//)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	spt abi.RegisteredSealProof

	workersLk  sync.RWMutex
	nextWorker WorkerID
	workers    map[WorkerID]*workerHandle

	newWorkers chan *workerHandle

	tasks             sync.Map //map[string]*taskCounter
	taskRecorder      sync.Map //map[abi.SectorID]*sectorTaskRecord
	workerAndID       map[string]WorkerID
	workScopeRecorder *ScopeOfWork
	isExistFreeWorker bool
	masterSwitch      bool

	watchClosing  chan WorkerID
	workerClosing chan WorkerID

	schedule chan *workerRequest
	// 拉取数据channel
	transferChannel sync.Map

	// owned by the sh.runSched goroutine
	schedQueue *requestQueue
	info       chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

type workerHandle struct {
	w Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk sync.Mutex
	//activeWindows []*schedWindow

	// stats / tracking
	wt *workTracker

	taskConf  *TaskConfig
	WorkScope ScopeType

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

//type schedWindowRequest struct {
//	worker WorkerID
//
//	done chan *schedWindow
//}

//type schedWindow struct {
//	allocated activeResources
//	todo      []*workerRequest
//}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cpuCount uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	//start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler(spt abi.RegisteredSealProof) *scheduler {
	return &scheduler{
		spt: spt,

		nextWorker: 0,
		workers:    map[WorkerID]*workerHandle{},

		newWorkers: make(chan *workerHandle),

		tasks:             sync.Map{},
		taskRecorder:      sync.Map{},
		workerAndID:       map[string]WorkerID{},
		workScopeRecorder: &ScopeOfWork{},
		isExistFreeWorker: true,
		masterSwitch:      true,

		watchClosing:  make(chan WorkerID),
		workerClosing: make(chan WorkerID),

		schedule: make(chan *workerRequest),

		schedQueue: &requestQueue{},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	logrus.SchedLogger.Infof("===== new req sched, sectorID:%+v, taskType:%+v\n", sector, taskType)

	if taskType == sealtasks.TTAddPiecePl {
		if sh.isExistFreeWorker && sh.masterSwitch {
			//taskType = sealtasks.TTAddPiece
		} else {
			logrus.SchedLogger.Warnf("===== no free workers , schedQueue:%+v, sectorID:%+v, taskType:%+v, isExistFreeWorker:%+v, masterSwitch:%+v\n",
				sh.schedQueue.Len(), sector, taskType, sh.isExistFreeWorker, sh.masterSwitch)
			return xerrors.Errorf("no free workers , schedQueue:%+v\n", sh.schedQueue.Len())
		}
	}

	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		//start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		logrus.SchedLogger.Warnf("===== request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []WorkerID
}

func (sh *scheduler) runSched() {
	defer close(sh.closed)

	go sh.runWorkerWatcher()

	for {
		select {
		case w := <-sh.newWorkers:
			sh.newWorker(w)
			sh.isExistFreeWorker = true
			sh.trySched()

		case wid := <-sh.workerClosing:
			sh.dropWorker(wid)

		case req := <-sh.schedule:

			if req.taskType == sealtasks.TTAddPiece {
				tr, ok := sh.taskRecorder.Load(req.sector)
				if !ok {
					logrus.SchedLogger.Infof("===== new req comming seal, schedQueue %d queued, sectorID:%+v, taskType:%+v, "+
						"isExistFreeWorker:%+v, masterSwitch:%+v\n",
						sh.schedQueue.Len(), req.sector, req.taskType, sh.isExistFreeWorker, sh.masterSwitch)
				} else {
					taskRd := tr.(sectorTaskRecord)
					logrus.SchedLogger.Infof("===== new req again, schedQueue %d queued, sectorID:%+v, taskType:%+v, "+
						"worker:%+v, isExistFreeWorker:%+v, masterSwitch:%+v\n",
						sh.schedQueue.Len(), req.sector, req.taskType, taskRd.workerFortask, sh.isExistFreeWorker, sh.masterSwitch)
				}
			} else {
				logrus.SchedLogger.Infof("===== new req comming, schedQueue %d queued, sectorID:%+v, taskType:%+v, "+
					"isExistFreeWorker:%+v, masterSwitch:%+v",
					sh.schedQueue.Len(), req.sector, req.taskType, sh.isExistFreeWorker, sh.masterSwitch)
			}

			sh.schedQueue.Push(req)
			go sh.StartStore(req.sector.Number, req.taskType, "", req.sector.Miner, TS_WAITING, time.Now())
			sh.trySched()

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}

		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-sh.closing:
			sh.schedClose()
			return
		}
	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	return out
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/
	var worker0 WorkerID = 0

	logrus.SchedLogger.Infof("SCHED %d queued", sh.schedQueue.Len())

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	// Step 1   首先先选出合适的windows，一个window表示一个worker及其做过的任务信息
	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		var acceptableWorkerID WorkerID
		var acceptableWorker string
		task := (*sh.schedQueue)[sqi]

		_, ok := sh.taskRecorder.Load(task.sector)
		if !ok {
			sh.taskRecorder.Store(task.sector, sectorTaskRecord{})
		}
		tr, _ := sh.taskRecorder.Load(task.sector)
		taskRd := tr.(sectorTaskRecord)

		task.indexHeap = sqi

		hostName, err := os.Hostname()
		if err != nil {
			panic(err)
		}

		switch task.taskType {
		case sealtasks.TTFinalize, sealtasks.TTFetch, sealtasks.TTReadUnsealed, sealtasks.TTUnseal:
			logrus.SchedLogger.Infof("===== taskType is :%+v, sqi:%d, sector %d,hostname:%+v,", task.taskType, sqi, task.sector.Number, sh.workers[worker0].info.Hostname)
			sh.assignWorker(worker0, sh.workers[worker0], task)
			sh.schedQueue.Remove(sqi)
			sqi--
			continue

		case sealtasks.TTAddPiecePl:
			//task.taskType = sealtasks.TTAddPiece
			var isFreeWorker bool
			for wid, worker := range sh.workers {
				// 过滤miner不做addpiece
				if worker.info.Hostname == hostName {
					continue
				}

				// 过滤 C2/deal woker不做addpice
				if worker.WorkScope == PRIORITYCOMMIT2 || worker.WorkScope == PRIORITYSEAL {
					continue
				}

				if !sh.canHandleRequestForTask(task.taskType, worker.info.Hostname, task.sector, wid) {
					logrus.SchedLogger.Infof("===== [canHandleRequestForTask] "+
						"workerID:%+v, Worker:%s, sector:%+v，type:%+v, sqi:%+v\n",
						wid, worker.info.Hostname, task.sector, task.taskType, sqi)
					continue
				}
				logrus.SchedLogger.Infof("===== assign pledge addpiece worker:%+v, wid:%+v, sectorid(%+v)，taskType:%s", worker.info.Hostname, wid, task.sector, task.taskType)
				sh.assignWorker(wid, worker, task)

				//bind
				taskRd.taskStatus = ADDPIECE_WAITING
				taskRd.workerFortask = worker.info.Hostname
				sh.taskRecorder.Store(task.sector, taskRd)
				logrus.SchedLogger.Infof("===== start bind , worker [%s] , workerID [%+v] , sectorID [%+v] \n",
					worker.info.Hostname, wid, task.sector)

				sh.schedQueue.Remove(sqi)
				sqi--
				isFreeWorker = true
				break
			}
			//TODO
			if !isFreeWorker {
				sh.isExistFreeWorker = false
				logrus.SchedLogger.Infof("===== CLOSE PLEDGE, sectorid(%+v)，taskType:%s", task.sector, task.taskType)
			}
			// 次数说明addpiece任务无论成功与否都应该continue,
			continue

		case sealtasks.TTAddPiece:
			if taskRd.workerFortask != "" {
				acceptableWorker = taskRd.workerFortask
				wid, ok := sh.workerAndID[acceptableWorker]
				if ok {
					acceptableWorkerID = wid
					logrus.SchedLogger.Infof("===== assign again deal ap worker:%+v, wid:%+v, sectorid(%+v)，taskType:%s", acceptableWorker, acceptableWorkerID, task.sector, task.taskType)
					sh.assignWorker(acceptableWorkerID, sh.workers[acceptableWorkerID], task)
					sh.schedQueue.Remove(sqi)
					sqi--
					continue
				}
				logrus.SchedLogger.Warnf("===== worker closed , worker:%+v, sectorid(%+v)，taskType:%s", acceptableWorker, task.sector, task.taskType)
				continue
			}
			if sh.workScopeRecorder.pick(PRIORITYSEAL) != "" {
				//1.find
				logrus.SchedLogger.Infof("===== find a suitable deal worker [%s] ", sh.workScopeRecorder.pick(PRIORITYSEAL))
				acceptableWorker = sh.workScopeRecorder.pick(PRIORITYSEAL)
				wid, ok := sh.workerAndID[acceptableWorker]
				if ok {
					//2.assign
					acceptableWorkerID = wid
					logrus.SchedLogger.Infof("===== assign deal ap worker:%+v, wid:%+v, sectorid(%+v)，taskType:%s", acceptableWorker, acceptableWorkerID, task.sector, task.taskType)
					sh.assignWorker(acceptableWorkerID, sh.workers[acceptableWorkerID], task)
					//3.bind
					taskRd.taskStatus = ADDPIECE_WAITING
					taskRd.workerFortask = acceptableWorker
					sh.taskRecorder.Store(task.sector, taskRd)
					logrus.SchedLogger.Infof("===== start bind , worker [%s] , workerID [%+v] , sectorID [%+v] \n",
						acceptableWorker, acceptableWorkerID, task.sector)

					sh.schedQueue.Remove(sqi)
					sqi--
					continue
				}
				logrus.SchedLogger.Warnf("===== worker closed , worker:%+v, sectorid(%+v)，taskType:%s", acceptableWorker, task.sector, task.taskType)
				continue
			}
			logrus.SchedLogger.Infof("===== deal ap worker not exist, workScopeRecorder:%+v", sh.workScopeRecorder)
			continue

		case sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1:
			if taskRd.workerFortask != "" {
				acceptableWorker = taskRd.workerFortask
				wid, ok := sh.workerAndID[acceptableWorker]
				if ok {
					acceptableWorkerID = wid
					goto Judge
				}
				logrus.SchedLogger.Warnf("===== worker closed , worker:%+v, sectorid(%+v)，taskType:%s", acceptableWorker, task.sector, task.taskType)
				continue
			}
			continue

		case sealtasks.TTCommit2:
			if sh.workScopeRecorder.pick(PRIORITYCOMMIT2) != "" {
				logrus.SchedLogger.Infof("===== find a suitable c2 worker [%s] ", sh.workScopeRecorder.pick(PRIORITYCOMMIT2))
				acceptableWorker = sh.workScopeRecorder.pick(PRIORITYCOMMIT2)
				wid, ok := sh.workerAndID[acceptableWorker]
				if ok {
					acceptableWorkerID = wid
					goto Judge
				}
				logrus.SchedLogger.Warnf("===== worker closed , worker:%+v, sectorid(%+v)，taskType:%s", acceptableWorker, task.sector, task.taskType)
				continue
			}
			logrus.SchedLogger.Infof("===== c2 worker not exist, workScopeRecorder:%+v", sh.workScopeRecorder)
			continue

		default:
			logrus.SchedLogger.Warnf("===== %+v not in the range of know types", task.taskType)
			continue
		}

	Judge:
		{
			if !sh.canHandleRequestForTask(task.taskType, acceptableWorker, task.sector, acceptableWorkerID) {
				logrus.SchedLogger.Infof("===== [canHandleRequestForTask] "+
					"workerID:%+v, Worker:%s, sector:%+v，type:%+v, sqi:%+v\n", acceptableWorkerID, acceptableWorker, task.sector, task.taskType, sqi)
				continue
			}
			logrus.SchedLogger.Infof("===== assign worker:%+v, wid:%+v, sectorid(%+v)，taskType:%s", acceptableWorker, acceptableWorkerID, task.sector, task.taskType)
			err := sh.assignWorker(acceptableWorkerID, sh.workers[acceptableWorkerID], task)
			if err != nil {
				logrus.SchedLogger.Error("assignWorker error: %+v", err)
				go task.respond(xerrors.Errorf("assignWorker error: %w", err))
			}
			sh.schedQueue.Remove(sqi)
			sqi--
		}
	}
}

func (sh *scheduler) updateTransforCount(updatetype int) {
	// 记录等待传输的数量
	tc, _ := sh.transferChannel.LoadOrStore("wCount", 0)
	wCount, _ := tc.(int)

	if updatetype == 1 {
		wCount += 1
	} else {
		wCount -= 1
	}

	sh.transferChannel.Store("wCount", wCount)
	logrus.SchedLogger.Infof("===== total wait transforing count is :%d\n", wCount)
}

// 拉取数据
func (sh *scheduler) tryFetchData(wid WorkerID, req *workerRequest) {
	// 开始调度C2 再去拉取数据
	if req.taskType != sealtasks.TTCommit2 {
		return
	}

	logrus.SchedLogger.Infof("===== before sector(%+v) to do SealCommit2 tryFetchData", req.sector)

	// transferChannel存储key：wid，value：channel
	// 存储channel用于计算任务完成后检测传输是否完成，如果未完成则等待传输完成。
	tch := make(chan abi.SectorID)
	sh.transferChannel.Store(req.sector, tch)

	// 更新传输任务等待状态
	sh.updateTransforCount(1)

	// 默认使用miner拉取
	sh.workersLk.Lock()
	w := sh.workers[WorkerID(0)]
	sh.workersLk.Unlock()

	// 执行拉取
	transforStartAt := time.Now()
	_ = w.w.FetchRealData(req.ctx, req.sector)
	logrus.SchedLogger.Infof("===== sector(%+v) transfor cost time : %s", req.sector, time.Now().Sub(transforStartAt))
	//time.Sleep(time.Minute * 1)

	// 写入数据完成信号
	tch <- req.sector
}

func (sh *scheduler) assignWorker(wid WorkerID, w *workerHandle, req *workerRequest) error {
	logrus.SchedLogger.Infof("===== assignfree, sector:%+v, tasktype:%+v", req.sector, req.taskType)
	needRes := ResourceTable[req.taskType][sh.spt]

	w.lk.Lock()
	w.preparing.add(w.info.Resources, needRes)
	w.lk.Unlock()

	go func() {
		// 拉取数据
		//go sh.tryFetchData(wid, req)

		//defer sh.freeTask(req.taskType, w.info.Hostname, req.sector)
		defer func() {
			logrus.SchedLogger.Infof("===== defer free, sector:%+v, tasktype:%+v", req.sector, req.taskType)
			sh.freeTask(req.taskType, w.info.Hostname, req.sector)
		}()

		err := req.prepare(req.ctx, w.wt.worker(w.w))
		sh.workersLk.Lock()

		if err != nil {
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			//sh.freeTask(req.taskType, w.info.Hostname, req.sector)
			sh.workersLk.Unlock()

			select {
			case <-sh.closing:
				logrus.SchedLogger.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				logrus.SchedLogger.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				logrus.SchedLogger.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		err = w.active.withResources(wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.lk.Lock()
			w.preparing.free(w.info.Resources, needRes)
			w.lk.Unlock()
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			err = req.work(req.ctx, w.wt.worker(w.w))

			//sh.freeTask(req.taskType, w.info.Hostname, req.sector)

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				logrus.SchedLogger.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				logrus.SchedLogger.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			logrus.SchedLogger.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (sh *scheduler) newWorker(w *workerHandle) {
	w.closedMgr = make(chan struct{})
	w.closingMgr = make(chan struct{})

	sh.workersLk.Lock()

	id := sh.nextWorker
	sh.workers[id] = w
	sh.workerAndID[w.info.Hostname] = id
	sh.nextWorker++

	sh.workersLk.Unlock()

	select {
	case sh.watchClosing <- id:
	case <-sh.closing:
		return
	}
}

func (sh *scheduler) dropWorker(wid WorkerID) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	w := sh.workers[wid]
	delete(sh.workerAndID, w.info.Hostname)

	sh.workerCleanup(wid, w)
	delete(sh.workers, wid)
}

func (sh *scheduler) workerCleanup(wid WorkerID, w *workerHandle) {
	select {
	case <-w.closingMgr:
	default:
		close(w.closingMgr)
	}

	sh.workersLk.Unlock()
	select {
	case <-w.closedMgr:
	case <-time.After(time.Second):
		logrus.SchedLogger.Errorf("timeout closing worker manager goroutine %d:%+v", wid, w.info.Hostname)
	}
	sh.workersLk.Lock()

	if !w.cleanupStarted {
		w.cleanupStarted = true
		logrus.SchedLogger.Infof("dropWorker %d:%+v", wid, w.info.Hostname)

		go func() {
			if err := w.w.Close(); err != nil {
				logrus.SchedLogger.Warnf("closing worker %d: %+v", err)
			}
		}()
	}
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	logrus.SchedLogger.Infof("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
