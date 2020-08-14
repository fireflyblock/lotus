package sectorstorage

import (
	"math"
	"sync"

	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/storiface"
	"github.com/filecoin-project/specs-actors/actors/abi"
)

func (a *activeResources) withResources(id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	for !a.canHandleRequest(r, id, wr) {
		if a.cond == nil {
			a.cond = sync.NewCond(locker)
		}
		a.cond.Wait()
	}

	a.add(wr, r)

	err := cb()

	a.free(wr, r)
	if a.cond != nil {
		a.cond.Broadcast()
	}

	return err
}

func (a *activeResources) add(wr storiface.WorkerResources, r Resources) {
	a.gpuUsed = r.CanGPU
	if r.MultiThread() {
		//a.cpuUse += wr.CPUs
		a.cpuCount += wr.CPUs
	} else {
		//a.cpuUse += uint64(r.Threads)
		a.cpuCount += uint64(r.Threads)
	}

	// 修改cpu计数
	if a.cpuCount >= wr.CPUs {
		a.cpuUse = wr.CPUs
	} else {
		a.cpuUse = a.cpuCount
	}

	a.memUsedMin += r.MinMemory
	a.memUsedMax += r.MaxMemory
}

func (a *activeResources) free(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsed = false
	}
	if r.MultiThread() {
		//a.cpuUse -= wr.CPUs
		a.cpuCount -= wr.CPUs
	} else {
		//a.cpuUse -= uint64(r.Threads)
		a.cpuCount -= uint64(r.Threads)
	}

	// 修改cpu计数
	if a.cpuCount >= wr.CPUs {
		a.cpuUse = wr.CPUs
	} else {
		a.cpuUse = a.cpuCount
	}

	a.memUsedMin -= r.MinMemory
	a.memUsedMax -= r.MaxMemory
}

func (a *activeResources) canHandleRequest(needRes Resources, wid WorkerID, res storiface.WorkerResources) bool {

	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + a.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	if minNeedMem > res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d; not enough physical memory - need: %dM, have %dM", wid, minNeedMem/mib, res.MemPhysical/mib)
		return false
	}

	maxNeedMem := res.MemReserved + a.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory

	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d; not enough virtual memory - need: %dM, have %dM", wid, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib)
		return false
	}

	//if needRes.MultiThread() {
	//	if a.cpuUse > 0 {
	//		log.Debugf("sched: not scheduling on worker %d; multicore process needs %d threads, %d in use, target %d", wid, res.CPUs, a.cpuUse, res.CPUs)
	//		return false
	//	}
	//} else {
	//	if a.cpuUse+uint64(needRes.Threads) > res.CPUs {
	//		log.Debugf("sched: not scheduling on worker %d; not enough threads, need %d, %d in use, target %d", wid, needRes.Threads, a.cpuUse, res.CPUs)
	//		return false
	//	}
	//}
	//
	//if len(res.GPUs) > 0 && needRes.CanGPU {
	//	if a.gpuUsed {
	//		log.Debugf("sched: not scheduling on worker %d; GPU in use", wid)
	//		return false
	//	}
	//}

	return true
}

// 检测
func (sh *scheduler) tryCanHandleRequestForTask(taskType sealtasks.TaskType, workerHostName string, sectorID abi.SectorID, wid WorkerID) bool {
	var judge bool

	v, ok := sh.tasks.Load(workerHostName)
	if !ok {
		sh.tasks.Store(workerHostName, &taskCounter{})
		v, _ = sh.tasks.Load(workerHostName)
	}

	if sh.workers[wid].taskConf == nil {
		sh.workers[wid].taskConf = &TaskConfig{}
	}
	//log.Infof("========== try check taskConfig, workerHostName: [%+v] ,tasks: [%+v] , taskConf: [%+v] ", workerHostName, v.(*taskCounter), sh.workers[wid].taskConf)
	//log.Infof("========== sectorFilter 1: [%+v] ", sh.sectorFilter(workerHostName, wid, sealtasks.TTPreCommit1))
	//log.Infof("========== sectorFilter 2: [%+v] ", sh.sectorFilter(workerHostName, wid, sealtasks.TTPreCommit2))
	switch taskType {
	case sealtasks.TTAddPiece:
		if v.(*taskCounter).addpiece < sh.workers[wid].taskConf.AddPieceSize &&
			v.(*taskCounter).precommit1 < sh.workers[wid].taskConf.Pre1CommitSize &&
			sh.sectorFilter(workerHostName, wid, sealtasks.TTPreCommit1) &&
			sh.sectorFilter(workerHostName, wid, sealtasks.TTPreCommit2) {
			judge = true
		}
	case sealtasks.TTPreCommit1:
		if v.(*taskCounter).precommit1 < sh.workers[wid].taskConf.Pre1CommitSize {
			judge = true
		}
	case sealtasks.TTPreCommit2:
		if v.(*taskCounter).precommit2 < sh.workers[wid].taskConf.Pre2CommitSize {
			judge = true
		}
	case sealtasks.TTCommit1:
		if v.(*taskCounter).commit1 < sh.workers[wid].taskConf.Commit1 {
			judge = true
		}
	case sealtasks.TTCommit2:
		if v.(*taskCounter).commit2 < sh.workers[wid].taskConf.Commit2 {
			// TODO 对于C2的worker如果要放开限制，是否可以在此处设置
			judge = true
		}
	default:
		log.Warnf("========== addTask--->found task %s ,we are not care!!!!", taskType)
		return true
	}

	if judge {
		log.Debugf("\n========================= check Task status "+
			"check workerHostName :%s can do  %s task for sector(%d) ----->worker task status  {addpiece:%d,precommit1:%d,precommit2:%d,commit1:%d,commit2:%d}\n",
			workerHostName, taskType, sectorID, v.(*taskCounter).addpiece, v.(*taskCounter).precommit1, v.(*taskCounter).precommit2, v.(*taskCounter).commit1, v.(*taskCounter).commit2)
		return true
	} else {
		log.Debugf("\n========================= check Task status "+
			"check workerHostName :%s can't do  %s task for sector(%d) ----->worker task status  {addpiece:%d,precommit1:%d,precommit2:%d,commit1:%d,commit2:%d}\n",
			workerHostName, taskType, sectorID, v.(*taskCounter).addpiece, v.(*taskCounter).precommit1, v.(*taskCounter).precommit2, v.(*taskCounter).commit1, v.(*taskCounter).commit2)
		return false
	}
}

func (sh *scheduler) canHandleRequestForTask(taskType sealtasks.TaskType, workerHostName string, sectorID abi.SectorID, wid WorkerID) bool {
	v, ok := sh.tasks.Load(workerHostName)
	if !ok {
		sh.tasks.Store(workerHostName, &taskCounter{})
		v, _ = sh.tasks.Load(workerHostName)
	}
	v.(*taskCounter).tasksLK.Lock()
	defer v.(*taskCounter).tasksLK.Unlock()

	//log.Infof("========== check taskConfig, workerHostName: [%+v] , sectorID: [%+v] , tasks: [%+v] ,taskConf: [%+v] ", workerHostName, sectorID, v.(*taskCounter), sh.workers[wid].taskConf)
	if sh.tryCanHandleRequestForTask(taskType, workerHostName, sectorID, wid) {
		switch taskType {
		case sealtasks.TTAddPiece:
			v.(*taskCounter).addpiece++
		case sealtasks.TTPreCommit1:
			v.(*taskCounter).precommit1++
		case sealtasks.TTPreCommit2:
			v.(*taskCounter).precommit2++
		case sealtasks.TTCommit1:
			v.(*taskCounter).commit1++
		case sealtasks.TTCommit2:
			v.(*taskCounter).commit2++
		default:
			log.Warnf("========== addTask--->found task %s ,we are not care!!!!", taskType)
			return true
		}
		sh.tasks.Store(workerHostName, v.(*taskCounter))
		log.Debugf("\n========================= select worker\n"+
			"select workerHostName :%s to do  %s task for sector(%d) ----->worker task status  {addpiece:%d,precommit1:%d,precommit2:%d,commit1:%d,commit2:%d}\n",
			workerHostName, taskType, sectorID, v.(*taskCounter).addpiece, v.(*taskCounter).precommit1, v.(*taskCounter).precommit2, v.(*taskCounter).commit1, v.(*taskCounter).commit2)
		return true
	} else {
		log.Errorf("\n========================= check Task status \n"+
			"select workerHostName :%s can't do  %s task for sector(%d) ----->worker task status  {addpiece:%d,precommit1:%d,precommit2:%d,commit1:%d,commit2:%d}\n",
			workerHostName, taskType, sectorID, v.(*taskCounter).addpiece, v.(*taskCounter).precommit1, v.(*taskCounter).precommit2, v.(*taskCounter).commit1, v.(*taskCounter).commit2)
		return false
	}
}

// 检查workerHostName是否能够添加AddPiece任务
// 判断逻辑为：
// 1 正在计算的的AddPiece数量小于AddPiece配置值
// 2 等待和正在计算的Pre1任务数量和必须小于Pre1配置值
// 3 等待和正在计算的Pre2任务数量和必须小于Pre1配置值
func (sh *scheduler) sectorFilter(workerHostName string, wid WorkerID, taskType sealtasks.TaskType) bool {
	var countForpre1Waiting uint
	var countForpre1Computing uint
	var countForpre2Waiting uint
	var countForpre2Computing uint
	var rate float64 = float64(100) / float64(100)
	//拿出pre1已做完的任务数量
	sh.taskRecorder.Range(func(key, value interface{}) bool {
		if value.(sectorTaskRecord).workerFortask == workerHostName {
			if value.(sectorTaskRecord).taskStatus == PRE1_WAITTING {
				countForpre1Waiting++
				//return false
			}
			if value.(sectorTaskRecord).taskStatus == PRE1_COMPUTING {
				countForpre1Computing++
				//return false
			}
			if value.(sectorTaskRecord).taskStatus == PRE2_WAITING {
				countForpre2Waiting++
				//return false
			}
			if value.(sectorTaskRecord).taskStatus == PRE2_COMPUTING {
				countForpre2Computing++
				//return false
			}
		}
		return true
	})
	// TODO 此日志可以考虑删除
	//log.Infof("========== sectorFilter: [%+v] ", taskType)
	//log.Infof("========== workerHostName: [%+v] ,countForpre1Waiting: [%+v] , countForpre1Computing: [%+v] ", workerHostName, countForpre1Waiting, countForpre1Computing)
	//log.Infof("========== workerHostName: [%+v] ,countForpre2Waiting: [%+v] , countForpre2Computing: [%+v] ", workerHostName, countForpre2Waiting, countForpre2Computing)
	switch taskType {
	case sealtasks.TTPreCommit1:
		if float64(countForpre1Waiting+countForpre1Computing) <= math.Floor(float64(sh.workers[wid].taskConf.Pre1CommitSize)*rate) {
			//log.Infof("========== sectorFilter_P1:true,%s can do AddPiece task(%.1f+%.1f<%.1f)", workerHostName, countForpre1Waiting, countForpre1Computing, math.Floor(float64(sh.workers[wid].taskConf.Pre1CommitSize)*rate))
			return true
		}
	case sealtasks.TTPreCommit2:
		//p2等待和p2计算中的总数量小于p1设置的总数，目的让刷单可以流动起来，其次对p1数量加个百分之75的比率，向下取整，用来留出p2失败的容错量
		if float64(countForpre2Waiting+countForpre2Computing) <= math.Floor(float64(sh.workers[wid].taskConf.Pre1CommitSize)*rate) {
			//log.Infof("========== sectorFilter_P2:true,%s can do AddPiece task(%.1f+%.1f<%.1f)", workerHostName, countForpre2Waiting, countForpre2Computing, math.Floor(float64(sh.workers[wid].taskConf.Pre1CommitSize)*rate))
			return true
		}
	default:
		log.Warnf("========== sectorFilter:false ,Unexpect type(%s)", taskType)
		return false
	}
	return false
}

func (sh *scheduler) freeTask(taskType sealtasks.TaskType, workerHostName string, sectorID abi.SectorID) {
	v, ok := sh.tasks.Load(workerHostName)
	if !ok {
		sh.tasks.Store(workerHostName, &taskCounter{})
		return
	}

	log.Infof("================ sh.tasks: %+v\n", v)
	if v == nil {
		return
	}
	switch taskType {
	case sealtasks.TTAddPiece:
		if v.(*taskCounter).addpiece > 0 {
			v.(*taskCounter).addpiece--
			sh.tasks.Store(workerHostName, v.(*taskCounter))
		} else {
			log.Info("================ addpiece任务已经为0，无法继续释放 =================")
		}
	case sealtasks.TTPreCommit1:
		if v.(*taskCounter).precommit1 > 0 {
			v.(*taskCounter).precommit1--
			sh.tasks.Store(workerHostName, v.(*taskCounter))
		} else {
			log.Info("================ precommit1任务已经为0，无法继续释放 =================")
		}
	case sealtasks.TTPreCommit2:
		if v.(*taskCounter).precommit2 > 0 {
			v.(*taskCounter).precommit2--
			sh.tasks.Store(workerHostName, v.(*taskCounter))
		} else {
			log.Info("================ precommit2任务已经为0，无法继续释放 =================")
		}
	case sealtasks.TTCommit1:
		if v.(*taskCounter).commit1 > 0 {
			v.(*taskCounter).commit1--
			sh.tasks.Store(workerHostName, v.(*taskCounter))
		} else {
			log.Info("================ commit1任务已经为0，无法继续释放 =================")
		}
	case sealtasks.TTCommit2:
		if v.(*taskCounter).commit2 > 0 {
			v.(*taskCounter).commit2--
			sh.tasks.Store(workerHostName, v.(*taskCounter))
		} else {
			log.Info("================ commit2任务已经为0，无法继续释放 =================")
		}
	default:
		return
	}

	v, _ = sh.tasks.Load(workerHostName)
	log.Infof("\n========================= freeTask \n"+
		"workerHostName :%s free %s task of sector(%d), worker task status:{addpiece:%d,precommit1:%d,precommit2:%d,commit1:%d,commit2:%d}\n",
		workerHostName, taskType, sectorID, v.(*taskCounter).addpiece, v.(*taskCounter).precommit1, v.(*taskCounter).precommit2, v.(*taskCounter).commit1, v.(*taskCounter).commit2)

}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	if memMin > max {
		max = memMin
	}

	memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	if memMax > max {
		max = memMax
	}

	return max
}

var GB416 uint64 = 416 << 30
var GB64 uint64 = 64 << 30
var GB56 uint64 = 56 << 30
var GB2 uint64 = 2 << 30

var DiskSize uint64 = 4 << 40 //4TB
var DiskRate uint64 = 100

func CalculateResources(wr storiface.WorkerResources, disk int64) *TaskConfig {
	tc := &TaskConfig{}
	var diskSize uint64 = CalculatePercentage(uint64(disk), DiskRate)
	var memSize uint64 = wr.MemPhysical - GB2
	var p1Num uint64
	var p2Num uint64 = 4
	var c1Num uint64 = 1
	var c2Num uint64 = 3

	p1MemCount := (memSize-GB56)/GB64 - p2Num + 2 //内存容量下p1的最大数量
	log.Debugf("================ p1MemCount, p1MemCount [%d] , memSize: [%d] ", p1MemCount, memSize)
	p1DiskCount := diskSize / GB416 //磁盘容量下p1的最大数量
	log.Debugf("================ p1DiskCount, p1DiskCount [%s] ,diskSize: [%+v] ", p1DiskCount, diskSize)
	p1Num = selectMin(p1MemCount, p1DiskCount/2) //选择最小限度作为p1

	tc.AddPieceSize = uint8(p1Num)
	tc.Pre1CommitSize = uint8(p1Num)
	tc.Pre2CommitSize = uint8(p2Num)
	tc.Commit1 = uint8(c1Num)
	tc.Commit2 = c2Num

	return tc
}

func CalculatePercentage(num, per uint64) uint64 {
	rate := float64(per) / float64(100)
	res := math.Ceil(float64(num) * rate)

	return uint64(res)
}

func selectMin(a, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}
