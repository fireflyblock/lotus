package sectorstorage

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/sector-storage/storiface"
	"golang.org/x/xerrors"
)

const EASTEREGG = 20070920

func (m *Manager) WorkerStats() map[uint64]storiface.WorkerStats {
	m.sched.workersLk.RLock()
	defer m.sched.workersLk.RUnlock()

	out := map[uint64]storiface.WorkerStats{}

	for id, handle := range m.sched.workers {
		out[uint64(id)] = storiface.WorkerStats{
			Info:       handle.info,
			MemUsedMin: handle.active.memUsedMin,
			MemUsedMax: handle.active.memUsedMax,
			GpuUsed:    handle.active.gpuUsed,
			CpuUse:     handle.active.cpuUse,
		}
	}

	return out
}

func (m *Manager) WorkerJobs() map[uint64][]storiface.WorkerJob {
	m.sched.workersLk.RLock()
	defer m.sched.workersLk.RUnlock()

	out := map[uint64][]storiface.WorkerJob{}

	for id, handle := range m.sched.workers {
		out[uint64(id)] = handle.wt.Running()

		//handle.wndLk.Lock()
		//for wi, window := range handle.activeWindows {
		//	for _, request := range window.todo {
		//		out[uint64(id)] = append(out[uint64(id)], storiface.WorkerJob{
		//			ID:      0,
		//			Sector:  request.sector,
		//			Task:    request.taskType,
		//			RunWait: wi + 1,
		//			Start:   request.start,
		//		})
		//	}
		//}
		//handle.wndLk.Unlock()
	}

	return out
}

func (m *Manager) WorkerConfSet(hostname string, config []byte) WorkerID {
	taskConf := &TaskConfig{}
	err := json.Unmarshal(config, taskConf)
	if err != nil {
		log.Errorf("json Unmarshal err")
		return 0
	}
	//log.Infof("================ 远程更改config【%+v】", taskConf)
	for wid, handle := range m.sched.workers {
		if handle.info.Hostname == hostname {
			if taskConf.AddPieceSize != 255 {
				if taskConf.AddPieceSize > handle.taskConf.AddPieceSize {
					m.sched.isExistFreeWorker = true
				}
				handle.taskConf.AddPieceSize = taskConf.AddPieceSize
			}
			if taskConf.Pre1CommitSize != 255 {
				if taskConf.Pre1CommitSize > handle.taskConf.Pre1CommitSize {
					m.sched.isExistFreeWorker = true
				}
				handle.taskConf.Pre1CommitSize = taskConf.Pre1CommitSize
			}
			if taskConf.Pre2CommitSize != 255 {
				if taskConf.Pre2CommitSize > handle.taskConf.Pre2CommitSize {
					m.sched.isExistFreeWorker = true
				}
				handle.taskConf.Pre2CommitSize = taskConf.Pre2CommitSize
			}
			if taskConf.Commit1 != 255 {
				if taskConf.Commit1 > handle.taskConf.Commit1 {
					m.sched.isExistFreeWorker = true
				}
				handle.taskConf.Commit1 = taskConf.Commit1
			}
			if taskConf.Commit2 != EASTEREGG {
				if taskConf.Commit2 > handle.taskConf.Commit2 {
					m.sched.isExistFreeWorker = true
				}
				handle.taskConf.Commit2 = taskConf.Commit2
			}
			return wid
		}
	}
	return 0
}

func (m *Manager) WorkerConfGet(hostname string) ([]TasksNumber, error) {
	//log.Infof("================ 远程查询config【%+v】", taskConf)
	var tc TaskConfig
	var ctc *taskCounter
	var tcCount uint
	var countForP1Waiting uint8
	var countForP2Waiting uint8
	var countForC1Waiting uint8

	taskList := make([]TasksNumber, 5)
	for _, handle := range m.sched.workers {
		if handle.info.Hostname == hostname {
			tc = *handle.taskConf
			tcCount++
		}
	}
	v, ok := m.sched.tasks.Load(hostname)

	//拿出pre1已做完的任务数量
	m.sched.taskRecorder.Range(func(key, value interface{}) bool {
		if value.(sectorTaskRecord).workerFortask == hostname {
			if value.(sectorTaskRecord).taskStatus == PRE1_WAITTING {
				countForP1Waiting++
			}
			if value.(sectorTaskRecord).taskStatus == PRE2_WAITING {
				countForP2Waiting++
			}
			if value.(sectorTaskRecord).taskStatus == COMMIT1_WAITTING {
				countForC1Waiting++
			}
		}
		return true
	})

	if tcCount > 0 && ok {
		ctc = v.(*taskCounter)

		taskList[0].AddPieceSize = tc.AddPieceSize
		taskList[0].Pre1CommitSize = tc.Pre1CommitSize
		taskList[0].Pre2CommitSize = tc.Pre2CommitSize
		taskList[0].Commit1 = tc.Commit1
		taskList[0].Commit2 = tc.Commit2

		taskList[1].AddPieceSize = ctc.addpiece
		taskList[1].Pre1CommitSize = ctc.precommit1
		taskList[1].Pre2CommitSize = ctc.precommit2
		taskList[1].Commit1 = ctc.commit1
		taskList[1].Commit2 = ctc.commit2
		taskList[1].P1WatingSize = countForP1Waiting
		taskList[1].P2WatingSize = countForP2Waiting
		taskList[1].C1Wating = countForC1Waiting

		return taskList, nil
	}
	if ok {
		ctc = v.(*taskCounter)
		taskList[1].AddPieceSize = ctc.addpiece
		taskList[1].Pre1CommitSize = ctc.precommit1
		taskList[1].Pre2CommitSize = ctc.precommit2
		taskList[1].Commit1 = ctc.commit1
		taskList[1].Commit2 = ctc.commit2
		taskList[1].P1WatingSize = countForP1Waiting
		taskList[1].P2WatingSize = countForP2Waiting
		taskList[1].C1Wating = countForC1Waiting

		taskList[0] = TasksNumber{}
		taskList[2] = TasksNumber{AddPieceSize: 255} //means return err= "No taskConfig found. "
	}
	if !ok {
		taskList[0].AddPieceSize = tc.AddPieceSize
		taskList[0].Pre1CommitSize = tc.Pre1CommitSize
		taskList[0].Pre2CommitSize = tc.Pre2CommitSize
		taskList[0].Commit1 = tc.Commit1
		taskList[0].Commit2 = tc.Commit2

		taskList[1] = TasksNumber{}
		taskList[3] = TasksNumber{AddPieceSize: 255} //means return err= "No current taskCount. "
	}

	return taskList, nil
}

func (m *Manager) PledgeSwitch(signal string) error {
	switch signal {
	case "on":
		m.sched.masterSwitch = true
	case "off":
		m.sched.masterSwitch = false
	default:
		return xerrors.Errorf("unrecognized switch signal")
	}
	return nil
}

func (m *Manager) GetSwitchStatus() ([]bool, error) {
	statusList := make([]bool, 2)
	statusList[0] = m.sched.masterSwitch
	statusList[1] = m.sched.isExistFreeWorker
	return statusList, nil
}

func (m *Manager) DealTransCount(size int) (int, error) {
	if size == EASTEREGG {
		return m.maxTransCount, nil
	} else {
		m.maxTransCount = size
	}

	return m.maxTransCount, nil
}

func (m *Manager) DeleteTaskCount(hostname string) (bool, error) {
	v, ok := m.sched.tasks.Load(hostname)
	if ok {
		fmt.Println("===== delete1 tc", v)
		m.sched.tasks.Delete(hostname)
		//fmt.Println("===== delete2 tc",v)
		//m.sched.tasks.Store(hostname,&taskCounter{})
		//fmt.Println("===== delete3 tc",v)
		return true, nil
	}
	return false, nil
}
