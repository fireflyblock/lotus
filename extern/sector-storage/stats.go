package sectorstorage

import (
	"encoding/json"
	"github.com/filecoin-project/sector-storage/storiface"
	"golang.org/x/xerrors"
)

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
			if taskConf.Commit2 != 20070920 {
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

func (m *Manager) WorkerConfGet(hostname string) ([]TaskConfig, error) {
	//log.Infof("================ 远程查询config【%+v】", taskConf)
	var tc TaskConfig
	var ctc *taskCounter
	var tcCount uint
	taskList := make([]TaskConfig, 4)
	for _, handle := range m.sched.workers {
		if handle.info.Hostname == hostname {
			tc = *handle.taskConf
			tcCount++
		}
	}
	v, ok := m.sched.tasks.Load(hostname)

	if tcCount > 0 && ok {
		ctc = v.(*taskCounter)

		taskList[0] = tc

		taskList[1].AddPieceSize = ctc.addpiece
		taskList[1].Pre1CommitSize = ctc.precommit1
		taskList[1].Pre2CommitSize = ctc.precommit2
		taskList[1].Commit1 = ctc.commit1
		taskList[1].Commit2 = ctc.commit2

		return taskList, nil
	}
	if tcCount == 0 {
		taskList[2] = TaskConfig{AddPieceSize: 255} //means return err= "No taskConfig found. "
	}
	if tcCount > 0 {
		taskList[0] = tc
		taskList[1] = TaskConfig{}
	}
	if !ok {
		taskList[3] = TaskConfig{AddPieceSize: 255} //means return err= "No current taskCount. "
	}
	if ok {
		taskList[0] = TaskConfig{}

		ctc = v.(*taskCounter)
		taskList[1].AddPieceSize = ctc.addpiece
		taskList[1].Pre1CommitSize = ctc.precommit1
		taskList[1].Pre2CommitSize = ctc.precommit2
		taskList[1].Commit1 = ctc.commit1
		taskList[1].Commit2 = ctc.commit2
	}

	return taskList, nil
}

func (m *Manager) PledgeSwitch(signal string) error {
	log.Infof("================ switch pledge")
	switch signal {
	case "on":
		m.sched.isExistFreeWorker = true
	case "off":
		m.sched.isExistFreeWorker = false
	default:
		return xerrors.Errorf("unrecognized switch signal")
	}
	return nil
}

func (m *Manager) GetSwitchStatus() (bool, error) {
	log.Infof("================ get switch status")

	switch m.sched.isExistFreeWorker {
	case true:
		return true, nil
	case false:
		return false, nil
	}
	return false, xerrors.Errorf("get switch status err")
}
