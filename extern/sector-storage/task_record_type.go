package sectorstorage

import (
	"sync"
	"time"
)

type TaskType uint8

const (
	_TaskType = iota
	TT_ADDPIECE
	TT_PRECOMMIT1
	TT_PRECOMMIT2
	TT_COMMIT1
	TT_COMMIT2
	TT_Finalize
)

type TaskStatus uint8

const (
	_TaskStatus = iota
	TS_WAITING
	TS_COMPUTING
	TS_COMPLETE
)

type CurrentTaskStatus int32

const (
	_CurrentTaskStatus = iota
	// AddPiece status
	ADDPIECE_WAITING
	ADDPIECE_COMPUTING

	// PreCommit1 status
	PRE1_WAITTING
	PRE1_COMPUTING

	// PreCommit2 status
	PRE2_WAITING
	PRE2_COMPUTING

	// Commit1 status
	COMMIT1_WAITTING
	COMMIT1_COMPUTING

	// Commit2 status
	COMMIT2_WAITTING
	COMMIT2_COMPUTING

	// seal finished
	SEAL_FINISHED

	// Transfor status
	TRANSFOR_WAIT
	TRANSFOR_ING
	TRANSFOR_FINISHED
)

// Sector任务状态记录
type sectorTaskRecord struct {
	// AddPiece, p1,p2,c1 task worker
	workerFortask string

	// Commit2所属的worker
	workerFortaskC2 string

	// task status
	taskStatus CurrentTaskStatus

	// tansfor record
	transforStatus CurrentTaskStatus
	transforStart  time.Time
	transforEnd    time.Time
}

type taskCounter struct {
	addpiece   uint8
	precommit1 uint8
	precommit2 uint8
	commit1    uint8
	commit2    uint64
	tasksLK    sync.Mutex
}

type TaskConfig struct {
	AddPieceSize   uint8  `json:"add_piece_size"`
	Pre1CommitSize uint8  `json:"pre_1_commit_size"`
	Pre2CommitSize uint8  `json:"pre_2_commit_size"`
	Commit1        uint8  `json:"commit_1"`
	Commit2        uint64 `json:"commit_2"`
}

type TasksNumber struct {
	AddPieceSize   uint8  `json:"add_piece_size"`
	Pre1CommitSize uint8  `json:"pre_1_commit_size"`
	Pre2CommitSize uint8  `json:"pre_2_commit_size"`
	Commit1        uint8  `json:"commit_1"`
	Commit2        uint64 `json:"commit_2"`
	P1WatingSize   uint8  `json:"p_1_wating_size"`
	P2WatingSize   uint8  `json:"p_2_wating_size"`
	C1Wating       uint8  `json:"c_1_wating"`
}
