package minertype

import (
	"fmt"
	"golang.org/x/xerrors"
	"strings"
)

type MinerType uint8

var mtype MinerType

const (
	ALL     MinerType = 7
	WINPOST MinerType = 1
	WDPOST  MinerType = 2
	SCHED   MinerType = 4
)

const (
	STR_WINPOST = "winpost"
	STR_WDPOST  = "wdpost"
	STR_SCHED   = "sched"
)

func SetMinerType(smt string) error {
	if strings.Contains(smt, STR_WINPOST) {
		fmt.Println("Can do winpost")
		mtype |= WINPOST
	}

	if strings.Contains(smt, STR_WDPOST) {
		fmt.Println("Can do wdpost")
		mtype |= WDPOST
	}

	if strings.Contains(smt, STR_SCHED) {
		fmt.Println("Can do sched")
		mtype |= SCHED
	}

	if len(smt) == 0 {
		mtype = WINPOST | WDPOST | SCHED
		fmt.Println("Can do all task")
	} else {
		if mtype == 0 {
			// smt 不为空，且mtype为0则说明配置的字符从有误
			return xerrors.Errorf("miner-type must be winpost,wdpost,sched or empty string.")
		}
	}

	return nil
}

func GetMinerType() string {
	var task_type []string
	if CanDoSched() {
		task_type = append(task_type, STR_SCHED)
	}
	if CanDoWDPost() {
		task_type = append(task_type, STR_WDPOST)
	}
	if CanDoWinningPost() {
		task_type = append(task_type, STR_WINPOST)
	}
	return strings.Join(task_type, ",")
}

func CanDoWinningPost() bool {
	return mtype&WINPOST != 0
}

func CanDoWDPost() bool {
	return mtype&WDPOST != 0
}
func CanDoSched() bool {
	return mtype&SCHED != 0
}
