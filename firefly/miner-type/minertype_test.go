package minertype

import (
	"fmt"
	"testing"
)

func TestSet(t *testing.T) {
	SetMinerType("222")
	PrintCanDoTask()

}

func TestGetMinerType(t *testing.T) {

	SetMinerType("sched,wdpost")
	fmt.Println(GetMinerType())
}

func PrintCanDoTask() {
	if CanDoWinningPost() {
		fmt.Println("Can do winning post")
	}
	if CanDoWDPost() {
		fmt.Println("Can do wd post")
	}
	if CanDoSched() {
		fmt.Println("Can do sched")
	}
}
