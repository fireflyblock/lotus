package sectorstorage

import (
	"context"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"golang.org/x/xerrors"
)

type ScopeType int32

const (
	_ScopeType = iota
	PRIORITYCOMMIT2
	PRIORITYREADPIECE
	PRIORITYSEAL
)

func GetWorkScope(ctx context.Context, w Worker) (ScopeType, error) {
	var scope ScopeType
	tasks, err := w.TaskTypes(ctx)
	if err != nil {
		return scope, xerrors.Errorf("(AddWorker)getting supported worker task types: %w", err)
	}
	if _, supported := tasks[sealtasks.TTAddPiece]; !supported {
		scope = PRIORITYCOMMIT2
	}

	if _, supported := tasks[sealtasks.TTCommit2]; !supported {
		scope = PRIORITYSEAL
	}

	return scope, nil
}

type ScopeOfWork struct {
	PriorityCommit2 []string
	PrioritySeal    []string
}

func (wt *ScopeOfWork) append(st ScopeType, hostname string) {
	switch st {
	case PRIORITYCOMMIT2:
		wt.PriorityCommit2 = append(wt.PriorityCommit2, hostname)
		//case 1:
		//	wt.PriorityReadPiece = append(wt.PriorityReadPiece, hostname)
	case PRIORITYSEAL:
		wt.PrioritySeal = append(wt.PrioritySeal, hostname)
	}
}
func (wt *ScopeOfWork) delete(st ScopeType, hostname string) {
	target := []string{}
	switch st {
	case PRIORITYCOMMIT2:
		if wt.PriorityCommit2 == nil {
			return
		}
		target = wt.PriorityCommit2
		//case 1:
	case PRIORITYSEAL:
		if wt.PrioritySeal == nil {
			return
		}
		target = wt.PrioritySeal
	}

	if len(target) == 0 {
		return
	}
	i := 0
	for j := 0; j < len(target); j++ {
		if target[j] != hostname {
			target[i] = target[j]
			i++
		}
	}

	switch st {
	case PRIORITYCOMMIT2:
		wt.PriorityCommit2 = target[:i]
	case PRIORITYSEAL:
		wt.PrioritySeal = target[:i]
	}
}

func (wt *ScopeOfWork) search(st ScopeType, hostname string) bool {
	switch st {
	case PRIORITYCOMMIT2:
		for _, st := range wt.PriorityCommit2 {
			if st == hostname {
				return true
			}
		}
		return false
	case PRIORITYSEAL:
		for _, st := range wt.PrioritySeal {
			if st == hostname {
				return true
			}
		}
		return false
	default:
		return false
		//case 1:
		//	wt.PriorityReadPiece = append(wt.PriorityReadPiece, hostname)
	}
}

func (wt *ScopeOfWork) pick(st ScopeType) string {
	switch st {
	case PRIORITYCOMMIT2:
		if len(wt.PriorityCommit2) > 0 {
			return wt.PriorityCommit2[0]
		} else {
			return ""
		}
	case PRIORITYSEAL:
		if len(wt.PrioritySeal) > 0 {
			return wt.PrioritySeal[0]
		} else {
			return ""
		}
	default:
		return ""
		//case 1:
		//	wt.PriorityReadPiece = append(wt.PriorityReadPiece, hostname)
	}
}
