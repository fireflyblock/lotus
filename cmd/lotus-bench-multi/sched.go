package main

import (
	_ "container/list"
	"context"
	"fmt"
	_ "github.com/filecoin-project/specs-actors/actors/abi"
	"sync"
)

func sched(states *taskStates,group *sync.WaitGroup,pre1count,pre2count,commit2count int64)  {
	addPieceCh:=make(chan *taskInfo ,10)
	pre1Ch:=make(chan *taskInfo)
	pre2Ch:=make(chan *taskInfo)
	seal1Ch:=make(chan *taskInfo)
	seal2Ch:=make(chan *taskInfo)
	endCh:=make(chan *taskInfo)

	printStats(states)

	for i:=0;i<10;i++{
		go doAddPieceTask(states,addPieceCh)
	}

	for {
		select {
		case finisedAddPiece:=<-addPieceCh:
			addPieceFinished(finisedAddPiece,states,addPieceCh,pre1Ch,pre1count)
			printStats(states)
		case finishedPre1:=<-pre1Ch:
			pre1CommitFinished(finishedPre1,states,pre1Ch,pre2Ch,pre2count)
			printStats(states)
		case finishedPre2:=<-pre2Ch:
			pre2CommitFinished(finishedPre2,states,pre2Ch,seal1Ch)
			printStats(states)
		case finishedSeal1:=<-seal1Ch:
			seal1CommitFinished(finishedSeal1,states,seal1Ch,seal2Ch,commit2count)
			printStats(states)
		case finishedSeal2:=<-seal2Ch:
			info:=finishedSeal2
			if info.finished{
				log.Infof("---sector %d finished seal and pass verify!!!!\n",info.number)
			}else{
				log.Errorf("---sector %d failed seal !!!!\n",info.number)
			}
			printStats(states)
			seal2CommitFinished(states,seal2Ch,endCh)
		case <-endCh:
			log.Info("--------all task are finished!!!---------")
			group.Done()
			return
		}
	}
	group.Done()
}

func printStats(states *taskStates){
	fmt.Println("+++++++++++++++++++++++++++++++++++")
	fmt.Printf("addPieceTask:\nwait %d\nworking %d\nfinished %d\n\n",len(states.addPieceWait),len(states.addPieceWorking),len(states.addPieceFinished))
	fmt.Printf("preCommit1Task:\nwait %d\nworking %d\nfinished %d\n\n",len(states.pre1Wait),len(states.pre1Working),len(states.pre1Finished))
	fmt.Printf("preCommit2Task:\nwait %d\nworking %d\nfinished %d\n\n",len(states.pre2Wait),len(states.pre2Working),len(states.pre2Finished))
	fmt.Printf("sealCommit1Task:\nwait %d\nworking %d\nfinished %d\n\n",len(states.seal1Wait),len(states.seal1Working),len(states.seal1Finished))
	fmt.Printf("sealCommit2Task:\nwait %d\nworking %d\nfinished %d\n\n",len(states.seal2Wait),len(states.seal2Working),len(states.seal2Finished))
	fmt.Println("+++++++++++++++++++++++++++++++++++")
}

func seal2CommitFinished(stats *taskStates,seal2Ch,endCh chan *taskInfo)  {
	// 做seal2
	go doSeal2Task(stats,seal2Ch)

	// 判断是否所有的任务多完成
	finishedAll:=false
	stats.lkSeal2.Lock()
	if len(stats.seal2Finished)==int(stats.totalTask){
		finishedAll=true
	}
	stats.lkSeal2.Unlock()
	if finishedAll{
		go func() {
			endCh<-&taskInfo{}
		}()
	}
}

func seal1CommitFinished(finishedSeal1 *taskInfo,states *taskStates,seal1Ch,seal2Ch chan *taskInfo,seal2Count int64){

	// 做seal1
	go doSeal1Task(states,seal1Ch)

	// 存储seal2
	states.lkSeal2.Lock()
	states.seal2Wait[int(finishedSeal1.number)]=finishedSeal1
	if len(states.seal2Working)< int(seal2Count){
		go doSeal2Task(states,seal2Ch)
	}
	states.lkSeal2.Unlock()

}

func pre2CommitFinished(finishedPre2 *taskInfo,states *taskStates,pre2Ch,seal1Ch chan *taskInfo){

	// 做pre2
	go doPre2CommitTask(states,pre2Ch)

	// 存储seal1
	states.lkSeal1.Lock()
	states.seal1Wait[int(finishedPre2.number)]=finishedPre2
	go doSeal1Task(states,seal1Ch)
	states.lkSeal1.Unlock()

}

func pre1CommitFinished(finishedPre1 *taskInfo,states *taskStates,pre1Ch,pre2Ch chan *taskInfo,pre2count int64){

	// 做pre1
	go doPre1CommitTask(states,pre1Ch)

	// 存储pre2
	states.lkPre2.Lock()
	states.pre2Wait[int(finishedPre1.number)]=finishedPre1
	if len(states.pre2Working)<int(pre2count){
		go doPre2CommitTask(states,pre2Ch)
	}
	states.lkPre2.Unlock()

}

func addPieceFinished(piece *taskInfo,states *taskStates,addPieceCh,pre1Ch chan *taskInfo,pre1count int64){

	// 做addpiece
	go doAddPieceTask(states,addPieceCh)

	// 存储pre1
	states.lkPre1.Lock()
	states.pre1Wait[int(piece.number)]=piece
	if len(states.pre1Working)<int(pre1count){
		go doPre1CommitTask(states,pre1Ch)
	}
	states.lkPre1.Unlock()

}

func doSeal2Task(state *taskStates,ch chan *taskInfo){
	// do seal2
	state.lkSeal2.Lock()
	if len(state.seal2Wait)<=0{
		state.lkSeal2.Unlock()
		log.Info("no waiting sealcommit2 !!!!")
		return
	}
	var info *taskInfo
	for _,_info:=range state.seal2Wait{
		info=_info
		state.seal2Working[int(_info.number)]=_info
		delete(state.seal2Wait,int(_info.number))
		break
	}
	state.lkSeal2.Unlock()

	log.Infof("doing sealcommit2 for sector %d ...\n",info.number)
	if err:=sealCommit2(context.TODO(),info.sb,info);err!=nil{
		log.Error(err)
	}

	state.lkSeal2.Lock()
	delete(state.seal2Working,int(info.number))
	state.seal2Finished[int(info.number)]=info
	state.lkSeal2.Unlock()

	ch<-info
}

func doSeal1Task(state *taskStates,ch chan *taskInfo){
	// do seal1
	state.lkSeal1.Lock()
	if len(state.seal1Wait)<=0{
		state.lkSeal1.Unlock()
		return
	}
	var info *taskInfo
	for _,_info:=range state.seal1Wait{
		info=_info
		state.seal1Working[int(_info.number)]=_info
		delete(state.seal1Wait,int(_info.number))
		break
	}
	state.lkSeal1.Unlock()

	log.Infof("doing sealcommit1 for sector %d ...\n",info.number)
	if err:=sealCommit1(context.TODO(),info.sb,info);err!=nil{
		log.Error(err)
	}

	state.lkSeal1.Lock()
	delete(state.seal1Working,int(info.number))
	state.seal1Finished[int(info.number)]=info
	state.lkSeal1.Unlock()

	ch<-info
}

func doPre2CommitTask(state *taskStates,ch chan *taskInfo){
	// do pre2
	state.lkPre2.Lock()
	if len(state.pre2Wait)<=0{
		state.lkPre2.Unlock()
		return
	}

	var info *taskInfo
	for _number,_info:=range state.pre2Wait{
		info=_info
		state.pre2Working[int(_number)]=info
		delete(state.pre2Wait,_number)
		break
	}
	state.lkPre2.Unlock()

	log.Infof("doing preCommit2 for sector %d ...\n",info.number)
	if err:=precommit2(context.TODO(),info.sb,info);err!=nil{
		log.Error(err)
	}

	state.lkPre2.Lock()
	delete(state.pre2Working,int(info.number))
	state.pre2Finished[int(info.number)]=info
	state.lkPre2.Unlock()

	ch<-info
}

func doPre1CommitTask(state *taskStates,ch chan *taskInfo){
	// do pre1
	state.lkPre1.Lock()
	if len(state.pre1Wait)<=0{
		state.lkPre1.Unlock()
		return
	}

	var info *taskInfo
	for _number,_info:=range state.pre1Wait{
		info=_info
		state.pre1Working[int(_number)]=info
		delete(state.pre1Wait,_number)
		break
	}
	state.lkPre1.Unlock()

	log.Infof("doing preCommit1 for sector %d ...\n",info.number)
	if err:=precommit1(context.TODO(),info.sb,info);err!=nil{
		log.Error(err)
	}

	state.lkPre1.Lock()
	delete(state.pre1Working,int(info.number))
	state.pre1Finished[int(info.number)]=info
	state.lkPre1.Unlock()

	ch<-info
}

func doAddPieceTask(state *taskStates,ch chan *taskInfo){
	// add piece
	state.lkAddPiece.Lock()
	if len(state.addPieceWait)<=0 {
		state.lkAddPiece.Unlock()
		return
	}
	var info *taskInfo
	for _number,_info:=range state.addPieceWait{
		info=_info
		state.addPieceWorking[_number]=_info
		delete(state.addPieceWait,_number)
		break
	}
	state.lkAddPiece.Unlock()

	log.Infof("Write piece into sector %d ...\n",info.number)
	if err:=addPiece(context.TODO(),info.sb,info);err!=nil{
		log.Error(err)
	}

	state.lkAddPiece.Lock()
	delete(state.addPieceWorking,int(info.number))
	state.addPieceFinished[int(info.number)]=info
	state.lkAddPiece.Unlock()

	ch<-info
}




