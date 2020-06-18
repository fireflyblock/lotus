package main

import (
	"fmt"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/prometheus/common/log"
	"net/url"
	"os"
)

type taskInfo struct {
	mid abi.ActorID
	sid abi.SectorID
	number abi.SectorNumber
	spt abi.RegisteredProof
	seed lapi.SealSeed
	sectorSize abi.SectorSize
	trand [32]byte
	ticket abi.SealRandomness
	pi abi.PieceInfo
	pc1o storage.PreCommit1Out
	cids storage.SectorCids
	c1o storage.Commit1Out
}

type TaskInfoOut struct {
	Mid abi.ActorID `json:"mid"`
	Sid abi.SectorID `json:"sid"`
	Number abi.SectorNumber `json:"number"`
	Spt abi.RegisteredProof `json:"spt"`
	Seed lapi.SealSeed `json:"seed"`
	SectorSize abi.SectorSize `json:"sectorsize"`
	Trand [32]byte `json:"trand"`
	Ticket abi.SealRandomness `json:"ticket"`
	Pi abi.PieceInfo `json:"pi"`
	Pc1o storage.PreCommit1Out `json:"pc1o"`
	Cids storage.SectorCids `json:"cids"`
	C1o storage.Commit1Out `json:"c1o"`
}

func (t *TaskInfoOut)transfer(info taskInfo){
	t.Mid=info.mid
	t.Sid=info.sid
	t.Number=info.number
	t.Spt=info.spt
	t.SectorSize=info.sectorSize
	t.Trand=info.trand
	t.Ticket=info.ticket
	t.Pi=info.pi
	t.Pc1o=info.pc1o
	t.Cids=info.cids
	t.C1o=info.c1o
}

func (t *taskInfo)transfer(info TaskInfoOut){
	t.mid=info.Mid
	t.sid=info.Sid
	t.number=info.Number
	t.spt=info.Spt
	t.sectorSize=info.SectorSize
	t.trand=info.Trand
	t.ticket=info.Ticket
	t.pi=info.Pi
	t.pc1o=info.Pc1o
	t.cids=info.Cids
	t.c1o=info.C1o

}
type SectorState string
const (
	UndefinedSectorState SectorState = ""

	// happy path
	Empty          SectorState = "Empty"
	Packing        SectorState = "Packing"       // sector not in sealStore, and not on chain
	PreCommit1     SectorState = "PreCommit1"    // do PreCommit1
	PreCommit2     SectorState = "PreCommit2"    // do PreCommit1
	PreCommitting  SectorState = "PreCommitting" // on chain pre-commit
	WaitSeed       SectorState = "WaitSeed"      // waiting for seed
	Committing     SectorState = "Committing"
	CommitWait     SectorState = "CommitWait" // waiting for message to land on chain
	FinalizeSector SectorState = "FinalizeSector"
	Proving        SectorState = "Proving"
	// error modes
	FailedUnrecoverable SectorState = "FailedUnrecoverable"
	SealFailed          SectorState = "SealFailed"
	PreCommitFailed     SectorState = "PreCommitFailed"
	ComputeProofFailed  SectorState = "ComputeProofFailed"
	CommitFailed        SectorState = "CommitFailed"
	PackingFailed       SectorState = "PackingFailed"
	Faulty              SectorState = "Faulty"        // sector is corrupted or gone for some reason
	FaultReported       SectorState = "FaultReported" // sector has been declared as a fault on chain
	FaultedFinal        SectorState = "FaultedFinal"  // fault declared on chain
)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func acquireWorkerHost() string  {
	sinfo:="http://172.16.0.10:2349/remote/sealed/s-t01012-12"
	//if sinfo!=nil && len(sinfo.URLs)>0{
		url,err:=url.Parse(sinfo)
		if err!=nil {
			log.Errorf("======acquire worker`s storageinfo failed, err: %+v",err)
			return ""
		}
		return  url.Hostname()
	//}
	return ""
}

func main(){
	fmt.Println(acquireWorkerHost())
//	log.Infof("start")
//	processMsgStart:=time.Now()
//	time.Sleep(time.Second*7)
//	// 只处理5秒的消息
//	if time.Now().Sub(processMsgStart).Seconds()>=5{
//		log.Infof("=======process MSG total cost %fs,interrupt!!",time.Now().Sub(processMsgStart).Seconds())
//	}
//	log.Info("end")
	//end:=1024*1024*1024*50
	//wg:=sync.WaitGroup{}
	//wg.Add(1)
	//for j:=0;j<5;j++{
	//	go func() {
	//		for  i:=0;i<end;{
	//			i++
	//		}
	//		wg.Done()
	//	}()
	//}

	//wg.Wait()

}

//func main()  {
//
//	path:="/opt/lotus/bench-save/sectorinfo.gob"
//	b,err:=PathExists(path)
//	if err!=nil{
//		fmt.Errorf("===err:",err)
//		return
//	}
//	if b{
//		fmt.Println("sectorinfo.gob exist")
//	}else{
//		fmt.Println("sectorinfo.gob not exist")
//
//	}
//}

//func main() {
//	to:=TaskInfoOut{Mid: 1024}
//
//	//ret, err := json.MarshalIndent(to, "", " ")
//	ret, err := json.Marshal(to)
//	if err != nil {
//		fmt.Println(err)
//	} else {
//		fmt.Println(string(ret))
//		/*
//			{
//			 "name": "satori",
//			 "age": 16,
//			 "gender": "f",
//			 "where": "东方地灵殿",
//			 "is_married": false
//			}
//		*/
//	}
//	fmt.Println("=============================")
//
//
//	// 反序列化
//	rev:=taskInfo{}
//	rev1:=TaskInfoOut{}
//	//传入json字符串，和指针
//	err = json.Unmarshal(ret, &rev1)
//	if err != nil {
//		fmt.Println(err)
//	}
//	fmt.Println(rev1)  //{satori 16 f 东方地灵殿 false}
//
//	rev.transfer(rev1)
//	fmt.Println("=============================")
//	fmt.Println(rev) // satori 16
//}
