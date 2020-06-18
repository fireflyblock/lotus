package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"golang.org/x/xerrors"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type taskStates struct {
	lkAddPiece sync.Mutex
	lkPre1     sync.Mutex
	lkPre2     sync.Mutex
	lkSeal1    sync.Mutex
	lkSeal2    sync.Mutex

	totalTask uint16

	// addPiece task states
	addPieceWait     map[int]*taskInfo
	addPieceWorking  map[int]*taskInfo
	addPieceFinished map[int]*taskInfo

	// sealPreCommit1 task states
	pre1Wait     map[int]*taskInfo
	pre1Working  map[int]*taskInfo
	pre1Finished map[int]*taskInfo

	// sealPreCommit2 task states
	pre2Wait     map[int]*taskInfo
	pre2Working  map[int]*taskInfo
	pre2Finished map[int]*taskInfo

	// sealCommit1 task states
	seal1Wait     map[int]*taskInfo
	seal1Working  map[int]*taskInfo
	seal1Finished map[int]*taskInfo

	// sealCommit2 task states
	seal2Wait     map[int]*taskInfo
	seal2Working  map[int]*taskInfo
	seal2Finished map[int]*taskInfo
}

func (info *taskStates) new() {
	// addPiece
	info.addPieceWait = make(map[int]*taskInfo)
	info.addPieceWorking = make(map[int]*taskInfo)
	info.addPieceFinished = make(map[int]*taskInfo)

	// pre1
	info.pre1Wait = make(map[int]*taskInfo)
	info.pre1Working = make(map[int]*taskInfo)
	info.pre1Finished = make(map[int]*taskInfo)

	// pre2
	info.pre2Wait = make(map[int]*taskInfo)
	info.pre2Working = make(map[int]*taskInfo)
	info.pre2Finished = make(map[int]*taskInfo)

	// seal1
	info.seal1Wait = make(map[int]*taskInfo)
	info.seal1Working = make(map[int]*taskInfo)
	info.seal1Finished = make(map[int]*taskInfo)

	// seal2
	info.seal2Wait = make(map[int]*taskInfo)
	info.seal2Working = make(map[int]*taskInfo)
	info.seal2Finished = make(map[int]*taskInfo)
}

type taskInfo struct {
	// init params
	sb         *ffiwrapper.Sealer
	sbdir      string
	spt        abi.RegisteredSealProof
	mid        abi.ActorID
	sectorSize abi.SectorSize

	sid    abi.SectorID
	number abi.SectorNumber
	seed   lapi.SealSeed
	trand  [32]byte
	ticket abi.SealRandomness
	pi     abi.PieceInfo
	pc1o   storage.PreCommit1Out
	cids   storage.SectorCids
	c1o    storage.Commit1Out

	// costtime
	SealResult SealingResult

	finished bool
}

type TaskInfoOut struct {
	Mid        abi.ActorID             `json:"mid"`
	Sid        abi.SectorID            `json:"sid"`
	Number     abi.SectorNumber        `json:"number"`
	Spt        abi.RegisteredSealProof `json:"spt"`
	Seed       lapi.SealSeed           `json:"seed"`
	SectorSize abi.SectorSize          `json:"sectorsize"`
	Trand      [32]byte                `json:"trand"`
	Ticket     abi.SealRandomness      `json:"ticket"`
	Pi         abi.PieceInfo           `json:"pi"`
	Pc1o       storage.PreCommit1Out   `json:"pc1o"`
	Cids       storage.SectorCids      `json:"cids"`
	C1o        storage.Commit1Out      `json:"c1o"`
}

func (t *TaskInfoOut) transfer(info *taskInfo) {
	t.Mid = info.mid
	t.Sid = info.sid
	t.Number = info.number
	t.Spt = info.spt
	t.SectorSize = info.sectorSize
	t.Trand = info.trand
	t.Ticket = info.ticket
	t.Pi = info.pi
	t.Pc1o = info.pc1o
	t.Cids = info.cids
	t.C1o = info.c1o
}

func (t *TaskInfoOut) GobSerializeAndWriteFile(filename string) {
	fmt.Println("-----------------------------", filename)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(*t); err != nil {
		fmt.Println(err)
		return
	}

	if err := ioutil.WriteFile(filename, buf.Bytes(), 0644); err != nil {
		fmt.Println(err)
		return
	}
}

func (t *TaskInfoOut) GobDeSerializeFromFile(filename string) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	dec := gob.NewDecoder(bytes.NewBuffer(buf))
	if err := dec.Decode(t); err != nil {
		fmt.Println(err)
		return
	}
}

func (t *taskInfo) transfer(info TaskInfoOut) {
	t.mid = info.Mid
	t.sid = info.Sid
	t.number = info.Number
	t.spt = info.Spt
	t.sectorSize = info.SectorSize
	t.trand = info.Trand
	t.ticket = info.Ticket
	t.pi = info.Pi
	t.pc1o = info.Pc1o
	t.cids = info.Cids
	t.c1o = info.C1o
}

func addPiece(ctx context.Context, sb *ffiwrapper.Sealer, tp *taskInfo) error {
	start := time.Now()
	r := rand.New(rand.NewSource(100 + int64(tp.number)))

	pi, err := sb.AddPiece(context.TODO(), tp.sid, nil, abi.PaddedPieceSize(tp.sectorSize).Unpadded(), r)
	tp.pi = pi
	tp.SealResult.AddPiece = time.Now().Sub(start)
	tp.SealResult.AddPieceStart = start
	tp.SealResult.AddPieceEnd = time.Now()
	out := fmt.Sprintf("|%s|%s|%s|%s|\n", "", start.Format("2006-01-02 15:04:05"), tp.SealResult.AddPieceEnd.Format("2006-01-02 15:04:05"), tp.SealResult.AddPiece)
	fmt.Println(out)
	if err != nil {
		return err
	}
	return nil
}

func precommit1(ctx context.Context, sb *ffiwrapper.Sealer, tp *taskInfo) error {

	start := time.Now()
	ticket := abi.SealRandomness(tp.trand[:])
	tp.ticket = ticket
	pieces := []abi.PieceInfo{tp.pi}
	pc1o, err := sb.SealPreCommit1(context.TODO(), tp.sid, ticket, pieces)
	tp.pc1o = pc1o
	tp.SealResult.PreCommit1 = time.Now().Sub(start)
	tp.SealResult.PreCommit1Start = start
	tp.SealResult.PreCommit1End = time.Now()
	if err != nil {
		return err
	}
	return nil
}

func precommit2(ctx context.Context, sb *ffiwrapper.Sealer, tp *taskInfo) error {

	start := time.Now()
	cids, err := sb.SealPreCommit2(context.TODO(), tp.sid, tp.pc1o)
	tp.cids = cids
	tp.SealResult.PreCommit2 = time.Now().Sub(start)
	tp.SealResult.PreCommit2Start = start
	tp.SealResult.PreCommit2End = time.Now()
	if err != nil {
		return err
	}
	return nil
}

func sealCommit1(ctx context.Context, sb *ffiwrapper.Sealer, tp *taskInfo) error {
	seed := lapi.SealSeed{
		Epoch: 101,
		Value: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 255},
	}
	tp.seed = seed

	start := time.Now()
	pieces := []abi.PieceInfo{tp.pi}
	c1o, err := sb.SealCommit1(context.TODO(), tp.sid, tp.ticket, seed.Value, pieces, tp.cids)
	tp.c1o = c1o
	tp.SealResult.Commit1 = time.Now().Sub(start)
	tp.SealResult.SealCommit1Start = start
	tp.SealResult.SealCommit1End = time.Now()
	if err != nil {
		return err
	}
	return nil
}

func sealCommit2(ctx context.Context, sb *ffiwrapper.Sealer, tp *taskInfo) error {

	start := time.Now()
	proof, err := sb.SealCommit2(context.TODO(), tp.sid, tp.c1o)

	tp.SealResult.Commit2 = time.Now().Sub(start)
	tp.SealResult.SealCommit2Start = start
	tp.SealResult.SealCommit2End = time.Now()

	svi := abi.SealVerifyInfo{
		SectorID:              abi.SectorID{Miner: tp.mid, Number: tp.number},
		SealedCID:             tp.cids.Sealed,
		SealProof:       sb.SealProofType(),
		Proof:                 proof,
		DealIDs:               nil,
		Randomness:            tp.ticket,
		InteractiveRandomness: tp.seed.Value,
		UnsealedCID:           tp.cids.Unsealed,
	}

	ok, err := ffiwrapper.ProofVerifier.VerifySeal(svi)
	if err != nil {
		return err
	}
	if !ok {
		return xerrors.Errorf("porep proof for sector %d was invalid", tp.number)
	}
	log.Infof("sector %d seal successed", tp.number)
	tp.finished = true
	return nil
}

//
func printTimeResult(start, end time.Time, title, filename string, showTitle bool) {
	var out string
	if len(filename) == 0 {

		filename = "out.result"
	}

	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	if showTitle {
		out = fmt.Sprint("======================================================")
		out += fmt.Sprintf("\n|%s|%s|%s|%s|\n", title, "start", "end", "cost")
		data := []byte(out)
		writeFile(path, filename, data)
		return
	}

	out = fmt.Sprintf("|%s|%s|%s|%s|\n", title, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), end.Sub(start))
	//fmt.Println(out)
	data := []byte(out)
	writeFile(path, filename, data)
}

func writeFile(path, filename string, data []byte) error {

	f, err := os.OpenFile(path+"/"+filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err == nil {
		err = err1
	} else {
		log.Error(err)
		return err
	}
	return nil
}
