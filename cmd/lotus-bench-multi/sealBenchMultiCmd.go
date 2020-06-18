package main

import (
	"fmt"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var sealBenchMultiCmd = &cli.Command{
	Name: "multiSealing",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "storage-dir",
			Value: "~/.lotus-bench",
			Usage: "Path to the storage directory that will store sectors long term",
		},
		&cli.StringFlag{
			Name:  "sector-size",
			Value: "512MiB",
			Usage: "size of the sectors in bytes, i.e. 32GiB",
		},
		&cli.BoolFlag{
			Name:  "skip-commit2",
			Usage: "skip the commit2 (snark) portion of the benchmark",
		},
		&cli.IntFlag{
			Name:  "num-sectors",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "p1Count",
			Value: 4,
		},
		&cli.IntFlag{
			Name:  "p2Count",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "c2Count",
			Value: 1,
		},
	},
	Action: func(c *cli.Context) error {

		// TODO remember to delete dir
		///defer func() {
		///	if err := os.RemoveAll(tsdir); err != nil {
		///		log.Warn("remove all: ", err)
		///	}
		///}()

		// taskinfo record task params
		var taskinfos []*taskInfo

		err := runMultiSeals(c, &taskinfos)
		if err != nil {
			return xerrors.Errorf("failed to run seals: %w", err)
		}

		return nil
	},
}

func initSector(sdir, sSize string, num abi.SectorNumber, ti *taskInfo) error {
	sdir, err := homedir.Expand(sdir)
	if err != nil {
		return err
	}

	os.MkdirAll(sdir, 0775)

	tsdir, err := ioutil.TempDir(sdir, "bench")
	if err != nil {
		return err
	}

	// TODO: pretty sure this isnt even needed?
	if err := os.MkdirAll(tsdir, 0775); err != nil {
		return err
	}

	// miner address
	maddr, err := address.NewFromString("t01000")
	if err != nil {
		return err
	}
	amid, err := address.IDFromAddress(maddr)
	if err != nil {
		return err
	}
	mid := abi.ActorID(amid)

	// sector size
	sectorSizeInt, err := units.RAMInBytes(sSize)
	if err != nil {
		return err
	}
	sectorSize := abi.SectorSize(sectorSizeInt)

	spt, err := ffiwrapper.SealProofTypeFromSectorSize(sectorSize)
	if err != nil {
		return err
	}

	cfg := &ffiwrapper.Config{
		SealProofType: spt,
	}
	sbfs := &basicfs.Provider{
		Root: tsdir,
	}

	sid := abi.SectorID{
		Miner:  mid,
		Number: num,
	}

	sb, err := ffiwrapper.New(sbfs, cfg)
	if err != nil {
		return err
	}

	ti.sbdir = tsdir
	ti.sb = sb
	ti.sectorSize = sectorSize
	ti.spt = spt
	ti.mid = mid
	ti.sid = sid
	ti.number = num
	return nil
}

//func runSeals(sb *ffiwrapper.Sealer, sbfs *basicfs.Provider, numSectors int, mid abi.ActorID, sectorSize abi.SectorSize, ticketPreimage []byte, saveC2inp string, skipc2, skipunseal bool)  error {
func runMultiSeals(c *cli.Context, taskinfos *[]*taskInfo) error {

	var taskstate taskStates
	taskstate.new()
	taskstate.totalTask = uint16(c.Int("num-sectors"))

	for i := uint16(1); i <= taskstate.totalTask; i++ {
		tInfo := taskInfo{}
		initSector(c.String("storage-dir"), c.String("sector-size"), abi.SectorNumber(i), &tInfo)

		*taskinfos = append(*taskinfos, &tInfo)
		taskstate.addPieceWait[int(i)] = &tInfo
	}

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go sched(&taskstate, &wg, c.Int64("p1Count"), c.Int64("p2Count"), c.Int64("c2Count"))
	total := time.Now().Sub(start)
	fmt.Printf("----\ntotal cost time: %s\n", total)
	wg.Wait()

	// printcost time
	printTimeResult(time.Now(), time.Now(), fmt.Sprintf("size-%s count-%d", c.String("sector-size"), c.Int("num-sectors")), c.String("result-filename"), true)
	for _, info := range *taskinfos {
		printTimeResult(info.SealResult.AddPieceStart, info.SealResult.AddPieceEnd, fmt.Sprintf("s-%d-addpiece", info.number), c.String("result-filename"), false)
		printTimeResult(info.SealResult.PreCommit1Start, info.SealResult.PreCommit1End, fmt.Sprintf("s-%d-precommit1", info.number), c.String("result-filename"), false)
		printTimeResult(info.SealResult.PreCommit2Start, info.SealResult.PreCommit2End, fmt.Sprintf("s-%d-precommit2", info.number), c.String("result-filename"), false)
		printTimeResult(info.SealResult.SealCommit1Start, info.SealResult.SealCommit1End, fmt.Sprintf("s-%d-sealcommit1", info.number), c.String("result-filename"), false)
		printTimeResult(info.SealResult.SealCommit2Start, info.SealResult.SealCommit2End, fmt.Sprintf("s-%d-sealcommit2", info.number), c.String("result-filename"), false)
	}

	return nil
	//return sealTimings, sealedSectors, nil
}
