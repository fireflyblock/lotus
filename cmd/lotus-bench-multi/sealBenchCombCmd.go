package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	paramfetch "github.com/filecoin-project/go-paramfetch"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/minio/blake2b-simd"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"time"
)

var sealBenchCombCmd = &cli.Command{
	Name: "combSealing",
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
			Name:  "no-gpu",
			Usage: "disable gpu usage for the benchmark run",
		},
		&cli.StringFlag{
			Name:  "miner-addr",
			Usage: "pass miner address (only necessary if using existing sectorbuilder)",
			Value: "t01000",
		},
		&cli.StringFlag{
			Name:  "benchmark-existing-sectorbuilder",
			Usage: "pass a directory to run post timings on an existing sectorbuilder",
		},
		&cli.BoolFlag{
			Name:  "json-out",
			Usage: "output results in json format",
		},
		&cli.BoolFlag{
			Name:  "skip-commit2",
			Usage: "skip the commit2 (snark) portion of the benchmark",
		},
		&cli.BoolFlag{
			Name:  "skip-unseal",
			Usage: "skip the unseal portion of the benchmark",
		},
		&cli.BoolFlag{
			Name:  "save-sectorinfo",
			Usage: "save bench result,default false",
		},
		&cli.StringFlag{
			Name:  "save-path",
			Usage: "path for save bench result,default /opt/lotus",
			Value: "/opt/lotus",
		},
		&cli.BoolFlag{
			Name:  "only-precommit1",
			Value: false,
			Usage: "if true ,only do preCommit1, default false",
		},
		&cli.BoolFlag{
			Name:  "only-precommit2",
			Value: false,
			Usage: "if true ,only do preCommit2, default false",
		},
		&cli.BoolFlag{
			Name:  "only-sealcommit2",
			Value: false,
			Usage: "if true ,only do sealCommit2, default false",
		},
		&cli.BoolFlag{
			Name:  "only-post",
			Value: false,
			Usage: "if true ,only do post, default false",
		},
		&cli.StringFlag{
			Name:  "save-commit2-input",
			Usage: "Save commit2 input to a file",
		},
		&cli.IntFlag{
			Name:  "num-sectors",
			Value: 1,
		},
		&cli.IntFlag{
			Name:  "p1Count",
			Value: 0,
		},
	},
	Action: func(c *cli.Context) error {
		if c.Bool("no-gpu") {
			os.Setenv("BELLMAN_NO_GPU", "1")
		}

		robench := c.String("benchmark-existing-sectorbuilder")

		var sbdir = c.String("save-path")

		if c.Bool("save-sectorinfo") {
			sbdir += "/bench-save"
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			fmt.Println("sectorinfo storage path:", sbdir)
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			os.RemoveAll(sbdir)
			if err := os.MkdirAll(sbdir, 0775); err != nil {
				fmt.Println(err)
				return nil
			}
		} else if c.Bool("only-precommit1") || c.Bool("only-precommit2") || c.Bool("only-sealcommit2") || c.Bool("only-post") {
			//sbdir += "/bench-save"
		} else {
			if robench == "" {
				sdir, err := homedir.Expand(c.String("storage-dir"))
				if err != nil {
					return err
				}

				os.MkdirAll(sdir, 0775)

				tsdir, err := ioutil.TempDir(sdir, "bench")
				if err != nil {
					return err
				}
				defer func() {
					if err := os.RemoveAll(tsdir); err != nil {
						log.Warn("remove all: ", err)
					}
				}()

				// TODO: pretty sure this isnt even needed?
				if err := os.MkdirAll(tsdir, 0775); err != nil {
					return err
				}

				sbdir = tsdir
			} else {
				exp, err := homedir.Expand(robench)
				if err != nil {
					return err
				}
				sbdir = exp
			}
		}

		var taskinfo taskInfo
		filename := sbdir + "/sectorinfo.gob"
		if c.Bool("only-precommit1") || c.Bool("only-precommit2") || c.Bool("only-sealcommit2") || c.Bool("only-post") {
			sbdir += "/bench-save"
			filename = sbdir + "/sectorinfo.gob"
			sectorInfoExist, err := PathExists(filename)
			if err != nil {
				fmt.Println(err)
				return err
			}

			if !sectorInfoExist {
				fmt.Println("=====sectorinfo.gob 不存在，无法执行任务")
				return nil
			} else {
				tOut := TaskInfoOut{}
				tOut.GobDeSerializeFromFile(filename)
				taskinfo.transfer(tOut)
			}
		}

		// miner address
		maddr, err := address.NewFromString(c.String("miner-addr"))
		if err != nil {
			return err
		}
		amid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}
		mid := abi.ActorID(amid)

		// sector size
		sectorSizeInt, err := units.RAMInBytes(c.String("sector-size"))
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

		// Only fetch parameters if actually needed
		if !c.Bool("skip-commit2") {
			if err := paramfetch.GetParams(lcli.ReqContext(c),build.ParametersJSON(), uint64(sectorSize)); err != nil {
				return xerrors.Errorf("getting params: %w", err)
			}
		}

		sbfs := &basicfs.Provider{
			Root: sbdir,
		}

		sb, err := ffiwrapper.New(sbfs, cfg)
		if err != nil {
			return err
		}

		//var sealTimings []SealingResult
		//var sealedSectors []abi.SectorInfo

		taskinfo.sectorSize = sectorSize
		taskinfo.spt = sb.SealProofType()
		gloabStart := time.Now()

		printTimeResult(time.Now(),time.Now(),"",c.String("result-filename"),true)
		if !c.Bool("only-post") {
			var err error
			//_, sealedSectors, err = runSeals(sb, sbfs, c.Int("num-sectors"), mid, sectorSize, []byte(c.String("ticket-preimage")), c.String("save-commit2-input"), c.Bool("skip-commit2"), c.Bool("skip-unseal"))
			err = runCombSeals(c, sb, mid, &taskinfo, sbdir, filename)
			if err != nil {
				return xerrors.Errorf("failed to run seals: %w", err)
			}
		}

		beforePost := time.Now()

		var challenge [32]byte
		rand.Read(challenge[:])

		bo := BenchResults{
			SectorSize: sectorSize,
			//SealingResults: sealTimings,
		}
		sealedSectors := []abi.SectorInfo{abi.SectorInfo{taskinfo.spt, taskinfo.number, taskinfo.cids.Sealed}}

		if !c.Bool("skip-commit2") && !c.Bool("only-precommit2") && !c.Bool("only-precommit1") {
			log.Info("generating winning post candidates")
			wipt, err := taskinfo.spt.RegisteredWinningPoStProof()
			if err != nil {
				return err
			}
			fcandidates, err := ffiwrapper.ProofVerifier.GenerateWinningPoStSectorChallenge(context.TODO(), wipt, mid, challenge[:], 1)
			if err != nil {
				return err
			}

			candidates := make([]abi.SectorInfo, len(fcandidates))
			for i, fcandidate := range fcandidates {
				candidates[i] = sealedSectors[fcandidate]
			}
			gencandidates := time.Now()

			log.Info("computing winning post snark (cold)")
			proof1, err := sb.GenerateWinningPoSt(context.TODO(), mid, candidates, challenge[:])
			if err != nil {
				return err
			}

			winnnigpost1 := time.Now()

			log.Info("computing winning post snark (hot)")
			proof2, err := sb.GenerateWinningPoSt(context.TODO(), mid, candidates, challenge[:])
			if err != nil {
				return err
			}

			winnningpost2 := time.Now()

			pvi1 := abi.WinningPoStVerifyInfo{
				Randomness:        abi.PoStRandomness(challenge[:]),
				Proofs:            proof1,
				ChallengedSectors: candidates,
				Prover:            mid,
			}
			ok, err := ffiwrapper.ProofVerifier.VerifyWinningPoSt(context.TODO(), pvi1)
			if err != nil {
				return err
			}
			if !ok {
				log.Error("post verification failed")
			}

			verifyWinnnigPost1 := time.Now()

			pvi2 := abi.WinningPoStVerifyInfo{
				Randomness:        abi.PoStRandomness(challenge[:]),
				Proofs:            proof2,
				ChallengedSectors: candidates,
				Prover:            mid,
			}

			ok, err = ffiwrapper.ProofVerifier.VerifyWinningPoSt(context.TODO(), pvi2)
			if err != nil {
				return err
			}
			if !ok {
				log.Error("post verification failed")
			}
			verifyWinningPost2 := time.Now()

			log.Info("computing window post snark (cold)")
			wproof1, _,err := sb.GenerateWindowPoSt(context.TODO(), mid, sealedSectors, challenge[:])
			if err != nil {
				return err
			}

			windowpost1 := time.Now()

			log.Info("computing window post snark (hot)")
			wproof2, _,err := sb.GenerateWindowPoSt(context.TODO(), mid, sealedSectors, challenge[:])
			if err != nil {
				return err
			}

			windowpost2 := time.Now()

			wpvi1 := abi.WindowPoStVerifyInfo{
				Randomness:        challenge[:],
				Proofs:            wproof1,
				ChallengedSectors: sealedSectors,
				Prover:            mid,
			}
			ok, err = ffiwrapper.ProofVerifier.VerifyWindowPoSt(context.TODO(), wpvi1)
			if err != nil {
				return err
			}
			if !ok {
				log.Error("post verification failed")
			}

			verifyWindowpost1 := time.Now()

			wpvi2 := abi.WindowPoStVerifyInfo{
				Randomness:        challenge[:],
				Proofs:            wproof2,
				ChallengedSectors: sealedSectors,
				Prover:            mid,
			}
			ok, err = ffiwrapper.ProofVerifier.VerifyWindowPoSt(context.TODO(), wpvi2)
			if err != nil {
				return err
			}
			if !ok {
				log.Error("post verification failed")
			}

			verifyWindowpost2 := time.Now()

			bo.PostGenerateCandidates = gencandidates.Sub(beforePost)
			bo.PostWinningProofCold = winnnigpost1.Sub(gencandidates)
			bo.PostWinningProofHot = winnningpost2.Sub(winnnigpost1)
			bo.VerifyWinningPostCold = verifyWinnnigPost1.Sub(winnningpost2)
			bo.VerifyWinningPostHot = verifyWinningPost2.Sub(verifyWinnnigPost1)

			bo.PostWindowProofCold = windowpost1.Sub(verifyWinningPost2)
			bo.PostWindowProofHot = windowpost2.Sub(windowpost1)
			bo.VerifyWindowPostCold = verifyWindowpost1.Sub(windowpost2)
			bo.VerifyWindowPostHot = verifyWindowpost2.Sub(verifyWindowpost1)
		}

		if c.Bool("json-out") {
			data, err := json.MarshalIndent(bo, "", "  ")
			if err != nil {
				return err
			}

			fmt.Println(string(data))
		} else {
			fmt.Printf("----\nresults (v26) (%d)\n", sectorSize)
			tp := taskinfo
			total := time.Now().Sub(gloabStart)
			if robench == "" {
				if c.Bool("only-precommit1") {
					fmt.Printf("----\nonly precommit1 results (v26) (%d)\n", sectorSize)
					fmt.Printf("seal: preCommit phase 1: %s (%s)\n", tp.SealResult.PreCommit1, bps(tp.sectorSize, tp.SealResult.PreCommit1))
					fmt.Println("===================================================")
					fmt.Println("")
					fmt.Printf("----\ntotal cost time: %s\n", total)
				} else if c.Bool("only-precommit2") {
					fmt.Printf("----\nonly precommit2 results (v26) (%d)\n", sectorSize)
					fmt.Printf("seal: preCommit phase 2: %s (%s)\n", tp.SealResult.PreCommit2, bps(tp.sectorSize, tp.SealResult.PreCommit2))
					fmt.Println("===================================================")
					fmt.Println("")
					fmt.Printf("----\ntotal cost time: %s\n", total)
				} else if c.Bool("only-sealcommit2") {
					fmt.Printf("----\nonly sealcommit2 results (v26) (%d)\n", sectorSize)
					fmt.Printf("seal: commit phase 1: %s (%s)\n", tp.SealResult.Commit1, bps(tp.sectorSize, tp.SealResult.Commit1))
					fmt.Printf("seal: commit phase 2: %s (%s)\n", tp.SealResult.Commit2, bps(tp.sectorSize, tp.SealResult.Commit2))
					fmt.Println("===================================================")
					fmt.Println("")
					fmt.Printf("----\ntotal cost time: %s\n", total)
				} else if c.Bool("only-post") {
					fmt.Printf("----\nonly post results (v26) (%d)\n", sectorSize)
				} else {
					fmt.Printf("----\nresults (v26) (%d)\n", sectorSize)
					fmt.Printf("seal: addPiece: %s (%s)\n", tp.SealResult.AddPiece, bps(tp.sectorSize, tp.SealResult.AddPiece)) // TODO: average across multiple sealings
					fmt.Printf("seal: preCommit phase 1: %s (%s)\n", tp.SealResult.PreCommit1, bps(tp.sectorSize, tp.SealResult.PreCommit1))
					fmt.Printf("seal: preCommit phase 2: %s (%s)\n", tp.SealResult.PreCommit2, bps(tp.sectorSize, tp.SealResult.PreCommit2))
					fmt.Printf("seal: commit phase 1: %s (%s)\n", tp.SealResult.Commit1, bps(tp.sectorSize, tp.SealResult.Commit1))
					fmt.Printf("seal: commit phase 2: %s (%s)\n", tp.SealResult.Commit2, bps(tp.sectorSize, tp.SealResult.Commit2))
					fmt.Println("===================================================")
					fmt.Println("")
					fmt.Printf("----\ntotal cost time: %s\n", total)

				}

				//fmt.Printf("seal: addPiece: %s (%s)\n", taskinfo.SealResult.AddPiece, bps(taskinfo.sectorSize, taskinfo.SealResult.AddPiece)) // TODO: average across multiple sealings
				//fmt.Printf("seal: preCommit phase 1: %s (%s)\n", taskinfo.SealResult.PreCommit1, bps(taskinfo.sectorSize, taskinfo.SealResult.PreCommit1))
				//fmt.Printf("seal: preCommit phase 2: %s (%s)\n", taskinfo.SealResult.PreCommit2, bps(taskinfo.sectorSize, taskinfo.SealResult.PreCommit2))
				//fmt.Printf("seal: commit phase 1: %s (%s)\n", taskinfo.SealResult.Commit1, bps(taskinfo.sectorSize, taskinfo.SealResult.Commit1))
				//fmt.Printf("seal: commit phase 2: %s (%s)\n", taskinfo.SealResult.Commit2, bps(taskinfo.sectorSize, taskinfo.SealResult.Commit2))
				//fmt.Printf("seal: verify: %s\n", taskinfo.SealResult.Verify)
				////if !c.Bool("skip-unseal") {
				////	fmt.Printf("unseal: %s  (%s)\n", taskinfo.SealResult.Unseal, bps(taskinfo.sectorSize, taskinfo.SealResult.Unseal))
				////}
				//fmt.Println("")

				if !c.Bool("skip-commit2") && !c.Bool("only-precommit2") && !c.Bool("only-precommit1") {
					fmt.Printf("generate candidates: %s (%s)\n", bo.PostGenerateCandidates, bps(bo.SectorSize*abi.SectorSize(len(bo.SealingResults)), bo.PostGenerateCandidates))
					fmt.Printf("compute winnnig post proof (cold): %s\n", bo.PostWinningProofCold)
					fmt.Printf("compute winnnig post proof (hot): %s\n", bo.PostWinningProofHot)
					fmt.Printf("verify winnnig post proof (cold): %s\n", bo.VerifyWinningPostCold)
					fmt.Printf("verify winnnig post proof (hot): %s\n\n", bo.VerifyWinningPostHot)

					fmt.Printf("compute window post proof (cold): %s\n", bo.PostWindowProofCold)
					fmt.Printf("compute window post proof (hot): %s\n", bo.PostWindowProofHot)
					fmt.Printf("verify window post proof (cold): %s\n", bo.VerifyWindowPostCold)
					fmt.Printf("verify window post proof (hot): %s\n", bo.VerifyWindowPostHot)
				}
			}
		}
		return nil
	},
}

//func runSeals(sb *ffiwrapper.Sealer, sbfs *basicfs.Provider, numSectors int, mid abi.ActorID, sectorSize abi.SectorSize, ticketPreimage []byte, saveC2inp string, skipc2, skipunseal bool)  error {
func runCombSeals(c *cli.Context, sb *ffiwrapper.Sealer, mid abi.ActorID, taskinfo *taskInfo, sbdir, filename string) error {

	var wg sync.WaitGroup
	// 套做P1
	if c.Int("p1Count") > 0 {
		spt, err := ffiwrapper.SealProofTypeFromSectorSize(taskinfo.sectorSize)
		if err != nil {
			return err
		}

		cfg := &ffiwrapper.Config{
			SealProofType: spt,
		}

		for i := abi.SectorNumber(1); i <= abi.SectorNumber(c.Int("p1Count")); i++ {
			wg.Add(1)
			go func(i abi.SectorNumber) {
				sdir, err := homedir.Expand(c.String("storage-dir"))
				if err != nil {
					wg.Done()
					return
				}

				os.MkdirAll(sdir, 0775)

				tsdir, err := ioutil.TempDir(sdir, "bench")
				if err != nil {
					wg.Done()
					return
				}
				defer func() {
					if err := os.RemoveAll(tsdir); err != nil {
						log.Warn("remove all: ", err)
					}
				}()

				// TODO: pretty sure this isnt even needed?
				if err := os.MkdirAll(tsdir, 0775); err != nil {
					wg.Done()
					return
				}

				log.Infof("========tsdir=%s\n", tsdir)
				sbfs := &basicfs.Provider{
					Root: tsdir,
				}

				sb1, err := ffiwrapper.New(sbfs, cfg)
				if err == nil {

					// addpiece
					start := time.Now()
					r := rand.New(rand.NewSource(100 + int64(i)))
					pi, err := sb1.AddPiece(context.TODO(), abi.SectorID{taskinfo.mid, i}, nil, abi.PaddedPieceSize(taskinfo.sectorSize).Unpadded(), r)
					fmt.Printf("\n|p1-0%d|%s|%s|%s\n", i, start.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(start))

					// SealPreCommit1
					log.Infof("Runing SealPreCommit1 for sector %d ...\n", i)
					ticket := abi.SealRandomness(taskinfo.trand[:])
					taskinfo.ticket = ticket
					pieces := []abi.PieceInfo{pi}
					start = time.Now()
					_, err = sb1.SealPreCommit1(context.TODO(), abi.SectorID{taskinfo.mid, i}, ticket, pieces)
					if err != nil {
						log.Error(err)
						wg.Done()
						return
					}
					//SealResult.PreCommit1=time.Now().Sub(start)
					//fmt.Printf("\np1-0%d|%s|%s|%s\n", i, start.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(start))
					printTimeResult(start,time.Now(),fmt.Sprintf("p1-%d",i),c.String("result-filename"),false)
				}
				wg.Done()
			}(i)
		}

	}

	for i := abi.SectorNumber(1); i <= abi.SectorNumber(c.Int("num-sectors")); i++ {

		if c.Bool("only-precommit1") {
			// SealPreCommit1
			log.Infof("Runing SealPreCommit1 for sector %d ...\n", i)
			start:=time.Now()
			if err := precommit1(context.TODO(), sb, taskinfo); err != nil {
				log.Error(err)
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("precommit1"),c.String("result-filename"),false)
			// 复制sealed文件
			//CopyDir(sbdir+"/sealed",sbdir+"/sealedbak")
			fileCopy(sbdir+"/sealed/s-t01000-1", sbdir+"/sealed/s-t01000-1.bak")
			// gob序列化保存文件
			serializeAndSaveSectorInfo(taskinfo, filename)
		} else if c.Bool("only-precommit2") {
			//os.RemoveAll(sbdir+"/sealed")
			//CopyDir(sbdir+"/sealedbak",sbdir+"/sealed")
			fileCopy(sbdir+"/sealed/s-t01000-1.bak", sbdir+"/sealed/s-t01000-1")
			WalkDirAndDelete(sbdir+"/cache/s-t01000-1", "data-tree-c", "data-tree-r-last")

			// SealPreCommit2
			log.Infof("Runing SealPreCommit2 for sector %d ...\n", i)
			start:=time.Now()
			if err := precommit2(context.TODO(), sb, taskinfo); err != nil {
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("precommit2"),c.String("result-filename"),false)
			// gob序列化保存文件
			serializeAndSaveSectorInfo(taskinfo, filename)
		} else if c.Bool("only-sealcommit2") {
			// SealCommit1
			log.Infof("Runing SealCommit1 for sector %d ...\n", i)
			if err := sealCommit1(context.TODO(), sb, taskinfo); err != nil {
				log.Error(err)
				return nil
			}

			// SealCommit2
			log.Infof("Runing SealCommit2 for sector %d ...\n", i)
			start:=time.Now()
			if err := sealCommit2(context.TODO(), sb, taskinfo); err != nil {
				log.Error(err)
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("commit2"),c.String("result-filename"),false)
			// gob序列化保存文件
			serializeAndSaveSectorInfo(taskinfo, filename)
		} else {
			sid := abi.SectorID{
				Miner:  mid,
				Number: i,
			}

			log.Info("Writing piece into sector...")

			//r := rand.New(rand.NewSource(99 + int64(i)))
			trand := blake2b.Sum256([]byte(""))
			ticket := abi.SealRandomness(trand[:])
			//构造任务数据记录结构
			taskinfo.number = i
			taskinfo.sid = sid
			taskinfo.mid = mid
			taskinfo.trand = trand
			taskinfo.ticket = ticket
			// add piece
			log.Infof("Write piece into sector %d ...\n", i)
			start:=time.Now()
			if err := addPiece(context.TODO(), sb, taskinfo); err != nil {
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("addpiece"),c.String("result-filename"),false)

			// SealPreCommit1
			log.Infof("Runing SealPreCommit1 for sector %d ...\n", i)
			start=time.Now()
			if err := precommit1(context.TODO(), sb, taskinfo); err != nil {
				log.Error(err)
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("precommit1"),c.String("result-filename"),false)
			// 复制sealed文件
			fileCopy(sbdir+"/sealed/s-t01000-1", sbdir+"/sealed/s-t01000-1.bak")

			// SealPreCommit2
			log.Infof("Runing SealPreCommit2 for sector %d ...\n", i)
			start=time.Now()
			if err := precommit2(context.TODO(), sb, taskinfo); err != nil {
				log.Error(err)
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("precommit2"),c.String("result-filename"),false)

			// SealCommit1
			log.Infof("Runing SealCommit1 for sector %d ...\n", i)
			start=time.Now()
			if err := sealCommit1(context.TODO(), sb, taskinfo); err != nil {
				log.Error(err)
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("commit1"),c.String("result-filename"),false)

			// SealCommit2
			log.Infof("Runing SealCommit2 for sector %d ...\n", i)
			start=time.Now()
			if err := sealCommit2(context.TODO(), sb, taskinfo); err != nil {
				log.Error(err)
				return nil
			}
			printTimeResult(start,time.Now(),fmt.Sprintf("commit2"),c.String("result-filename"),false)

			log.Infof("====sector %d finished all task\n", i)

			// gob序列化保存文件
			serializeAndSaveSectorInfo(taskinfo, filename)

		}

		//// add piece
		//log.Infof("Write piece into sector %d ...\n", i)
		//if addPiece(context.TODO(), sb, taskinfo) != nil {
		//	//wg.Done()
		//	return nil, nil, nil
		//}

		//// SealPreCommit1
		//log.Infof("Runing SealPreCommit1 for sector %d ...\n", i)
		//if precommit1(context.TODO(), sb, taskinfo) != nil {
		//	//wg.Done()
		//	return nil, nil, nil
		//}

		//// SealPreCommit2
		//log.Infof("Runing SealPreCommit2 for sector %d ...\n", i)
		//if precommit2(context.TODO(), sb, taskinfo) != nil {
		//	return nil, nil, nil
		//}

		//// SealCommit1
		//log.Infof("Runing SealCommit1 for sector %d ...\n", i)
		//if sealCommit1(context.TODO(), sb, taskinfo) != nil {
		//	return nil, nil, nil
		//}

		//// SealCommit2
		//log.Infof("Runing SealCommit2 for sector %d ...\n", i)
		//if sealCommit2(context.TODO(), sb, taskinfo) != nil {
		//	return nil, nil, nil
		//}
	}

	wg.Wait()

	return nil
	//return sealTimings, sealedSectors, nil
}
