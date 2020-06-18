package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	paramfetch "github.com/filecoin-project/go-paramfetch"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var sealBench2Cmd = &cli.Command{
	Name: "sealing2",
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
		&cli.StringFlag{
			Name:  "sector-count",
			Value: "1",			Usage: "sector count default 1",
		},
		&cli.StringFlag{
			Name:  "pre1-count",
			Value: "1",
			Usage: "pre1 parallel count default 1",
		},
		&cli.StringFlag{
			Name:  "pre2-count",
			Value: "1",
			Usage: "pre2 parallel count default 1",
		},
		&cli.StringFlag{
			Name:  "commit2-count",
			Value: "1",
			Usage: "commit2 parallel count default 1",
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
		&cli.StringFlag{
			Name:  "save-commit2-input",
			Usage: "Save commit2 input to a file",
		},
	},
	Action: func(c *cli.Context) error {
		if c.Bool("no-gpu") {
			os.Setenv("BELLMAN_NO_GPU", "1")
		}

		robench := c.String("benchmark-existing-sectorbuilder")

		var sbdir string

		if robench == "" {
			sdir, err := homedir.Expand(c.String("storage-dir"))
			if err != nil {
				return err
			}

			os.MkdirAll(sdir, 0775)

			tsdir, err := ioutil.TempDir(sdir, "bench")
			fmt.Println("tsdir = ",tsdir)
			if err != nil {
				return err
			}
			//defer func() {
			//	if err := os.RemoveAll(tsdir); err != nil {
			//		log.Warn("remove all: ", err)
			//	}
			//}()
			sbdir = tsdir
		} else {
			exp, err := homedir.Expand(robench)
			if err != nil {
				return err
			}
			sbdir = exp
		}

		maddr, err := address.NewFromString(c.String("miner-addr"))
		if err != nil {
			return err
		}

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

		if robench == "" {
			if err := os.MkdirAll(sbdir, 0775); err != nil {
				return err
			}
		}

		if !c.Bool("skip-commit2") {
			if err := paramfetch.GetParams(lcli.ReqContext(c),build.ParametersJSON(), uint64(sectorSize)); err != nil {
				return xerrors.Errorf("getting params: %w", err)
			}
		}


		if c.Bool("save-sectorinfo"){
			sbdir="/opt/lotus/bench-save"
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			fmt.Println("sectorinfo storage path:",sbdir)
			fmt.Println("+++++++++++++++++++++++++++++++++++++")
			os.RemoveAll(sbdir)
			if err := os.MkdirAll(sbdir, 0775); err != nil {
				fmt.Println(err)
				return nil
			}
		}

		var taskinfo taskInfo
		filename:=sbdir+"/sectorinfo.gob"

		if c.Bool("only-precommit1") || c.Bool("only-precommit2") || c.Bool("only-sealcommit2"){
			sbdir="/opt/lotus/bench-save"
			filename=sbdir+"/sectorinfo.gob"
			sectorInfoExist,err:=PathExists(filename)
			if err!=nil{
				fmt.Println(err)
				return err
			}

			if !sectorInfoExist{
				fmt.Println("=====sectorinfo.gob 不存在，无法执行任务")
				return nil
			}else {
				tOut:=TaskInfoOut{}
				tOut.GobDeSerializeFromFile(filename)
				taskinfo.transfer(tOut)
			}
		}

		sbfs := &basicfs.Provider{
			Root: sbdir,
		}

		sb, err := ffiwrapper.New(sbfs, cfg)
		if err != nil {
			return err
		}

		amid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}

		mid := abi.ActorID(amid)
		numSectors:=abi.SectorNumber(1)
		gloabStart:=time.Now()


		// wait gorution return
		var wg sync.WaitGroup
		for i := abi.SectorNumber(1); i <= numSectors && robench == ""; i++ {

			log.Infof("new gorution %d ...\n", i)
			wg.Add(1)
			go func(i abi.SectorNumber) error {

				if c.Bool("only-precommit1"){
					// SealPreCommit1
					log.Infof("Runing SealPreCommit1 for sector %d ...\n",i)
					if err=precommit1(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}
					// 复制sealed文件
					//CopyDir(sbdir+"/sealed",sbdir+"/sealedbak")
					fileCopy(sbdir+"/sealed/s-t01000-1",sbdir+"/sealed/s-t01000-1.bak")
					// gob序列化保存文件
					serializeAndSaveSectorInfo(&taskinfo,filename)
				} else if c.Bool("only-precommit2"){
					//os.RemoveAll(sbdir+"/sealed")
					//CopyDir(sbdir+"/sealedbak",sbdir+"/sealed")
					fileCopy(sbdir+"/sealed/s-t01000-1.bak",sbdir+"/sealed/s-t01000-1")
					WalkDirAndDelete(sbdir+"/cache/s-t01000-1","data-tree-c","data-tree-r-last")

					// SealPreCommit2
					log.Infof("Runing SealPreCommit2 for sector %d ...\n",i)
					if err=precommit2(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}
					// gob序列化保存文件
					serializeAndSaveSectorInfo(&taskinfo,filename)
				}else if c.Bool("only-sealcommit2"){
					// SealCommit1
					log.Infof("Runing SealCommit1 for sector %d ...\n",i)
					if err=sealCommit1(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}

					// SealCommit2
					log.Infof("Runing SealCommit2 for sector %d ...\n",i)
					if err=sealCommit2(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}
					// gob序列化保存文件
					serializeAndSaveSectorInfo(&taskinfo,filename)
				}else {
					sid := abi.SectorID{
						Miner:  mid,
						Number: i,
					}
					trand := sha256.Sum256([]byte(c.String("ticket-preimage")))
					taskinfo=taskInfo{
						number: i,
						mid: mid,
						sectorSize: sectorSize,
						spt: spt,
						trand: trand,
						sid: sid,
					}

					// add piece
					log.Infof("Write piece into sector %d ...\n",i)
					if err=addPiece(context.TODO(),sb,&taskinfo);err!=nil{
						wg.Done()
						return nil
					}

					// SealPreCommit1
					log.Infof("Runing SealPreCommit1 for sector %d ...\n",i)
					if err=precommit1(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}
					// 复制sealed文件
					fileCopy(sbdir+"/sealed/s-t01000-1",sbdir+"/sealed/s-t01000-1.bak")

					// SealPreCommit2
					log.Infof("Runing SealPreCommit2 for sector %d ...\n",i)
					if err:=precommit2(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}

					// SealCommit1
					log.Infof("Runing SealCommit1 for sector %d ...\n",i)
					if err=sealCommit1(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}

					// SealCommit2
					log.Infof("Runing SealCommit2 for sector %d ...\n",i)
					if err=sealCommit2(context.TODO(),sb,&taskinfo);err!=nil{
						log.Error(err)
						wg.Done()
						return nil
					}

					log.Infof("====sector %d finished all task\n",i)

					// gob序列化保存文件
					serializeAndSaveSectorInfo(&taskinfo,filename)

				}
				wg.Done()
				return nil
			}(i)

		}

		// wait all gorution back
		wg.Wait()
		total:=time.Now().Sub(gloabStart)

		if c.Bool("json-out") {
			data, err := json.MarshalIndent(taskinfo, "", "  ")
			if err != nil {
				return err
			}

			fmt.Println(string(data))
		} else {
			tp:=taskinfo
			if c.Bool("only-precommit1"){
				fmt.Printf("----\nonly precommit1 results (v25) (%d)\n", sectorSize)
				fmt.Printf("seal: preCommit phase 1: %s (%s)\n", tp.SealResult.PreCommit1, bps(tp.sectorSize, tp.SealResult.PreCommit1))
				fmt.Println("===================================================")
				fmt.Println("")
				fmt.Printf("----\ntotal cost time: %s\n",total)
			}else if c.Bool("only-precommit2"){
				fmt.Printf("----\nonly precommit2 results (v25) (%d)\n", sectorSize)
				fmt.Printf("seal: preCommit phase 2: %s (%s)\n", tp.SealResult.PreCommit2, bps(tp.sectorSize, tp.SealResult.PreCommit2))
				fmt.Println("===================================================")
				fmt.Println("")
				fmt.Printf("----\ntotal cost time: %s\n",total)
			}else if c.Bool("only-sealcommit2"){
				fmt.Printf("----\nonly sealcommit2 results (v25) (%d)\n", sectorSize)
				fmt.Printf("seal: commit phase 1: %s (%s)\n", tp.SealResult.Commit1, bps(tp.sectorSize, tp.SealResult.Commit1))
				fmt.Printf("seal: commit phase 2: %s (%s)\n", tp.SealResult.Commit2, bps(tp.sectorSize, tp.SealResult.Commit2))
				fmt.Println("===================================================")
				fmt.Println("")
				fmt.Printf("----\ntotal cost time: %s\n",total)
			}else{
				fmt.Printf("----\nresults (v25) (%d)\n", sectorSize)
				fmt.Printf("seal: addPiece: %s (%s)\n", tp.SealResult.AddPiece, bps(tp.sectorSize, tp.SealResult.AddPiece)) // TODO: average across multiple sealings
				fmt.Printf("seal: preCommit phase 1: %s (%s)\n", tp.SealResult.PreCommit1, bps(tp.sectorSize, tp.SealResult.PreCommit1))
				fmt.Printf("seal: preCommit phase 2: %s (%s)\n", tp.SealResult.PreCommit2, bps(tp.sectorSize, tp.SealResult.PreCommit2))
				fmt.Printf("seal: commit phase 1: %s (%s)\n", tp.SealResult.Commit1, bps(tp.sectorSize, tp.SealResult.Commit1))
				fmt.Printf("seal: commit phase 2: %s (%s)\n", tp.SealResult.Commit2, bps(tp.sectorSize, tp.SealResult.Commit2))
				fmt.Println("===================================================")
				fmt.Println("")
				fmt.Printf("----\ntotal cost time: %s\n",total)
			}

		}
		return nil
	},
}

/* 获取指定路径下以及所有子目录下的所有文件，可匹配后缀过滤（suffix为空则不过滤）*/
func WalkDirAndDelete(dir, tree_c,tree_r string)  error {

	err := filepath.Walk(dir, func(fname string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			//忽略目录
			return nil
		}

		//if len(suffix) == 0 || strings.HasSuffix(strings.ToLower(fi.Name()), suffix) {
		//	//文件后缀匹配
		//	files = append(files, fname)
		//}

		fmt.Println("====filename:",strings.ToLower(fi.Name()))
		if strings.Contains(strings.ToLower(fi.Name()),tree_r)||strings.Contains(strings.ToLower(fi.Name()),tree_c) {
			fmt.Println("====del",dir+"/"+fi.Name())
			//文件后缀匹配
			if err=os.Remove(dir+"/"+fi.Name());err!=nil{
				fmt.Errorf("",err)
				return err
			}

		}
		return nil
	})

	return  err
}

func CopyDir(srcPath, desPath string) error {
	//检查目录是否正确
	if srcInfo, err := os.Stat(srcPath); err != nil {
		return err
	} else {
		if !srcInfo.IsDir() {
			return errors.New("源路径不是一个正确的目录！")
		}
	}

	if desInfo, err := os.Stat(desPath); err != nil {
		return err
	} else {
		if !desInfo.IsDir() {
			return errors.New("目标路径不是一个正确的目录！")
		}
	}

	if strings.TrimSpace(srcPath) == strings.TrimSpace(desPath) {
		return errors.New("源路径与目标路径不能相同！")
	}

	err := filepath.Walk(srcPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}

		//复制目录是将源目录中的子目录复制到目标路径中，不包含源目录本身
		if path == srcPath {
			return nil
		}

		//生成新路径
		destNewPath := strings.Replace(path, srcPath, desPath, -1)

		if !f.IsDir() {
			CopyFile(path, destNewPath)
		} else {
			if !FileIsExisted(destNewPath) {
				return MakeDir(destNewPath)
			}
		}

		return nil
	})

	return err
}
func MakeDir(dir string) error {
	if !FileIsExisted(dir) {
		if err := os.MkdirAll(dir, 0777); err != nil { //os.ModePerm
			fmt.Println("MakeDir failed:", err)
			return err
		}
	}
	return nil
}
func FileIsExisted(filename string) bool {
	existed := true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		existed = false
	}
	return existed
}
//使用io.Copy
func CopyFile(src, des string) (written int64, err error) {
	srcFile, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	//获取源文件的权限
	fi, _ := srcFile.Stat()
	perm := fi.Mode()

	//desFile, err := os.Create(des)  //无法复制源文件的所有权限
	desFile, err := os.OpenFile(des, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)  //复制源文件的所有权限
	if err != nil {
		return 0, err
	}
	defer desFile.Close()

	return io.Copy(desFile, srcFile)
}
func fileCopy(src, dst string) (int64, error) {
	start:=time.Now()
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}

	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	total:=time.Now().Sub(start)
	fmt.Printf("====copy %s to %s cost %s\n\n",src,dst,total)
	return nBytes, err
}

func serializeAndSaveSectorInfo(info *taskInfo,filename string)  {
	tOut:=TaskInfoOut{}
	tOut.transfer(info)
	tOut.GobSerializeAndWriteFile(filename)
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		log.Errorf("path:%s not found",path)
		return false, nil
	}
	return false, err
}
