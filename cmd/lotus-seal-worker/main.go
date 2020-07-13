package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	paramfetch "github.com/filecoin-project/go-paramfetch"
	"github.com/filecoin-project/sector-storage/ffiwrapper"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	"github.com/filecoin-project/lotus/node/repo"
	sectorstorage "github.com/filecoin-project/sector-storage"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"

	_ "net/http/pprof"
)

var log = logging.Logger("main")

const FlagStorageRepo = "workerrepo"
const DefaultAddPieceSize = 5
const DefaultPre1CommitSize = 4
const DefaultPre2CommitSize = 1
const DefaultCommit1Size = 1
const DefaultCommit2Size = 1

func init() {
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
}

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:7070", nil)
	}()
	lotuslog.SetupLogLevels()

	log.Info("Starting lotus worker")

	local := []*cli.Command{
		runCmd,
		setTasksNumberCmd,
	}

	app := &cli.App{
		Name:    "lotus-seal-worker",
		Usage:   "Remote storage miner worker",
		Version: build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagStorageRepo,
				EnvVars: []string{"WORKER_PATH"},
				Value:   "~/.lotusworker", // TODO: Consider XDG_DATA_HOME
			},
			&cli.StringFlag{
				Name:    "storagerepo",
				EnvVars: []string{"LOTUS_STORAGE_PATH"},
				Value:   "~/.lotusstorage", // TODO: Consider XDG_DATA_HOME
			},
			&cli.BoolFlag{
				Name:  "enable-gpu-proving",
				Usage: "enable use of GPU for mining operations",
				Value: true,
			},
		},

		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Worker

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start lotus worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "address",
			Usage: "Locally reachable address",
		},
		&cli.StringFlag{
			Name:  "pub-address",
			Usage: "publick reachable address,指定端口映射的公网ip:port",
		},
		&cli.BoolFlag{
			Name:  "no-local-storage",
			Usage: "don't use storageminer repo for sector storage",
		},
		&cli.BoolFlag{
			Name:  "precommit1",
			Usage: "enable precommit1 (32G sectors: 1 core, 128GiB Memory)",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "precommit2",
			Usage: "enable precommit2 (32G sectors: all cores, 96GiB Memory)",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "commit1",
			Usage: "enable commit1 (32G sectors: all cores or GPUs, 128GiB Memory + 64GiB swap)",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "commit2",
			Usage: "enable commit2 (32G sectors: all cores or GPUs, 128GiB Memory + 64GiB swap)",
			Value: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("enable-gpu-proving") {
			if err := os.Setenv("BELLMAN_NO_GPU", "true"); err != nil {
				return xerrors.Errorf("could not set no-gpu env: %+v", err)
			}
		}

		if cctx.String("address") == "" {
			return xerrors.Errorf("--address flag is required")
		}

		// Connect to storage-miner
		var nodeApi api.StorageMiner
		var closer func()
		var err error
		for {
			nodeApi, closer, err = lcli.GetStorageMinerAPI(cctx)
			if err == nil {
				break
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()
		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		v, err := nodeApi.Version(ctx)
		if err != nil {
			return err
		}
		if v.APIVersion != build.APIVersion {
			return xerrors.Errorf("lotus-storage-miner API version doesn't match: local: ", api.Version{APIVersion: build.APIVersion})
		}
		log.Infof("Remote version %s", v)

		watchMinerConn(ctx, cctx, nodeApi)

		// Check params

		act, err := nodeApi.ActorAddress(ctx)
		if err != nil {
			return err
		}
		ssize, err := nodeApi.ActorSectorSize(ctx, act)
		if err != nil {
			return err
		}

		if cctx.Bool("commit1") {
			if err := paramfetch.GetParams(ctx, build.ParametersJSON(), uint64(ssize)); err != nil {
				return xerrors.Errorf("get params: %w", err)
			}
		}

		var taskTypes []sealtasks.TaskType

		taskTypes = append(taskTypes, sealtasks.TTFetch)
		if cctx.Bool("precommit1") {
			taskTypes = append(taskTypes, sealtasks.TTPreCommit1)
			taskTypes = append(taskTypes, sealtasks.TTAddPiece)
		}
		if cctx.Bool("precommit2") {
			taskTypes = append(taskTypes, sealtasks.TTPreCommit2)
		}
		if cctx.Bool("commit1") {
			taskTypes = append(taskTypes, sealtasks.TTCommit1)
		}
		if cctx.Bool("commit2") {
			taskTypes = append(taskTypes, sealtasks.TTCommit2)
		}
		taskTypes = append(taskTypes, sealtasks.TTFinalize)

		if len(taskTypes) == 0 {
			return xerrors.Errorf("no task types specified")
		}

		// Open repo

		repoPath := cctx.String(FlagStorageRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Worker); err != nil {
				return err
			}

			lr, err := r.Lock(repo.Worker)
			if err != nil {
				return err
			}

			var localPaths []stores.LocalPath

			if !cctx.Bool("no-local-storage") {
				b, err := json.MarshalIndent(&stores.LocalStorageMeta{
					ID:       stores.ID(uuid.New().String()),
					Weight:   10,
					CanSeal:  true,
					CanStore: false,
				}, "", "  ")
				if err != nil {
					return xerrors.Errorf("marshaling storage config: %w", err)
				}

				if err := ioutil.WriteFile(filepath.Join(lr.Path(), "sectorstore.json"), b, 0644); err != nil {
					return xerrors.Errorf("persisting storage metadata (%s): %w", filepath.Join(lr.Path(), "sectorstore.json"), err)
				}

				localPaths = append(localPaths, stores.LocalPath{
					Path: lr.Path(),
				})
			}

			if err := lr.SetStorage(func(sc *stores.StorageConfig) {
				sc.StoragePaths = append(sc.StoragePaths, localPaths...)
			}); err != nil {
				return xerrors.Errorf("set storage config: %w", err)
			}

			{
				// init datastore for r.Exists
				_, err := lr.Datastore("/")
				if err != nil {
					return err
				}
			}
			if err := lr.Close(); err != nil {
				return xerrors.Errorf("close repo: %w", err)
			}
		}

		lr, err := r.Lock(repo.Worker)
		if err != nil {
			return err
		}

		log.Info("Opening local storage; connecting to master")

		localStore, err := stores.NewLocal(ctx, lr, nodeApi, []string{"http://" + cctx.String("address") + "/remote"})
		if err != nil {
			return err
		}

		// Setup remote sector store
		spt, err := ffiwrapper.SealProofTypeFromSectorSize(ssize)
		if err != nil {
			return xerrors.Errorf("getting proof type: %w", err)
		}

		sminfo, err := lcli.GetAPIInfo(cctx, repo.StorageMiner)
		if err != nil {
			return xerrors.Errorf("could not get api info: %w", err)
		}

		remote := stores.NewRemote(localStore, nodeApi, sminfo.AuthHeader())

		// Create / expose the worker
		log.Infof("============================ 手动 NEW localworker =========\n SealProof:%+v \n,TaskTypes:%+v \n,remote:%+v \n,localStore:%+v \n,nodeApi:,%+v \n",
			spt, taskTypes, remote, localStore, nodeApi)
		workerApi := &worker{
			LocalWorker: sectorstorage.NewLocalWorker(sectorstorage.WorkerConfig{
				SealProof: spt,
				TaskTypes: taskTypes,
			}, remote, localStore, nodeApi),
		}

		mux := mux.NewRouter()

		log.Info("Setting up control endpoint at " + cctx.String("address"))

		rpcServer := jsonrpc.NewServer()
		rpcServer.Register("Filecoin", apistruct.PermissionedWorkerAPI(workerApi))

		mux.Handle("/rpc/v0", rpcServer)
		mux.PathPrefix("/remote").HandlerFunc((&stores.FetchHandler{Local: localStore}).ServeHTTP)
		mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

		ah := &auth.Handler{
			Verify: nodeApi.AuthVerify,
			Next:   mux.ServeHTTP,
		}

		srv := &http.Server{
			Handler: ah,
			BaseContext: func(listener net.Listener) context.Context {
				return ctx
			},
		}

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", cctx.String("address"))
		if err != nil {
			return err
		}

		log.Info("Waiting for tasks")

		go func() {
			if cctx.String("pub-address") == "" {
				if err := nodeApi.WorkerConnect(ctx, "ws://"+cctx.String("address")+"/rpc/v0"); err != nil {
					log.Errorf("Registering worker failed: %+v", err)
					cancel()
					return
				}
				log.Info("============================ worker连接 miner=========:", "ws://"+cctx.String("address")+"/rpc/v0")
			} else {
				if err := nodeApi.WorkerConnect(ctx, "ws://"+cctx.String("pub-address")+"/rpc/v0"); err != nil {
					log.Errorf("Registering worker failed: %+v", err)
					cancel()
					return
				}
				log.Info("============================ worker连接 miner=========:", "ws://"+cctx.String("pub-address")+"/rpc/v0")
			}
		}()

		return srv.Serve(nl)
	},
}

var setTasksNumberCmd = &cli.Command{
	Name:  "set-tasks-number",
	Usage: "Set the number of acceptable tasks",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "apsize",
			Usage: "the number of addpiece tasks the worker can accept",
			Value: DefaultAddPieceSize,
		},
		&cli.UintFlag{
			Name:  "p1size",
			Usage: "the number of pre1commit tasks the worker can accept",
			Value: DefaultPre1CommitSize,
		},
		&cli.UintFlag{
			Name:  "p2size",
			Usage: "the number of pre2commit tasks the worker can accept",
			Value: DefaultPre2CommitSize,
		},
		&cli.UintFlag{
			Name:  "c1size",
			Usage: "the number of commit1 tasks the worker can accept",
			Value: DefaultCommit1Size,
		},
		&cli.UintFlag{
			Name:  "c2size",
			Usage: "the number of commit2 tasks the worker can accept",
			Value: DefaultCommit2Size,
		},
	},
	Action: func(cctx *cli.Context) error {
		hostname, err := os.Hostname()
		if err != nil {
			return xerrors.Errorf("getting hostname : %w", err)
		}
		tc := &sectorstorage.TaskConfig{}

		apSize := cctx.Uint("apsize")
		if apSize > 255 {
			return xerrors.Errorf("constant %d overflows uint8", apSize)
		}
		tc.AddPieceSize = uint8(apSize)

		p1Size := cctx.Uint("p1size")
		if p1Size > 255 {
			return xerrors.Errorf("constant %d overflows uint8", p1Size)
		}
		tc.Pre1CommitSize = uint8(p1Size)

		p2Size := cctx.Uint("p2size")
		if p2Size > 255 {
			return xerrors.Errorf("constant %d overflows uint8", p2Size)
		}
		tc.Pre2CommitSize = uint8(p2Size)

		c1Size := cctx.Uint("c1size")
		if c1Size > 255 {
			return xerrors.Errorf("constant %d overflows uint8", c1Size)
		}
		tc.Commit1 = uint8(c1Size)

		c2Size := cctx.Uint("c2size")
		if c2Size > 255 {
			return xerrors.Errorf("constant %d overflows uint8", c2Size)
		}
		tc.Commit2 = uint8(c2Size)

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		data, err := json.Marshal(tc)
		if err != nil {
			return err
		}
		err = nodeApi.WorkerConf(ctx, hostname, data)

		if err != nil {
			return xerrors.Errorf("set tasks number err: %w", err)
		}
		return nil
	},
}

func watchMinerConn(ctx context.Context, cctx *cli.Context, nodeApi api.StorageMiner) {
	go func() {
		closing, err := nodeApi.Closing(ctx)
		if err != nil {
			log.Errorf("failed to get remote closing channel: %+v", err)
		}

		select {
		case <-closing:
		case <-ctx.Done():
		}

		if ctx.Err() != nil {
			return // graceful shutdown
		}

		log.Warnf("Connection with miner node lost, restarting")

		exe, err := os.Executable()
		if err != nil {
			log.Errorf("getting executable for auto-restart: %+v", err)
		}

		log.Sync()

		// TODO: there are probably cleaner/more graceful ways to restart,
		//  but this is good enough for now (FSM can recover from the mess this creates)
		if err := syscall.Exec(exe, []string{exe, "run",
			fmt.Sprintf("--address=%s", cctx.String("address")),
			fmt.Sprintf("--no-local-storage=%t", cctx.Bool("no-local-storage")),
			fmt.Sprintf("--precommit1=%t", cctx.Bool("precommit1")),
			fmt.Sprintf("--precommit2=%t", cctx.Bool("precommit2")),
			fmt.Sprintf("--commit=%t", cctx.Bool("commit")),
		}, os.Environ()); err != nil {
			fmt.Println(err)
		}
	}()
}
