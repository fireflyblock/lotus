package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/go-units"
	"sync"

	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	paramfetch "github.com/filecoin-project/go-paramfetch"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apistruct"
	"github.com/filecoin-project/lotus/build"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/lib/lotuslog"
	"github.com/filecoin-project/lotus/lib/rpcenc"
	"github.com/filecoin-project/lotus/node/repo"
	sectorstorage "github.com/filecoin-project/sector-storage"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
)

var log = logging.Logger("main")

const FlagWorkerRepo = "worker-repo"

// TODO remove after deprecation period
const FlagWorkerRepoDeprecation = "workerrepo"

func main() {
	build.RunningNodeType = build.NodeWorker

	lotuslog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
		//infoCmd,
		storageCmd,
	}

	app := &cli.App{
		Name:    "lotus-worker",
		Usage:   "Remote miner worker",
		Version: build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagWorkerRepo,
				Aliases: []string{FlagWorkerRepoDeprecation},
				EnvVars: []string{"LOTUS_WORKER_PATH", "WORKER_PATH"},
				Value:   "~/.lotusworker", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env WORKER_PATH are DEPRECATION, will REMOVE SOON", FlagWorkerRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "miner-repo",
				Aliases: []string{"storagerepo"},
				EnvVars: []string{"LOTUS_MINER_PATH", "LOTUS_STORAGE_PATH"},
				Value:   "~/.lotusminer", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify miner repo path. flag storagerepo and env LOTUS_STORAGE_PATH are DEPRECATION, will REMOVE SOON"),
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
			Name:  "listen",
			Usage: "host address and port the worker api will listen on",
			Value: "0.0.0.0:3456",
		},
		&cli.StringFlag{
			Name:   "address",
			Hidden: true,
		},
		&cli.BoolFlag{
			Name:  "no-local-storage",
			Usage: "don't use storageminer repo for sector storage",
		},
		&cli.BoolFlag{
			Name:  "no-swap",
			Usage: "don't use swap",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "addpiece",
			Usage: "enable addpiece",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "precommit1",
			Usage: "enable precommit1 (32G sectors: 1 core, 128GiB Memory)",
			Value: true,
		},
		&cli.BoolFlag{
			Name:  "unseal",
			Usage: "enable unsealing (32G sectors: 1 core, 128GiB Memory)",
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
		&cli.IntFlag{
			Name:  "parallel-fetch-limit",
			Usage: "maximum fetch operations to run in parallel",
			Value: 5,
		},
		&cli.StringFlag{
			Name:  "timeout",
			Usage: "used when 'listen' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function",
			Value: "30m",
		},
		&cli.StringFlag{
			Name:  "sector-size",
			Usage: "worker pprof size",
			Value: "2048",
		},
	},
	Before: func(cctx *cli.Context) error {
		if cctx.IsSet("address") {
			log.Warnf("The '--address' flag is deprecated, it has been replaced by '--listen'")
			if err := cctx.Set("listen", cctx.String("address")); err != nil {
				return err
			}
		}

		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting lotus worker")

		if !cctx.Bool("enable-gpu-proving") {
			if err := os.Setenv("BELLMAN_NO_GPU", "true"); err != nil {
				return xerrors.Errorf("could not set no-gpu env: %+v", err)
			}
		}

		//判断生成哪种 worker
		// 为空 默认
		// Miner miner-worker
		// Docker docker-worker
		workerType := os.Getenv("LOTUS_WORKER_TYPE")

		switch workerType {
		case "REDIS":
			var taskTypes []sealtasks.TaskType

			taskTypes = append(taskTypes, sealtasks.TTFetch, sealtasks.TTFinalize)

			if cctx.Bool("addpiece") {
				taskTypes = append(taskTypes, sealtasks.TTAddPiece)
			}
			if cctx.Bool("precommit1") {
				taskTypes = append(taskTypes, sealtasks.TTPreCommit1)
			}
			if cctx.Bool("unseal") {
				taskTypes = append(taskTypes, sealtasks.TTUnseal)
				taskTypes = append(taskTypes, sealtasks.TTReadUnsealed)
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

			if len(taskTypes) == 0 {
				return xerrors.Errorf("no task types specified")
			}

			sectorSizeInt, err := units.RAMInBytes(cctx.String("sector-size"))
			log.Info("===== input sectorSizeInt:", sectorSizeInt)
			if err != nil {
				return err
			}

			ctx := lcli.ReqContext(cctx)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// Open repo
			repoPath := cctx.String(FlagWorkerRepo)
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
					_, err := lr.Datastore("/metadata")
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

			sb, err := sectorstorage.NewSealer(sectorSizeInt, lr.Path())
			if err != nil {
				return xerrors.Errorf("new sealer err: %w", err)
			}

			err = sb.RegisterWorker(ctx, lr.Path())
			if err != nil {
				return xerrors.Errorf("register worker err: %w", err)
			}
			log.Info("===== register worker success ")

			err = sb.RecoveryTask(ctx)
			if err != nil {
				return xerrors.Errorf("recovery task err: %w", err)
			}
			log.Info("===== recovery task success ")

			sw := &sync.WaitGroup{}
			sw.Add(2)

			go sb.RunCleaner(ctx, sw)
			log.Info("===== run Cleaner success ")

			err = sb.StartWorker(ctx, sw)
			if err != nil {
				return xerrors.Errorf("start worker err: %w", err)
			}
			log.Info("===== start worker success ")
			sw.Wait()

		case "", "MINER":
			// Connect to storage-miner
			ctx := lcli.ReqContext(cctx)

			var nodeApi api.StorageMiner
			var closer func()
			var err error
			for {
				//nodeApi, closer, err = lcli.GetStorageMinerAPI(cctx,
				//	jsonrpc.WithNoReconnect(),
				//	jsonrpc.WithTimeout(300*time.Second),
				//	jsonrpc.WithPingInterval(2*time.Second))
				//if err == nil {
				//	break
				//}
				nodeApi, closer, err = lcli.GetStorageMinerAPI(cctx, lcli.StorageMinerUseHttp)
				if err == nil {
					_, err = nodeApi.Version(ctx)
					if err == nil {
						break
					}
				}
				fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
				time.Sleep(time.Second)
				continue
			}

			defer closer()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			v, err := nodeApi.Version(ctx)
			if err != nil {
				return err
			}
			if v.APIVersion != build.MinerAPIVersion {
				return xerrors.Errorf("lotus-miner API version doesn't match: expected: %s", api.Version{APIVersion: build.MinerAPIVersion})
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

			if cctx.Bool("commit") {
				if err := paramfetch.GetParams(ctx, build.ParametersJSON(), uint64(ssize)); err != nil {
					return xerrors.Errorf("get params: %w", err)
				}
			}

			var taskTypes []sealtasks.TaskType

			taskTypes = append(taskTypes, sealtasks.TTFetch, sealtasks.TTFinalize)

			if cctx.Bool("addpiece") {
				taskTypes = append(taskTypes, sealtasks.TTAddPiece)
			}
			if cctx.Bool("precommit1") {
				taskTypes = append(taskTypes, sealtasks.TTPreCommit1)
			}
			if cctx.Bool("unseal") {
				taskTypes = append(taskTypes, sealtasks.TTUnseal)
				taskTypes = append(taskTypes, sealtasks.TTReadUnsealed)
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

			if len(taskTypes) == 0 {
				return xerrors.Errorf("no task types specified")
			}

			// Open repo

			repoPath := cctx.String(FlagWorkerRepo)
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
					_, err := lr.Datastore("/metadata")
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
			const unspecifiedAddress = "0.0.0.0"
			address := cctx.String("listen")
			addressSlice := strings.Split(address, ":")
			if ip := net.ParseIP(addressSlice[0]); ip != nil {
				if ip.String() == unspecifiedAddress {
					timeout, err := time.ParseDuration(cctx.String("timeout"))
					if err != nil {
						return err
					}
					rip, err := extractRoutableIP(timeout)
					if err != nil {
						return err
					}
					address = rip + ":" + addressSlice[1]
				}
			}

			//sectorSizeInt, err := units.RAMInBytes(cctx.String("sector-size"))
			localStore, err := stores.NewLocal(ctx, lr, nodeApi, []string{"http://" + address + "/remote"}) //abi.SectorSize(sectorSizeInt)
			if err != nil {
				return err
			}

			// Setup remote sector store
			sminfo, err := lcli.GetAPIInfo(cctx, repo.StorageMiner)
			if err != nil {
				return xerrors.Errorf("could not get api info: %w", err)
			}

			remote := stores.NewRemote(localStore, nodeApi, sminfo.AuthHeader(), cctx.Int("parallel-fetch-limit"))

			// Create / expose the worker

			//wsts := statestore.New(namespace.Wrap(ds, modules.WorkerCallsPrefix))

			workerApi := &worker{
				LocalWorker: sectorstorage.NewLocalWorker(sectorstorage.WorkerConfig{
					TaskTypes: taskTypes,
					NoSwap:    cctx.Bool("no-swap"),
				}, remote, localStore, nodeApi), //wsts
				localStore: localStore,
				ls:         lr,
			}
			mux := mux.NewRouter()

			log.Info("Setting up control endpoint at " + address)

			readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
			rpcServer := jsonrpc.NewServer(readerServerOpt)
			rpcServer.Register("Filecoin", apistruct.PermissionedWorkerAPI(workerApi))

			mux.Handle("/rpc/v0", rpcServer)
			mux.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
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

			nl, err := net.Listen("tcp", address)
			if err != nil {
				return err
			}

			{
				a, err := net.ResolveTCPAddr("tcp", address)
				if err != nil {
					return xerrors.Errorf("parsing address: %w", err)
				}

				ma, err := manet.FromNetAddr(a)
				if err != nil {
					return xerrors.Errorf("creating api multiaddress: %w", err)
				}

				if err := lr.SetAPIEndpoint(ma); err != nil {
					return xerrors.Errorf("setting api endpoint: %w", err)
				}

				ainfo, err := lcli.GetAPIInfo(cctx, repo.StorageMiner)
				if err != nil {
					return xerrors.Errorf("could not get miner API info: %w", err)
				}

				// TODO: ideally this would be a token with some permissions dropped
				if err := lr.SetAPIToken(ainfo.Token); err != nil {
					return xerrors.Errorf("setting api token: %w", err)
				}
			}

			log.Info("Waiting for tasks")

			go func() {
				if err := nodeApi.WorkerConnect(ctx, "ws://"+address+"/rpc/v0"); err != nil {
					log.Errorf("Registering worker failed: %+v", err)
					cancel()
					return
				}
				if workerType == "" {
					log.Info("============================ worker 连接 miner=========:", "ws://"+cctx.String("address")+"/rpc/v0")
				} else {
					log.Info("============================ worker_min 连接 miner=========:", "ws://"+cctx.String("address")+"/rpc/v0")
				}
			}()

			return srv.Serve(nl)

		case "DOCKER":
			sectorstorage.GrpcInit()
			return nil
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

		_ = log.Sync()

		// TODO: there are probably cleaner/more graceful ways to restart,
		//  but this is good enough for now (FSM can recover from the mess this creates)
		//nolint:gosec
		if err := syscall.Exec(exe, []string{exe,
			fmt.Sprintf("--worker-repo=%s", cctx.String("worker-repo")),
			fmt.Sprintf("--miner-repo=%s", cctx.String("miner-repo")),
			fmt.Sprintf("--enable-gpu-proving=%t", cctx.Bool("enable-gpu-proving")),
			"run",
			fmt.Sprintf("--listen=%s", cctx.String("listen")),
			fmt.Sprintf("--no-local-storage=%t", cctx.Bool("no-local-storage")),
			fmt.Sprintf("--no-swap=%t", cctx.Bool("no-swap")),
			fmt.Sprintf("--addpiece=%t", cctx.Bool("addpiece")),
			fmt.Sprintf("--precommit1=%t", cctx.Bool("precommit1")),
			fmt.Sprintf("--unseal=%t", cctx.Bool("unseal")),
			fmt.Sprintf("--precommit2=%t", cctx.Bool("precommit2")),
			fmt.Sprintf("--commit=%t", cctx.Bool("commit")),
			fmt.Sprintf("--parallel-fetch-limit=%d", cctx.Int("parallel-fetch-limit")),
			fmt.Sprintf("--timeout=%s", cctx.String("timeout")),
		}, os.Environ()); err != nil {
			fmt.Println(err)
		}
	}()
}

func extractRoutableIP(timeout time.Duration) (string, error) {
	minerMultiAddrKey := "MINER_API_INFO"
	deprecatedMinerMultiAddrKey := "STORAGE_API_INFO"
	env, ok := os.LookupEnv(minerMultiAddrKey)
	if !ok {
		// TODO remove after deprecation period
		_, ok = os.LookupEnv(deprecatedMinerMultiAddrKey)
		if ok {
			log.Warnf("Using a deprecated env(%s) value, please use env(%s) instead.", deprecatedMinerMultiAddrKey, minerMultiAddrKey)
		}
		return "", xerrors.New("MINER_API_INFO environment variable required to extract IP")
	}
	minerAddr := strings.Split(env, "/")
	conn, err := net.DialTimeout("tcp", minerAddr[2]+":"+minerAddr[4], timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck

	localAddr := conn.LocalAddr().(*net.TCPAddr)

	return strings.Split(localAddr.IP.String(), ":")[0], nil
}
