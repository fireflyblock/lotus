package main

import (
	"fmt"
	"os/signal"
	"syscall"

	"github.com/filecoin-project/lotus/build"

	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/lib/lotuslog"
	"github.com/filecoin-project/lotus/node/repo"
	sectorstorage "github.com/filecoin-project/sector-storage"
)

var log = logging.Logger("main")

const FlagWorkerRepo = "worker-repo"

// TODO remove after deprecation period
const FlagWorkerRepoDeprecation = "workerrepo"

func main() {
	lotuslog.SetupLogLevels()

	log.Info("Starting lotus worker")

	local := []*cli.Command{
		runCmd,
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
	Usage: "Start docker lotus worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "address",
			Usage: "locally reachable address",
			Value: "0.0.0.0:3456",
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
		&cli.IntFlag{
			Name:  "parallel-fetch-limit",
			Usage: "maximum fetch operations to run in parallel",
			Value: 5,
		},
		&cli.StringFlag{
			Name:  "timeout",
			Usage: "used when address is unspecified. must be a valid duration recognized by golang's time.ParseDuration function",
			Value: "30m",
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("enable-gpu-proving") {
			if err := os.Setenv("BELLMAN_NO_GPU", "true"); err != nil {
				return xerrors.Errorf("could not set no-gpu env: %+v", err)
			}
		}

		sectorstorage.GrpcInit()

		sig := make(chan os.Signal, 2)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig

		return nil
	},
}
