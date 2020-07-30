package main

import (
	"encoding/json"
	"fmt"
	sectorstorage "github.com/filecoin-project/sector-storage"
	"golang.org/x/xerrors"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/sector-storage/storiface"

	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
)

const DefaultSize = 255

var sealingCmd = &cli.Command{
	Name:  "sealing",
	Usage: "interact with sealing pipeline",
	Subcommands: []*cli.Command{
		sealingJobsCmd,
		sealingWorkersCmd,
		sealingSchedDiagCmd,
		setTasksNumberCmd,
		getTasksNumberCmd,
	},
}

var sealingWorkersCmd = &cli.Command{
	Name:  "workers",
	Usage: "list workers",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "color"},
	},
	Action: func(cctx *cli.Context) error {
		color.NoColor = !cctx.Bool("color")

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		stats, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}

		type sortableStat struct {
			id uint64
			storiface.WorkerStats
		}

		st := make([]sortableStat, 0, len(stats))
		for id, stat := range stats {
			st = append(st, sortableStat{id, stat})
		}

		sort.Slice(st, func(i, j int) bool {
			return st[i].id < st[j].id
		})

		for _, stat := range st {
			gpuUse := "not "
			gpuCol := color.FgBlue
			if stat.GpuUsed {
				gpuCol = color.FgGreen
				gpuUse = ""
			}

			fmt.Printf("Worker %d, host %s\n", stat.id, color.MagentaString(stat.Info.Hostname))

			var barCols = uint64(64)
			cpuBars := int(stat.CpuUse * barCols / stat.Info.Resources.CPUs)
			cpuBar := strings.Repeat("|", cpuBars) + strings.Repeat(" ", int(barCols)-cpuBars)

			fmt.Printf("\tCPU:  [%s] %d core(s) in use\n", color.GreenString(cpuBar), stat.CpuUse)

			ramBarsRes := int(stat.Info.Resources.MemReserved * barCols / stat.Info.Resources.MemPhysical)
			ramBarsUsed := int(stat.MemUsedMin * barCols / stat.Info.Resources.MemPhysical)
			ramBar := color.YellowString(strings.Repeat("|", ramBarsRes)) +
				color.GreenString(strings.Repeat("|", ramBarsUsed)) +
				strings.Repeat(" ", int(barCols)-ramBarsUsed-ramBarsRes)

			vmem := stat.Info.Resources.MemPhysical + stat.Info.Resources.MemSwap

			vmemBarsRes := int(stat.Info.Resources.MemReserved * barCols / vmem)
			vmemBarsUsed := int(stat.MemUsedMax * barCols / vmem)
			vmemBar := color.YellowString(strings.Repeat("|", vmemBarsRes)) +
				color.GreenString(strings.Repeat("|", vmemBarsUsed)) +
				strings.Repeat(" ", int(barCols)-vmemBarsUsed-vmemBarsRes)

			fmt.Printf("\tRAM:  [%s] %d%% %s/%s\n", ramBar,
				(stat.Info.Resources.MemReserved+stat.MemUsedMin)*100/stat.Info.Resources.MemPhysical,
				types.SizeStr(types.NewInt(stat.Info.Resources.MemReserved+stat.MemUsedMin)),
				types.SizeStr(types.NewInt(stat.Info.Resources.MemPhysical)))

			fmt.Printf("\tVMEM: [%s] %d%% %s/%s\n", vmemBar,
				(stat.Info.Resources.MemReserved+stat.MemUsedMax)*100/vmem,
				types.SizeStr(types.NewInt(stat.Info.Resources.MemReserved+stat.MemUsedMax)),
				types.SizeStr(types.NewInt(vmem)))

			for _, gpu := range stat.Info.Resources.GPUs {
				fmt.Printf("\tGPU: %s\n", color.New(gpuCol).Sprintf("%s, %sused", gpu, gpuUse))
			}
		}

		return nil
	},
}

var sealingJobsCmd = &cli.Command{
	Name:  "jobs",
	Usage: "list workers",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "color"},
	},
	Action: func(cctx *cli.Context) error {
		color.NoColor = !cctx.Bool("color")

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		jobs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker jobs: %w", err)
		}

		type line struct {
			storiface.WorkerJob
			wid uint64
		}

		lines := make([]line, 0)

		for wid, jobs := range jobs {
			for _, job := range jobs {
				lines = append(lines, line{
					WorkerJob: job,
					wid:       wid,
				})
			}
		}

		// oldest first
		sort.Slice(lines, func(i, j int) bool {
			return lines[i].Start.Before(lines[j].Start)
		})

		workerHostnames := map[uint64]string{}

		wst, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return xerrors.Errorf("getting worker stats: %w", err)
		}

		for wid, st := range wst {
			workerHostnames[wid] = st.Info.Hostname
		}

		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "ID\tSector\tWorker\tHostname\tTask\tTime\n")

		for _, l := range lines {
			_, _ = fmt.Fprintf(tw, "%d\t%d\t%d\t%s\t%s\t%s\n", l.ID, l.Sector.Number, l.wid, workerHostnames[l.wid], l.Task.Short(), time.Now().Sub(l.Start).Truncate(time.Millisecond*100))
		}

		return tw.Flush()
	},
}

var sealingSchedDiagCmd = &cli.Command{
	Name:  "sched-diag",
	Usage: "Dump internal scheduler state",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		st, err := nodeApi.SealingSchedDiag(ctx)
		if err != nil {
			return err
		}

		j, err := json.MarshalIndent(&st, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(j))

		return nil
	},
}

var setTasksNumberCmd = &cli.Command{
	Name:  "set-tasks-number",
	Usage: "Set the number of acceptable tasks,the default value is (255),which means no change",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "hostname",
			Usage: "the hostname of the worker to be queried",
		},
		&cli.UintFlag{
			Name:  "apsize",
			Usage: "the number of addpiece tasks the worker can accept",
			Value: DefaultSize,
		},
		&cli.UintFlag{
			Name:  "p1size",
			Usage: "the number of pre1commit tasks the worker can accept",
			Value: DefaultSize,
		},
		&cli.UintFlag{
			Name:  "p2size",
			Usage: "the number of pre2commit tasks the worker can accept",
			Value: DefaultSize,
		},
		&cli.UintFlag{
			Name:  "c1size",
			Usage: "the number of commit1 tasks the worker can accept",
			Value: DefaultSize,
		},
		&cli.UintFlag{
			Name:  "c2size",
			Usage: "the number of commit2 tasks the worker can accept",
			Value: DefaultSize,
		},
	},
	Action: func(cctx *cli.Context) error {
		hostname := cctx.String("hostname")
		if hostname == "" {
			return xerrors.Errorf("hostname cannot be empty")
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
		err = nodeApi.SetWorkerConf(ctx, hostname, data)

		if err != nil {
			return xerrors.Errorf("set tasks number err: %w", err)
		}
		fmt.Printf("Successful set tasks numbe\n")
		return nil
	},
}

var getTasksNumberCmd = &cli.Command{
	Name:  "get-tasks-number",
	Usage: "get the number of acceptable tasks",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "hostname",
			Usage: "the hostname of the worker to be queried",
		},
	},
	Action: func(cctx *cli.Context) error {
		hostname := cctx.String("hostname")
		if hostname == "" {
			return xerrors.Errorf("hostname cannot be empty")
		}
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		tcl, err := nodeApi.GetWorkerConf(ctx, hostname)
		if err != nil {
			return err
		}
		fmt.Printf("Worker:    【%s】 \nTaskConfig: %+v\nCurrentTaskCount: %+v\n", hostname, tcl[0], tcl[1])

		return nil
	},
}
