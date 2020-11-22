package main

import (
	//"encoding/hex"
	"encoding/json"
	"fmt"
	sectorstorage "github.com/filecoin-project/sector-storage"
	"golang.org/x/xerrors"
	//"os"
	"sort"
	//"strings"
	//"text/tabwriter"
	//"time"

	"github.com/fatih/color"
	//"github.com/google/uuid"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/sector-storage/storiface"

	lcli "github.com/filecoin-project/lotus/cli"
)

const DefaultSize = 255

var sealingCmd = &cli.Command{
	Name:  "sealing",
	Usage: "interact with sealing pipeline",
	Subcommands: []*cli.Command{
		//sealingJobsCmd,
		sealingWorkersCmd,
		sealingSchedDiagCmd,
		//sealingAbortCmd,
		setTasksNumberCmd,
		getTasksNumberCmd,
		dealTransCountCmd,
		deleteTaskConfigCmd,
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
			//id uuid.UUID
			storiface.WorkerStats
		}

		st := make([]sortableStat, 0, len(stats))
		for id, stat := range stats {
			st = append(st, sortableStat{id, stat})
		}

		//sort.Slice(st, func(i, j int) bool {
		//	return st[i].id.String() < st[j].id.String()
		//})
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

			//var disabled string
			//if !stat.Enabled {
			//	disabled = color.RedString(" (disabled)")
			//}

			//fmt.Printf("Worker %s, host %s%s\n", stat.id, color.MagentaString(stat.Info.Hostname), disabled)
			fmt.Printf("Worker %s, host %s\n", stat.id, color.MagentaString(stat.Info.Hostname))

			//var barCols = uint64(64)
			//cpuBars := int(stat.CpuUse * barCols / stat.Info.Resources.CPUs)
			//cpuBar := strings.Repeat("|", cpuBars) + strings.Repeat(" ", int(barCols)-cpuBars)

			//fmt.Printf("\tCPU:  [%s] %d/%d core(s) in use\n",
			//	color.GreenString(cpuBar), stat.CpuUse, stat.Info.Resources.CPUs)

			//ramBarsRes := int(stat.Info.Resources.MemReserved * barCols / stat.Info.Resources.MemPhysical)
			//ramBarsUsed := int(stat.MemUsedMin * barCols / stat.Info.Resources.MemPhysical)
			//ramBar := color.YellowString(strings.Repeat("|", ramBarsRes)) +
			//	color.GreenString(strings.Repeat("|", ramBarsUsed)) +
			//	strings.Repeat(" ", int(barCols)-ramBarsUsed-ramBarsRes)

			//vmem := stat.Info.Resources.MemPhysical + stat.Info.Resources.MemSwap

			//vmemBarsRes := int(stat.Info.Resources.MemReserved * barCols / vmem)
			//vmemBarsUsed := int(stat.MemUsedMax * barCols / vmem)
			//vmemBar := color.YellowString(strings.Repeat("|", vmemBarsRes)) +
			//	color.GreenString(strings.Repeat("|", vmemBarsUsed)) +
			//	strings.Repeat(" ", int(barCols)-vmemBarsUsed-vmemBarsRes)

			//fmt.Printf("\tRAM:  [%s] %d%% %s/%s\n", ramBar,
			//	(stat.Info.Resources.MemReserved+stat.MemUsedMin)*100/stat.Info.Resources.MemPhysical,
			//	types.SizeStr(types.NewInt(stat.Info.Resources.MemReserved+stat.MemUsedMin)),
			//	types.SizeStr(types.NewInt(stat.Info.Resources.MemPhysical)))

			//fmt.Printf("\tVMEM: [%s] %d%% %s/%s\n", vmemBar,
			//	(stat.Info.Resources.MemReserved+stat.MemUsedMax)*100/vmem,
			//	types.SizeStr(types.NewInt(stat.Info.Resources.MemReserved+stat.MemUsedMax)),
			//	types.SizeStr(types.NewInt(vmem)))

			for _, gpu := range stat.Info.Resources.GPUs {
				fmt.Printf("\tGPU: %s\n", color.New(gpuCol).Sprintf("%s, %sused", gpu, gpuUse))
			}
		}

		return nil
	},
}

//var sealingJobsCmd = &cli.Command{
//	Name:  "jobs",
//	Usage: "list running jobs",
//	Flags: []cli.Flag{
//		&cli.BoolFlag{Name: "color"},
//		&cli.BoolFlag{
//			Name:  "show-ret-done",
//			Usage: "show returned but not consumed calls",
//		},
//	},
//	Action: func(cctx *cli.Context) error {
//		color.NoColor = !cctx.Bool("color")
//
//		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
//		if err != nil {
//			return err
//		}
//		defer closer()
//
//		ctx := lcli.ReqContext(cctx)
//
//		jobs, err := nodeApi.WorkerJobs(ctx)
//		if err != nil {
//			return xerrors.Errorf("getting worker jobs: %w", err)
//		}
//
//		type line struct {
//			storiface.WorkerJob
//			wid uint64
//			//wid uuid.UUID
//		}
//
//		lines := make([]line, 0)
//
//		for wid, jobs := range jobs {
//			for _, job := range jobs {
//				lines = append(lines, line{
//					WorkerJob: job,
//					wid:       wid,
//				})
//			}
//		}
//
//		// oldest first
//		sort.Slice(lines, func(i, j int) bool {
//			if lines[i].RunWait != lines[j].RunWait {
//				return lines[i].RunWait < lines[j].RunWait
//			}
//			if lines[i].Start.Equal(lines[j].Start) {
//				return lines[i].ID.ID.String() < lines[j].ID.ID.String()
//			}
//			return lines[i].Start.Before(lines[j].Start)
//		})
//
//		workerHostnames := map[uint64]string{}
//		//workerHostnames := map[uuid.UUID]string{}
//
//		wst, err := nodeApi.WorkerStats(ctx)
//		if err != nil {
//			return xerrors.Errorf("getting worker stats: %w", err)
//		}
//
//		for wid, st := range wst {
//			workerHostnames[wid] = st.Info.Hostname
//		}
//
//		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
//		_, _ = fmt.Fprintf(tw, "ID\tSector\tWorker\tHostname\tTask\tState\tTime\n")
//
//		for _, l := range lines {
//			state := "running"
//			switch {
//			case l.RunWait > 0:
//				state = fmt.Sprintf("assigned(%d)", l.RunWait-1)
//			case l.RunWait == storiface.RWRetDone:
//				if !cctx.Bool("show-ret-done") {
//					continue
//				}
//				state = "ret-done"
//			case l.RunWait == storiface.RWReturned:
//				state = "returned"
//			case l.RunWait == storiface.RWRetWait:
//				state = "ret-wait"
//			}
//			dur := "n/a"
//			if !l.Start.IsZero() {
//				dur = time.Now().Sub(l.Start).Truncate(time.Millisecond * 100).String()
//			}
//
//			hostname, ok := workerHostnames[l.wid]
//			if !ok {
//				hostname = l.Hostname
//			}
//
//			_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
//				hex.EncodeToString(l.ID.ID[:4]),
//				l.sector.ID.Number,
//				hex.EncodeToString(l.wid[:4]),
//				hostname,
//				l.Task.Short(),
//				state,
//				dur)
//		}
//
//		return tw.Flush()
//	},
//}

var sealingSchedDiagCmd = &cli.Command{
	Name:  "sched-diag",
	Usage: "Dump internal scheduler state",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name: "force-sched",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		st, err := nodeApi.SealingSchedDiag(ctx, cctx.Bool("force-sched"))
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

//var sealingAbortCmd = &cli.Command{
//	Name:      "abort",
//	Usage:     "Abort a running job",
//	ArgsUsage: "[callid]",
//	Action: func(cctx *cli.Context) error {
//		if cctx.Args().Len() != 1 {
//			return xerrors.Errorf("expected 1 argument")
//		}
//
//		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
//		if err != nil {
//			return err
//		}
//		defer closer()
//
//		ctx := lcli.ReqContext(cctx)
//
//		jobs, err := nodeApi.WorkerJobs(ctx)
//		if err != nil {
//			return xerrors.Errorf("getting worker jobs: %w", err)
//		}
//
//		var job *storiface.WorkerJob
//	outer:
//		for _, workerJobs := range jobs {
//			for _, j := range workerJobs {
//				if strings.HasPrefix(j.ID.ID.String(), cctx.Args().First()) {
//					j := j
//					job = &j
//					break outer
//				}
//			}
//		}
//
//		if job == nil {
//			return xerrors.Errorf("job with specified id prefix not found")
//		}
//
//		fmt.Printf("aborting job %s, task %s, sector %d, running on host %s\n", job.ID.String(), job.Task.Short(), job.sector.ID.Number, job.Hostname)
//
//		return nodeApi.SealingAbort(ctx, job.ID)
//	},
//}

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
		&cli.Uint64Flag{
			Name:  "c2size",
			Usage: "the number of commit2 tasks the worker can accept",
			Value: 20070920,
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

		c2Size := cctx.Uint64("c2size")
		tc.Commit2 = c2Size

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

		tcl, _ := nodeApi.GetWorkerConf(ctx, hostname)
		var tcf, tct string
		if tcl[2].AddPieceSize == 255 {
			tcf = fmt.Sprintf("TaskConfig: No taskConfig found.\n")
		} else {
			var tc = sectorstorage.TaskConfig{}
			tc.AddPieceSize = tcl[0].AddPieceSize
			tc.Pre1CommitSize = tcl[0].Pre1CommitSize
			tc.Pre2CommitSize = tcl[0].Pre2CommitSize
			tc.Commit1 = tcl[0].Commit1
			tc.Commit2 = tcl[0].Commit2
			tcf = fmt.Sprintf("TaskConfig: %+v\n", tc)
		}
		if tcl[3].AddPieceSize == 255 {
			tct = fmt.Sprintf("CurrentTaskCount: No current taskCount.\n")
		} else {
			tct = fmt.Sprintf("CurrentTaskCount: %+v\n", tcl[1])
		}
		fmt.Printf("Worker:    【%s】 \n"+tcf+tct, hostname)

		return nil
	},
}

var dealTransCountCmd = &cli.Command{
	Name:  "trans_count",
	Usage: "deal the number of trans",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "size",
			Usage: "the number of the trans the miner can accept",
			Value: 20070920,
		},
	},
	Action: func(cctx *cli.Context) error {
		size := cctx.Int("size")

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		res, err := nodeApi.DealTransCount(ctx, size)

		if err != nil {
			return xerrors.Errorf("deal trans number err: %w", err)
		}

		switch size {
		case 20070920:
			fmt.Println("============================")
			fmt.Printf("trans-count:%d\n", res)
			fmt.Println("============================")
		default:
			fmt.Printf("\nSet successfully !\nTrans-Count:%d\n", res)
		}
		return nil
	},
}

var deleteTaskConfigCmd = &cli.Command{
	Name:  "delete-task-config",
	Usage: "delete taskconfig",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "hostname",
			Usage: "hostname of target worker",
		},
	},
	Action: func(cctx *cli.Context) error {
		hn := cctx.String("hostname")

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		res, err := nodeApi.DeleteTaskCount(ctx, hn)

		if err != nil {
			return xerrors.Errorf("deal trans number err: %w", err)
		}

		switch res {
		case true:
			fmt.Printf("successfully deleted\n")
		case false:
			fmt.Printf("target does not exist\n")
		}
		return nil
	},
}
