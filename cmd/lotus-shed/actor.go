package main

import (
	"fmt"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/urfave/cli/v2"
)

var actorCmd = &cli.Command{
	Name:  "actor",
	Usage: "actor",
	Flags: []cli.Flag{},
	Subcommands: []*cli.Command{
		compactCmd,
	},
}

var compactCmd = &cli.Command{
	Name:      "compact",
	Usage:     "compact",
	ArgsUsage: "",
	Flags: []cli.Flag{
		&cli.Uint64Flag{
			Name:  "begin",
			Usage: "begin",
			Value: 0,
		},
		&cli.Uint64Flag{
			Name:  "end",
			Usage: "end",
			Value: 0,
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		api, acloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer acloser()

		ctx := lcli.ReqContext(cctx)

		maddr, err := nodeApi.ActorAddress(ctx)
		if err != nil {
			return err
		}

		mi, err := api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
		if err != nil {
			return err
		}

		current := bitfield.New()
		begin := cctx.Uint64("begin")
		end := cctx.Uint64("end")
		for ; begin < end; begin += 1 {
			current.Set(begin)
		}
		params, err := actors.SerializeParams(&miner0.CompactSectorNumbersParams{
			MaskSectorNumbers: current,
		})
		if err != nil {
			fmt.Println("failed to serialize param", err)
			return err
		}

		smsg, err := api.MpoolPushMessage(ctx, &types.Message{
			To:     maddr,
			From:   mi.Owner,
			Value:  types.NewInt(0),
			Method: builtin.MethodsMiner.CompactSectorNumbers,
			Params: params,
		}, nil)
		if err != nil {
			return err
		}

		fmt.Println("wait msg:", smsg, smsg.Cid())
		wait, err := api.StateWaitMsg(ctx, smsg.Cid(), build.MessageConfidence)
		if err != nil {
			return err
		}

		// check it executed successfully
		if wait.Receipt.ExitCode != 0 {
			return err
		}
		return nil
	},
}
