package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/apibstore"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"path/filepath"
)

var sectorsstorageCmd = &cli.Command{
	Name:  "sector-storage",
	Usage: "Tools for interacting with sectors storage declear",
	Flags: []cli.Flag{},
	Subcommands: []*cli.Command{
		faultySectorsCmd,
		fixSectorDeclCmd,
	},
}

var faultySectorsCmd = &cli.Command{
	Name:  "faulty-sectors",
	Usage: "Tools for check all windowpost fault sectors,display storage infos",
	Action: func(cctx *cli.Context) error {
		faultSectors := getFaults(cctx)
		if len(faultSectors) == 0 {
			fmt.Println("fault Sectors is 0")
			return nil
		}
		ssps := getSectorsStorageInfo(cctx, faultSectors)
		if len(ssps) == 0 {
			fmt.Printf("faultSectors count %d, getSectorStorage count %d", len(faultSectors), len(ssps))
			return nil
		}

		// select right store path, drop fault store path
		for _, ssp := range ssps {
			// 检查cache路径中的文件即可
			fmt.Printf("check sector (%+v)\n", ssp.sid)
			for storeID, sp := range ssp.storePaths {

				sectorCachePath := filepath.Join(sp.localPath, storiface.FTCache.String(), fmt.Sprintf("s-t0%s-%d", ssp.sid.Miner.String(), ssp.sid.Number))
				if !sectorCachePathExist(ssp.sid, sp.localPath) {
					fmt.Printf("path(%s) storeID(%s) is not ok\n", sectorCachePath, storeID)
				} else {
					//fmt.Printf("path(%s) storeID(%s) is ok\n",sectorCachePath,storeID)
				}
			}
			fmt.Println("------")
		}
		return nil
	},
}

type storedSector struct {
	id        stores.ID
	store     stores.SectorStorageInfo
	localPath string

	unsealed, sealed, cache bool
}

type sectorStorePaths struct {
	sid        abi.SectorID
	storePaths map[stores.ID]storedSector
}

var fixSectorDeclCmd = &cli.Command{
	Name:  "fix-sectors-dec",
	Usage: "Tools for fix sectors declear",
	Action: func(cctx *cli.Context) error {
		faultSectors := getFaults(cctx)
		if len(faultSectors) == 0 {
			fmt.Println("fault Sectors is 0")
			return nil
		}
		ssps := getSectorsStorageInfo(cctx, faultSectors)
		if len(ssps) == 0 {
			fmt.Printf("faultSectors count %d, getSectorStorage count %d", len(faultSectors), len(ssps))
			return nil
		}

		fixSectorDeclear(cctx, ssps)
		//// select right store path, drop fault store path
		//for _, ssp := range ssps {
		//	// 检查cache路径中的文件即可
		//	fmt.Printf("check sector (%+v)\n",ssp.sid)
		//	for storeID,sp:=range  ssp.storePaths{

		//		sectorCachePath := filepath.Join(sp.localPath, storiface.FTCache.String(), fmt.Sprintf("s-t0%s-%d", ssp.sid.Miner.String(), ssp.sid.Number))
		//		if !sectorCachePathExist(ssp.sid,sp.localPath){
		//			fmt.Printf("path(%s) storeID(%s) is not ok\n",sectorCachePath,storeID)
		//		}else{
		//			//fmt.Printf("path(%s) storeID(%s) is ok\n",sectorCachePath,storeID)
		//		}
		//	}
		//	fmt.Println("------")
		//}
		return nil
	},
}

func fixSectorDeclear(cctx *cli.Context, ssps []*sectorStorePaths) {

	ctx := lcli.ReqContext(cctx)
	nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		return
	}
	defer closer()

	// select right store path, drop fault store path
	for _, ssp := range ssps {
		// 检查cache路径中的文件即可
		for storeID, sp := range ssp.storePaths {
			if !sectorCachePathExist(ssp.sid, sp.localPath) {
				sectorCachePath := filepath.Join(sp.localPath, storiface.FTCache.String(), fmt.Sprintf("s-t0%s-%d", ssp.sid.Miner.String(), ssp.sid.Number))
				fmt.Printf("path(%s) storeID(%s) is not ok\n", sectorCachePath, storeID)
				nodeApi.StorageDropSector(ctx, storeID, ssp.sid, storiface.FTSealed|storiface.FTCache)
			}
		}
	}
}

func sectorCachePathExist(sid abi.SectorID, localPath string) bool {
	sectorCachePath := filepath.Join(localPath, storiface.FTCache.String(), fmt.Sprintf("s-t0%s-%d", sid.Miner.String(), sid.Number))
	fileInfos, err := ioutil.ReadDir(sectorCachePath)
	if err != nil {
		fmt.Printf("open sector(%+v) store path [%s] error :%+v\n", sid, sectorCachePath, err)
		return false
	}

	//fmt.Printf("path(%s) subfile count is %d\n",sectorCachePath,len(fileInfos))
	if len(fileInfos) < 10 {
		//fmt.Printf("sector(%+v) store path [%s] subfiles is not right :%+v\n", sid, sectorCachePath, err)
		return false
	}
	return true
}

func getSectorsStorageInfo(cctx *cli.Context, sectors []uint64) []*sectorStorePaths {
	ctx := lcli.ReqContext(cctx)
	nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		return []*sectorStorePaths{}
	}
	defer closer()

	ma, err := nodeApi.ActorAddress(ctx)
	if err != nil {
		return []*sectorStorePaths{}
	}

	mid, err := address.IDFromAddress(ma)
	if err != nil {
		return []*sectorStorePaths{}
	}

	local, err := nodeApi.StorageLocal(ctx)
	if err != nil {
		return []*sectorStorePaths{}
	}

	var ssps []*sectorStorePaths
	for _, snum := range sectors {
		sid := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: abi.SectorNumber(snum),
		}

		ctx := lcli.ReqContext(cctx)
		c, err := nodeApi.StorageFindSector(ctx, sid, storiface.FTSealed, 0, false)
		if err != nil {
			fmt.Errorf("finding sector(%+v) cache: %w", sid, err)
			continue
		}

		//ssp:=map[stores.ID]sectorStorePaths{}
		ssp := sectorStorePaths{sid: sid, storePaths: map[stores.ID]storedSector{}}
		for _, info := range c {
			_, ok := ssp.storePaths[info.ID]
			if !ok {
				sts := storedSector{
					id:        info.ID,
					store:     info,
					localPath: local[info.ID],
				}
				ssp.storePaths[info.ID] = sts
			}
			//ssp.storePaths[info.ID].cache = true
		}
		ssps = append(ssps, &ssp)
	}
	return ssps
}

func getFaults(cctx *cli.Context) []uint64 {
	nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
	if err != nil {
		return []uint64{}
	}
	defer closer()

	api, acloser, err := lcli.GetFullNodeAPI(cctx)
	if err != nil {
		return []uint64{}
	}
	defer acloser()

	ctx := lcli.ReqContext(cctx)

	stor := store.ActorStore(ctx, apibstore.NewAPIBlockstore(api))

	maddr, err := getActorAddress(ctx, nodeApi, cctx.String("actor"))
	if err != nil {
		return []uint64{}
	}

	mact, err := api.StateGetActor(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return []uint64{}
	}

	mas, err := miner.Load(stor, mact)
	if err != nil {
		return []uint64{}
	}

	var faultSectors []uint64
	err = mas.ForEachDeadline(func(dlIdx uint64, dl miner.Deadline) error {
		return dl.ForEachPartition(func(partIdx uint64, part miner.Partition) error {
			faults, err := part.FaultySectors()
			if err != nil {
				return err
			}
			return faults.ForEach(func(num uint64) error {
				faultSectors = append(faultSectors, num)
				return nil
			})
		})
	})
	if err != nil {
		return []uint64{}
	}
	return faultSectors
}

func getActorAddress(ctx context.Context, nodeAPI api.StorageMiner, overrideMaddr string) (maddr address.Address, err error) {
	if overrideMaddr != "" {
		maddr, err = address.NewFromString(overrideMaddr)
		if err != nil {
			return maddr, err
		}
		return
	}

	maddr, err = nodeAPI.ActorAddress(ctx)
	if err != nil {
		return maddr, xerrors.Errorf("getting actor address: %w", err)
	}

	return maddr, nil
}
