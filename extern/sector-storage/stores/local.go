package stores

import (
	"context"
	"encoding/json"
	"github.com/filecoin-project/sector-storage/storiface"
	"github.com/filecoin-project/sector-storage/transfordata"
	"io/ioutil"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/sector-storage/fsutil"
)

type ByteSize int64

const (
	B  ByteSize = 1 << (10 * iota) // 1<<(10*0)
	KB                             // 1<<(10*1)
	MB                             // 1<<(10*2)
	GB                             // 1<<(10*3)
	TB                             // 1<<(10*4)
	PB                             //  1<<(10*5)
)

type StoragePath struct {
	ID     ID
	Weight uint64

	LocalPath string

	CanSeal  bool
	CanStore bool
}

// LocalStorageMeta [path]/sectorstore.json
type LocalStorageMeta struct {
	ID ID

	// A high weight means data is more likely to be stored in this path
	Weight uint64 // 0 = readonly

	// Intermediate data for the sealing process will be stored here
	CanSeal bool

	// Finalized sectors that will be proved over time will be stored here
	CanStore bool
}

// StorageConfig .lotusstorage/storage.json
type StorageConfig struct {
	StoragePaths []LocalPath
}

type LocalPath struct {
	Path string
}

type LocalStorage interface {
	GetStorage() (StorageConfig, error)
	SetStorage(func(*StorageConfig)) error

	Stat(path string) (fsutil.FsStat, error)

	// returns real disk usage for a file/directory
	// os.ErrNotExit when file doesn't exist
	DiskUsage(path string) (int64, error)
}

const MetaFile = "sectorstore.json"

type Local struct {
	localStorage LocalStorage
	index        SectorIndex
	urls         []string

	paths map[ID]*path
	size  abi.SectorSize

	localLk sync.RWMutex
}

type path struct {
	local string // absolute local path

	reserved     int64
	reservations map[abi.SectorID]storiface.SectorFileType
}

func (p *path) stat(ls LocalStorage) (fsutil.FsStat, error) {
	stat, err := ls.Stat(p.local)
	if err != nil {
		return fsutil.FsStat{}, xerrors.Errorf("stat %s: %w", p.local, err)
	}

	stat.Reserved = p.reserved

	for id, ft := range p.reservations {
		for _, fileType := range storiface.PathTypes {
			if fileType&ft == 0 {
				continue
			}

			sp := p.sectorPath(id, fileType)

			used, err := ls.DiskUsage(sp)
			if err == os.ErrNotExist {
				p, ferr := tempFetchDest(sp, false)
				if ferr != nil {
					return fsutil.FsStat{}, ferr
				}

				used, err = ls.DiskUsage(p)
			}
			if err != nil {
				log.Errorf("getting disk usage of '%s': %+v", p.sectorPath(id, fileType), err)
				continue
			}

			stat.Reserved -= used
		}
	}

	if stat.Reserved < 0 {
		log.Warnf("negative reserved storage: p.reserved=%d, reserved: %d", p.reserved, stat.Reserved)
		stat.Reserved = 0
	}

	stat.Available -= stat.Reserved
	if stat.Available < 0 {
		stat.Available = 0
	}

	return stat, err
}

func (p *path) sectorPath(sid abi.SectorID, fileType storiface.SectorFileType) string {
	return filepath.Join(p.local, fileType.String(), storiface.SectorName(sid))
}

func NewLocal(ctx context.Context, ls LocalStorage, index SectorIndex, urls []string, size abi.SectorSize) (*Local, error) {
	l := &Local{
		localStorage: ls,
		index:        index,
		urls:         urls,

		paths: map[ID]*path{},

		size: size,
	}
	return l, l.open(ctx)
}

func (st *Local) OpenPath(ctx context.Context, p string) error {
	st.localLk.Lock()
	defer st.localLk.Unlock()

	mb, err := ioutil.ReadFile(filepath.Join(p, MetaFile))
	if err != nil {
		return xerrors.Errorf("reading storage metadata for %s: %w", p, err)
	}

	var meta LocalStorageMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return xerrors.Errorf("unmarshalling storage metadata for %s: %w", p, err)
	}

	// TODO: Check existing / dedupe

	out := &path{
		local: p,

		reserved:     0,
		reservations: map[abi.SectorID]storiface.SectorFileType{},
	}

	fst, err := out.stat(st.localStorage)
	if err != nil {
		return err
	}

	err = st.index.StorageAttach(ctx, StorageInfo{
		ID:       meta.ID,
		URLs:     st.urls,
		Weight:   meta.Weight,
		CanSeal:  meta.CanSeal,
		CanStore: meta.CanStore,
	}, fst)
	if err != nil {
		return xerrors.Errorf("declaring storage in index: %w", err)
	}

	for _, t := range storiface.PathTypes {
		ents, err := ioutil.ReadDir(filepath.Join(p, t.String()))
		if err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(filepath.Join(p, t.String()), 0755); err != nil { // nolint
					return xerrors.Errorf("openPath mkdir '%s': %w", filepath.Join(p, t.String()), err)
				}

				continue
			}
			return xerrors.Errorf("listing %s: %w", filepath.Join(p, t.String()), err)
		}

		for _, ent := range ents {

			// 只处理存储机器上面的数据，
			//if meta.CanStore && !meta.CanSeal {
			//	// 清理不完整的sector数据，暂时之考虑chache目录和sealed目录
			//	if t == storiface.FTSealed || t == storiface.FTCache {
			//		if !st.checkSectorFine(ent, p, t) {
			//			continue
			//		}
			//	}
			//}

			if ent.Name() == FetchTempSubdir {
				continue
			}

			sid, err := storiface.ParseSectorID(ent.Name())
			if err != nil {
				log.Error(xerrors.Errorf("parse sector id %s: %w", ent.Name(), err))
				continue
				//return xerrors.Errorf("parse sector id %s: %w", ent.Name(), err)
			}

			if err := st.index.StorageDeclareSector(ctx, meta.ID, sid, t, meta.CanStore); err != nil {
				return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", sid, t, meta.ID, err)
			}

		}
	}

	st.paths[meta.ID] = out

	return nil
}

// 解决bug（192.168.20.201/filecoin-project/teamwork/-/issues/109)
// 问题描述参考bug描述
// 办法：
// 1 在检测sealed文件的时候，判断sealed文件是否为32GB，是则认为sector正确，否则认为错误，并且删除
// 2 在检测cache文件的时候，判断是否存在8个tree_r_last以及两个aux文件，如果存在则认为正确，否则认为不正确，并且删除掉
func (st *Local) checkSectorFine(ent os.FileInfo, p string, t storiface.SectorFileType) bool {
	//  32  << 30  --> 32  GB
	//  512 << 20  --> 512 MB
	//  2   << 10  --> 2   KB
	//log.Info("original size ------- ", st.size)
	sectorSize := int64(st.size)
	//log.Infof("try to check %s, size %d",p+t.String(),ent.Size())

	// check sealed
	if t == storiface.FTSealed {
		if ent.Size() == sectorSize {
			return st.checkCacheFiles(ent, p, t)
		} else {
			//
			p := filepath.Join(p, storiface.FTSealed.String(), ent.Name())
			log.Errorf("try to delete error sector %s, file size %d,need size %d", filepath.Join(p, t.String(), ent.Name()), ent.Size(), sectorSize)
			//err := os.Remove(p)
			//if err != nil {
			//	log.Errorf("delete %s error :%+v", p+FTSealed.String()+ent.Name(), err)
			//}
			return false
		}
	}

	// check cache
	if t == storiface.FTCache {
		return st.checkCacheFiles(ent, p, t)
	}

	return false
}

func (st *Local) checkCacheFiles(ent os.FileInfo, p string, t storiface.SectorFileType) bool {
	// 判断文件数量
	var fileNums int
	switch st.size {
	case 2 << 10:
		fileNums = 3
	case 512 << 20:
		fileNums = 3
	case 32 << 30:
		fileNums = 10
	}

	// check cache
	path := filepath.Join(p, storiface.FTCache.String(), ent.Name())
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Errorf("read file %s error, err: %s", path, err.Error())
		return false
	}

	if len(files) != fileNums {
		// 删除对应的cache和sealed文件
		log.Errorf("check sectorSize %d file %s cache files count %d should be %d, remove cache dir and sealed file!!!!!", st.size, path, len(files), fileNums)
		//err = os.RemoveAll(path)
		//if err != nil {
		//	log.Errorf("remove %s failed, %s", path, err.Error())
		//} else {
		//	log.Warnf("Remove %s  success", path)
		//}

		_ = filepath.Join(p, storiface.FTSealed.String(), ent.Name())
		//err := os.Remove(sealPath)
		//if err != nil {
		//	log.Errorf("remove %s failed, %s", sealPath, err.Error())
		//} else {
		//	log.Warnf("Remove sealed file %s success", ent.Name())
		//}
		return false
	}
	return true
}

func (st *Local) open(ctx context.Context) error {
	cfg, err := st.localStorage.GetStorage()
	if err != nil {
		return xerrors.Errorf("getting local storage config: %w", err)
	}

	for _, path := range cfg.StoragePaths {
		err := st.OpenPath(ctx, path.Path)
		if err != nil {
			return xerrors.Errorf("opening path %s: %w", path.Path, err)
		}
	}

	go st.reportHealth(ctx)

	return nil
}

func (st *Local) reportHealth(ctx context.Context) {
	// randomize interval by ~10%
	interval := (HeartbeatInterval*100_000 + time.Duration(rand.Int63n(10_000))) / 100_000

	for {
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}

		st.reportStorage(ctx)
	}
}

func (st *Local) reportStorage(ctx context.Context) {
	st.localLk.RLock()

	toReport := map[ID]HealthReport{}
	for id, p := range st.paths {
		stat, err := p.stat(st.localStorage)
		r := HealthReport{Stat: stat}
		if err != nil {
			r.Err = err.Error()
		}

		toReport[id] = r
	}

	st.localLk.RUnlock()

	for id, report := range toReport {
		if err := st.index.StorageReportHealth(ctx, id, report); err != nil {
			log.Warnf("error reporting storage health for %s (%+v): %+v", id, report, err)
		}
	}
}

func (st *Local) Reserve(ctx context.Context, sid abi.SectorID, ssize abi.SectorSize, ft storiface.SectorFileType, storageIDs storiface.SectorPaths, overheadTab map[storiface.SectorFileType]int) (func(), error) {
	//ssize, err := spt.SectorSize()
	//if err != nil {
	//	return nil, xerrors.Errorf("getting sector size: %w", err)
	//}

	st.localLk.Lock()

	done := func() {}
	deferredDone := func() { done() }
	defer func() {
		st.localLk.Unlock()
		deferredDone()
	}()

	for _, fileType := range storiface.PathTypes {
		if fileType&ft == 0 {
			continue
		}

		id := ID(storiface.PathByType(storageIDs, fileType))

		p, ok := st.paths[id]
		if !ok {
			return nil, errPathNotFound
		}

		stat, err := p.stat(st.localStorage)
		if err != nil {
			return nil, xerrors.Errorf("getting local storage stat: %w", err)
		}

		overhead := int64(overheadTab[fileType]) * int64(ssize) / storiface.FSOverheadDen

		if stat.Available < overhead {
			return nil, xerrors.Errorf("can't reserve %d bytes in '%s' (id:%s), only %d available", overhead, p.local, id, stat.Available)
		}

		p.reserved += overhead

		prevDone := done
		done = func() {
			prevDone()

			st.localLk.Lock()
			defer st.localLk.Unlock()

			p.reserved -= overhead
		}
	}

	deferredDone = func() {}
	return done, nil
}

func (st *Local) AcquireSector(ctx context.Context, sid abi.SectorID, ssize abi.SectorSize, existing storiface.SectorFileType, allocate storiface.SectorFileType, pathType storiface.PathType, op storiface.AcquireMode) (storiface.SectorPaths, storiface.SectorPaths, error) {
	if existing|allocate != existing^allocate {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.New("can't both find and allocate a sector")
	}

	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out storiface.SectorPaths
	var storageIDs storiface.SectorPaths

	for _, fileType := range storiface.PathTypes {
		if fileType&existing == 0 {
			continue
		}

		si, err := st.index.StorageFindSector(ctx, sid, fileType, ssize, false)
		if err != nil {
			log.Warnf("finding existing sector %d(t:%d) failed: %+v", sid, fileType, err)
			continue
		}

		for _, info := range si {
			p, ok := st.paths[info.ID]
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				continue
			}

			spath := p.sectorPath(sid, fileType)
			storiface.SetPathByType(&out, fileType, spath)
			storiface.SetPathByType(&storageIDs, fileType, string(info.ID))

			existing ^= fileType
			break
		}
	}

	for _, fileType := range storiface.PathTypes {
		if fileType&allocate == 0 {
			continue
		}

		sis, err := st.index.StorageBestAlloc(ctx, fileType, ssize, pathType)
		if err != nil {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("finding best storage for allocating : %w", err)
		}

		var best string
		var bestID ID

		for _, si := range sis {
			p, ok := st.paths[si.ID]
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				continue
			}

			if (pathType == storiface.PathSealing) && !si.CanSeal {
				continue
			}

			if (pathType == storiface.PathStorage) && !si.CanStore {
				continue
			}

			// TODO: Check free space

			best = p.sectorPath(sid, fileType)
			bestID = si.ID
			break
		}

		if best == "" {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("couldn't find a suitable path for a sector")
		}

		storiface.SetPathByType(&out, fileType, best)
		storiface.SetPathByType(&storageIDs, fileType, string(bestID))
		allocate ^= fileType
	}

	return out, storageIDs, nil
}

func (st *Local) Local(ctx context.Context) ([]StoragePath, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out []StoragePath
	for id, p := range st.paths {
		if p.local == "" {
			continue
		}

		si, err := st.index.StorageInfo(ctx, id)
		if err != nil {
			return nil, xerrors.Errorf("get storage info for %s: %w", id, err)
		}

		out = append(out, StoragePath{
			ID:        id,
			Weight:    si.Weight,
			LocalPath: p.local,
			CanSeal:   si.CanSeal,
			CanStore:  si.CanStore,
		})
	}

	return out, nil
}

func (st *Local) getLayersAndTreeCAndTreeDFiles(url string, proofType abi.RegisteredSealProof) []string {
	layerLabel := "/sc-02-data-layer-"
	treeCLabel := "/sc-02-data-tree-c"
	treeD := "/sc-02-data-tree-d.dat"
	tailLabel := ".dat"
	var files []string

	if proofType == abi.RegisteredSealProof_StackedDrg2KiBV1 || proofType == abi.RegisteredSealProof_StackedDrg512MiBV1 {
		files = append(files, url+layerLabel+"1"+tailLabel)
		files = append(files, url+layerLabel+"2"+tailLabel)
		files = append(files, url+treeD)
		files = append(files, url+treeCLabel+tailLabel)
	} else if proofType == abi.RegisteredSealProof_StackedDrg32GiBV1 {
		for i := 0; i < 12; i++ {
			files = append(files, url+layerLabel+strconv.Itoa(i)+tailLabel)
		}
		for j := 0; j < 8; j++ {
			files = append(files, url+treeCLabel+"-"+strconv.Itoa(j)+tailLabel)
		}
		files = append(files, url+treeD)
	}
	return files
}

func (st *Local) RemoveLayersAndTreeCAndD(ctx context.Context, sid abi.SectorID, proofType abi.RegisteredSealProof, typ storiface.SectorFileType, isDealWorker bool) error {

	log.Infof("===== try to remove sector [%+v] from worker", sid)

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	if len(si) == 0 {
		return xerrors.Errorf("can't delete sector %v(%d), not found", sid, typ)
	}

	for _, info := range si {
		p, ok := st.paths[info.ID]
		if !ok {
			continue
		}

		if p.local == "" { // TODO: can that even be the case?
			continue
		}

		// 不要修改index记录,会影响后面的fetch
		//if err := st.index.StorageDropSector(ctx, info.ID, sid, typ); err != nil {
		//	return xerrors.Errorf("dropping sector from index: %w", err)
		//}

		spath := filepath.Join(p.local, typ.String(), storiface.SectorName(sid))

		files := st.getLayersAndTreeCAndTreeDFiles(spath, proofType)
		log.Infof("===== try to remove sector [%+v] from worker--------->delete files:[%+v]", sid, files)
		for _, file := range files {
			log.Infof("remove %s", file)

			if err := os.RemoveAll(file); err != nil {
				log.Errorf("removing sector (%v) from %s: %+v", sid, spath, err)
			}
		}

		// 非deal worker则删除数据，否则不删除
		if !isDealWorker {
			// delete unseal
			spath = filepath.Join(p.local, "unsealed", storiface.SectorName(sid))
			log.Infof("remove %s", spath)
			if err := os.RemoveAll(spath); err != nil {
				log.Errorf("removing sector (%v) from %s: %+v", sid, spath, err)
			}
		}

	}
	log.Infof("===== who call me ??????????????????????????")
	return nil
}
func (st *Local) TransforDataToStorageServer(ctx context.Context, sector abi.SectorID, proofType abi.RegisteredSealProof, dest string, isDealWorker bool) error {
	// 获取worker存储
	pathCfg, err := st.localStorage.GetStorage()
	if err != nil || len(pathCfg.StoragePaths) == 0 {
		log.Errorf("try to send sector (%+v) to storage Server,pathCfg(%+v),Get Worker Storage Path error : %w", sector, pathCfg, err)
		return xerrors.Errorf("try to send sector (%+v) to storage Server,pathCfg(%+v),Get Worker Storage Path error : %w", sector, pathCfg, err)
	}

	// 尝试 开始发送 通过解析p.local来获取NFS ip
	ip, destPath := transfordata.PareseDestFromePath(dest)

	// 删除数据,完成传输文件
	log.Infof("===== after finished SealCommit1 for sector [%v], delete local layer,tree-c,tree-d files...", sector)
	st.RemoveLayersAndTreeCAndD(ctx, sector, proofType, storiface.FTCache, isDealWorker)

	start := time.Now()
	// send storiface.FTSealed
	srcSealedPath := filepath.Join(pathCfg.StoragePaths[0].Path, storiface.FTSealed.String()) + "/"
	src := storiface.SectorName(sector)
	sealedPath := filepath.Join(destPath, storiface.FTSealed.String()) + "/"
	log.Infof("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v)", sector, srcSealedPath, src, ip, sealedPath)
	_, err = transfordata.SendFile(srcSealedPath, src, sealedPath, ip)
	if err != nil {
		return err
	}

	// send storiface.FTCache
	srcCachePath := filepath.Join(pathCfg.StoragePaths[0].Path, storiface.FTCache.String()) + "/"
	cachePath := filepath.Join(destPath, storiface.FTCache.String()) + "/"
	//src:=SectorName(sector)
	log.Infof("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v)", sector, srcCachePath, src, ip, cachePath)
	err = transfordata.SendZipFile(srcCachePath, src, cachePath, ip)
	if err != nil {
		log.Infof("try to send sector(%+v) form srcPath(%s) + src(%s) ----->>>> to ip(%+v) destPath(%+v),error:%+v", sector, srcCachePath, src, ip, cachePath, err)
		return err
	}
	log.Infof("===== transfor sector(%+v) to Storage(%+v) cost time %s", sector, destPath, time.Now().Sub(start))

	// 删除sealed文件
	err = os.Remove(srcSealedPath + src)
	if err != nil {
		log.Warnf("===== transfor sector(%+v) to Storage(%+v) success, but remove %s error", sector, sealedPath, srcSealedPath)
		//panic(err)
	}

	// 删除cache文件
	err = os.RemoveAll(srcCachePath + src)
	if err != nil {
		log.Warnf("===== transfor sector(%+v) to Storage(%+v) success, but remove %s error", sector, cachePath, srcCachePath)
		//panic(err)
	}

	return nil
}
func (st *Local) Remove(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType, force bool) error {
	if bits.OnesCount(uint(typ)) != 1 {
		return xerrors.New("delete expects one file type")
	}

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	if len(si) == 0 && !force {
		return xerrors.Errorf("can't delete sector %v(%d), not found", sid, typ)
	}

	for _, info := range si {
		if err := st.removeSector(ctx, sid, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func (st *Local) RemoveCopies(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType) error {
	if bits.OnesCount(uint(typ)) != 1 {
		return xerrors.New("delete expects one file type")
	}

	si, err := st.index.StorageFindSector(ctx, sid, typ, 0, false)
	if err != nil {
		return xerrors.Errorf("finding existing sector %d(t:%d) failed: %w", sid, typ, err)
	}

	var hasPrimary bool
	for _, info := range si {
		if info.Primary {
			hasPrimary = true
			break
		}
	}

	if !hasPrimary {
		log.Warnf("RemoveCopies: no primary copies of sector %v (%s), not removing anything", sid, typ)
		return nil
	}

	for _, info := range si {
		if info.Primary {
			continue
		}

		if err := st.removeSector(ctx, sid, typ, info.ID); err != nil {
			return err
		}
	}

	return nil
}

func (st *Local) removeSector(ctx context.Context, sid abi.SectorID, typ storiface.SectorFileType, storage ID) error {
	p, ok := st.paths[storage]
	if !ok {
		return nil
	}

	if p.local == "" { // TODO: can that even be the case?
		return nil
	}

	if err := st.index.StorageDropSector(ctx, storage, sid, typ); err != nil {
		return xerrors.Errorf("dropping sector from index: %w", err)
	}

	spath := p.sectorPath(sid, typ)
	log.Infof("remove %s", spath)

	if err := os.RemoveAll(spath); err != nil {
		log.Errorf("removing sector (%v) from %s: %+v", sid, spath, err)
	}

	return nil
}

func (st *Local) MoveStorage(ctx context.Context, s abi.SectorID, ssize abi.SectorSize, types storiface.SectorFileType) error {
	dest, destIds, err := st.AcquireSector(ctx, s, ssize, storiface.FTNone, types, storiface.PathStorage, storiface.AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire dest storage: %w", err)
	}

	src, srcIds, err := st.AcquireSector(ctx, s, ssize, types, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
	if err != nil {
		return xerrors.Errorf("acquire src storage: %w", err)
	}

	for _, fileType := range storiface.PathTypes {
		if fileType&types == 0 {
			continue
		}

		sst, err := st.index.StorageInfo(ctx, ID(storiface.PathByType(srcIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		dst, err := st.index.StorageInfo(ctx, ID(storiface.PathByType(destIds, fileType)))
		if err != nil {
			return xerrors.Errorf("failed to get source storage info: %w", err)
		}

		if sst.ID == dst.ID {
			log.Debugf("not moving %v(%d); src and dest are the same", s, fileType)
			continue
		}

		if sst.CanStore {
			log.Debugf("not moving %v(%d); source supports storage", s, fileType)
			continue
		}

		log.Debugf("moving %v(%d) to storage: %s(se:%t; st:%t) -> %s(se:%t; st:%t)", s, fileType, sst.ID, sst.CanSeal, sst.CanStore, dst.ID, dst.CanSeal, dst.CanStore)

		if err := st.index.StorageDropSector(ctx, ID(storiface.PathByType(srcIds, fileType)), s, fileType); err != nil {
			return xerrors.Errorf("dropping source sector from index: %w", err)
		}

		if err := move(storiface.PathByType(src, fileType), storiface.PathByType(dest, fileType)); err != nil {
			// TODO: attempt some recovery (check if src is still there, re-declare)
			return xerrors.Errorf("moving sector %v(%d): %w", s, fileType, err)
		}

		if err := st.index.StorageDeclareSector(ctx, ID(storiface.PathByType(destIds, fileType)), s, fileType, true); err != nil {
			return xerrors.Errorf("declare sector %d(t:%d) -> %s: %w", s, fileType, ID(storiface.PathByType(destIds, fileType)), err)
		}
	}

	return nil
}

var errPathNotFound = xerrors.Errorf("fsstat: path not found")

func (st *Local) FsStat(ctx context.Context, id ID) (fsutil.FsStat, error) {
	st.localLk.RLock()
	defer st.localLk.RUnlock()

	p, ok := st.paths[id]
	if !ok {
		return fsutil.FsStat{}, errPathNotFound
	}

	return p.stat(st.localStorage)
}

func (st *Local) GetBindSectors(ctx context.Context) ([]abi.SectorID, error) {
	cfg, err := st.localStorage.GetStorage()
	if err != nil {
		return []abi.SectorID{}, xerrors.Errorf("getting local storage config: %w", err)
	}

	sectors := make([]abi.SectorID, 0)
	for _, path := range cfg.StoragePaths {
		sids, err := st.parseSectorsByPath(ctx, path.Path)
		if err != nil {
			return []abi.SectorID{}, xerrors.Errorf("opening path %s: %w", path.Path, err)
		}
		sectors = append(sectors, sids...)
	}

	return sectors, nil
	//return nil
}

func (st *Local) parseSectorsByPath(ctx context.Context, p string) ([]abi.SectorID, error) {
	sectors := make([]abi.SectorID, 0)

	// 只获取sector path 足够
	//for _, t := range storiface.PathTypes {
	ents, err := ioutil.ReadDir(filepath.Join(p, storiface.FTUnsealed.String()))
	if err != nil {
		return sectors, xerrors.Errorf("not found dir '%s': %w", filepath.Join(p, storiface.FTUnsealed.String()), err)
	}

	for _, ent := range ents {
		if ent.Name() == FetchTempSubdir {
			continue
		}

		sid, err := storiface.ParseSectorID(ent.Name())
		if err != nil {
			return []abi.SectorID{}, xerrors.Errorf("parse sector id %s: %w", ent.Name(), err)
		}
		sectors = append(sectors, sid)
	}
	//}

	return sectors, nil
}

var _ Store = &Local{}
