package sectorstorage

import (
	"context"
	"github.com/filecoin-project/sector-storage/docker"
	"github.com/filecoin-project/sector-storage/fsutil"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/elastic/go-sysinfo"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	logrus "github.com/filecoin-project/sector-storage/log"
	storage2 "github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/sector-storage/storiface"
)

var pathTypes = []storiface.SectorFileType{storiface.FTUnsealed, storiface.FTSealed, storiface.FTCache}

// local lock to lock addpiece task .
//var addPieceLock sync.Mutex

type WorkerConfig struct {
	SealProof abi.RegisteredSealProof
	TaskTypes []sealtasks.TaskType
	NoSwap    bool
}

type LocalWorker struct {
	scfg       *ffiwrapper.Config
	storage    stores.Store
	localStore *stores.Local
	sindex     stores.SectorIndex
	noSwap     bool

	acceptTasks map[sealtasks.TaskType]struct{}

	// 任务数量控制锁
	taskContralLk    sync.Mutex
	localTasksRecord map[abi.SectorID]LocalTask
}

type LocalTask struct {
	//sector   abi.SectorID
	taskType CurrentTaskStatus
}

func NewLocalWorker(wcfg WorkerConfig, store stores.Store, local *stores.Local, sindex stores.SectorIndex) *LocalWorker {
	acceptTasks := map[sealtasks.TaskType]struct{}{}
	for _, taskType := range wcfg.TaskTypes {
		acceptTasks[taskType] = struct{}{}
	}

	return &LocalWorker{
		scfg: &ffiwrapper.Config{
			SealProofType: wcfg.SealProof,
		},
		storage:    store,
		localStore: local,
		sindex:     sindex,
		noSwap:     wcfg.NoSwap,

		acceptTasks: acceptTasks,

		localTasksRecord: map[abi.SectorID]LocalTask{},
	}
}

type localWorkerPathProvider struct {
	w  *LocalWorker
	op storiface.AcquireMode
}

func GrpcInit() {
	docker.Init()
}

func (l *localWorkerPathProvider) AcquireSector(ctx context.Context, sector abi.SectorID, existing storiface.SectorFileType, allocate storiface.SectorFileType, sealing storiface.PathType) (storiface.SectorPaths, func(), error) {
	ssize, err := l.w.scfg.SealProofType.SectorSize()
	if err != nil {
		return storiface.SectorPaths{}, nil, err
	}
	paths, storageIDs, err := l.w.storage.AcquireSector(ctx, sector, ssize, existing, allocate, sealing, l.op)
	if err != nil {
		return storiface.SectorPaths{}, nil, err
	}

	releaseStorage, err := l.w.localStore.Reserve(ctx, sector, ssize, allocate, storageIDs, storiface.FSOverheadSeal)
	if err != nil {
		return storiface.SectorPaths{}, nil, xerrors.Errorf("reserving storage space: %w", err)
	}

	log.Debugf("acquired sector %d (e:%d; a:%d): %v", sector, existing, allocate, paths)

	return paths, func() {
		releaseStorage()

		for _, fileType := range pathTypes {
			if fileType&allocate == 0 {
				continue
			}

			sid := storiface.PathByType(storageIDs, fileType)

			if err := l.w.sindex.StorageDeclareSector(ctx, stores.ID(sid), sector, fileType, l.op == storiface.AcquireMove); err != nil {
				log.Errorf("declare sector error: %+v", err)
			}
		}
	}, nil
}

func (l *LocalWorker) sb() (ffiwrapper.Storage, error) {
	return ffiwrapper.New(&localWorkerPathProvider{w: l}, l.scfg)
}

func (l *LocalWorker) NewSector(ctx context.Context, sector abi.SectorID) error {
	sb, err := l.sb()
	if err != nil {
		return err
	}

	return sb.NewSector(ctx, sector)
}

//func (l *LocalWorker) AddPiece(ctx context.Context, sector abi.SectorID, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, filePath string, fileName string) (abi.PieceInfo, error) {
func (l *LocalWorker) AddPiece1(ctx context.Context, sector abi.SectorID, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader, apType string) (abi.PieceInfo, error) {
	/*sb, err := l.sb()
	if err != nil {
		return abi.PieceInfo{}, err
	}

	// check disk size
	for {
		if l.CheckCanDoTaskByAvaliableDisk(sector, 1) {
			break
		}
		log.Warnf("Disk avaliable size is low, and can not do addpiece for sector(%+v)", sector)
		time.Sleep(time.Minute * 5)
	}

	startAt := time.Now()
	pi, err := sb.AddPiece(ctx, sector, epcs, sz, r, apType)
	logrus.SchedLogger.Infof("===== worker  finished %+v  [AddPiece] start at:%s end at:%s cost time:%s",
		sector, startAt.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(startAt))

	return pi, err*/
	return abi.PieceInfo{}, nil
}

func (l *LocalWorker) Fetch(ctx context.Context, sector abi.SectorID, fileType storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) error {
	_, done, err := (&localWorkerPathProvider{w: l, op: am}).AcquireSector(ctx, sector, fileType, storiface.FTNone, ptype)
	if err != nil {
		return err
	}
	done()
	return nil
}

func (l *LocalWorker) SealPreCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, pieces []abi.PieceInfo, recover bool) (out storage2.PreCommit1Out, err error) {
	if !recover {
		{
			// cleanup previous failed attempts if they exist
			if err := l.storage.Remove(ctx, sector, storiface.FTSealed, true); err != nil {
				return nil, xerrors.Errorf("cleaning up sealed data: %w", err)
			}

			if err := l.storage.Remove(ctx, sector, storiface.FTCache, true); err != nil {
				return nil, xerrors.Errorf("cleaning up cache data: %w", err)
			}
		}
	}

	sb, err := l.sb()
	if err != nil {
		return nil, err
	}

	// check disk size
	for {
		if l.CheckCanDoTaskByAvaliableDisk(sector, 2) {
			break
		}
		log.Warnf("Disk avaliable size is low, and can not do p1 for sector(%+v)", sector)
		time.Sleep(time.Minute * 5)
	}

	startAt := time.Now()
	out, err = sb.SealPreCommit1(ctx, sector, ticket, pieces, recover)
	logrus.SchedLogger.Infof("===== worker  finished %+v  [SealPreCommit1] start at:%s end at:%s cost time:%s",
		sector, startAt.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(startAt))

	return out, err

	//	return sb.SealPreCommit1(ctx, sector, ticket, pieces)
}

func (l *LocalWorker) SealPreCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage2.PreCommit1Out) (cids storage2.SectorCids, err error) {
	sb, err := l.sb()
	if err != nil {
		return storage2.SectorCids{}, err
	}

	// check disk size
	for {
		if l.CheckCanDoTaskByAvaliableDisk(sector, 3) {
			break
		}
		log.Warnf("Disk avaliable size is low, and can not do p2 for sector(%+v)", sector)
		time.Sleep(time.Minute * 5)
	}

	startAt := time.Now()
	cids, err = sb.SealPreCommit2(ctx, sector, phase1Out)
	logrus.SchedLogger.Infof("===== worker  finished %+v [SealPreCommit2] start at:%s end at:%s cost time:%s",
		sector, startAt.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(startAt))
	return cids, err
	//return sb.SealPreCommit2(ctx, sector, phase1Out)
}

func (l *LocalWorker) SealCommit1(ctx context.Context, sector abi.SectorID, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage2.SectorCids) (output storage2.Commit1Out, err error) {
	sb, err := l.sb()
	if err != nil {
		return nil, err
	}

	startAt := time.Now()
	out, err := sb.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
	logrus.SchedLogger.Infof("===== worker  finished %+v [SealCommit1] start at:%s end at:%s cost time:%s",
		sector, startAt.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(startAt))

	//logrus.SchedLogger.Infof("===== after finished SealCommit1 for sector [%v], delete local layer,tree-c,tree-d files...", sector)
	//l.localStore.RemoveLayersAndTreeCAndD(ctx, sector, l.scfg.SealProofType, stores.FTCache)

	return out, err

	//return sb.SealCommit1(ctx, sector, ticket, seed, pieces, cids)
}

// 从manager传入index用以获取存储路径
/*
	传输文件到存储服务机器
	目的：worker直接写入文件到存储机器，并且创建sector索引
	注意点：通过这个方法传输文件，则不使用FetchRealData来拉取数据
*/
func (l *LocalWorker) PushDataToStorage(ctx context.Context, sid abi.SectorID, dest string) error {
	// 根据C2是否为存在C2类型来判断worker是否是做deal订单的机器
	var isDealWorker bool
	_, ok := l.acceptTasks[sealtasks.TTCommit2]
	if !ok {
		isDealWorker = true
		log.Infof("===== I am deal worker....")
	} else {
		isDealWorker = false
		log.Infof("===== I am normal worker....")
	}
	return l.localStore.TransforDataToStorageServer(ctx, sid, l.scfg.SealProofType, dest, isDealWorker)
}

// this method can only called by worker0
func (l *LocalWorker) FetchRealData(ctx context.Context, sector abi.SectorID) error {
	//logrus.SchedLogger.Infof("===== before to do SealCommit2 for sector [%v], delete remote workers' cache layer and tree_d and tree_c files",
	//	sector)
	//l.storage.RemoveLayersAndTreeCAndD(ctx,sector,l.scfg.SealProofType,stores.FTCache)

	// fetch cache dir and sealed dir
	logrus.SchedLogger.Infof("===== before to do SealCommit2 for sector [%v], fetch remote workers' cache layer and tree_d and tree_c files", sector)
	l.Fetch(ctx, sector, storiface.FTCache|storiface.FTSealed, storiface.PathStorage, storiface.AcquireMove)

	logrus.SchedLogger.Infof("===== before to do SealCommit2 for sector [%v], fetch finished", sector)
	return nil
}

func (l *LocalWorker) SealCommit2(ctx context.Context, sector abi.SectorID, phase1Out storage2.Commit1Out) (proof storage2.Proof, err error) {
	sb, err := l.sb()
	if err != nil {
		return nil, err
	}

	startAt := time.Now()
	proof, err = sb.SealCommit2(ctx, sector, phase1Out)
	logrus.SchedLogger.Infof("===== worker  finished %+v [Commit2] start at:%s end at:%s cost time:%s",
		sector, startAt.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(startAt))

	return proof, err
	//return sb.SealCommit2(ctx, sector, phase1Out)
}

func (l *LocalWorker) FinalizeSector(ctx context.Context, sector abi.SectorID, keepUnsealed []storage2.Range) error {
	sb, err := l.sb()
	if err != nil {
		return err
	}

	if err := sb.FinalizeSector(ctx, sector, keepUnsealed); err != nil {
		return xerrors.Errorf("finalizing sector: %w", err)
	}

	if len(keepUnsealed) == 0 {
		if err := l.storage.Remove(ctx, sector, storiface.FTUnsealed, true); err != nil {
			return xerrors.Errorf("removing unsealed data: %w", err)
		}
	}

	return nil
}

func (l *LocalWorker) ReleaseUnsealed(ctx context.Context, sector abi.SectorID, safeToFree []storage2.Range) error {
	return xerrors.Errorf("implement me")
}

func (l *LocalWorker) Remove(ctx context.Context, sector abi.SectorID) error {
	var err error

	if rerr := l.storage.Remove(ctx, sector, storiface.FTSealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTCache, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := l.storage.Remove(ctx, sector, storiface.FTUnsealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (l *LocalWorker) MoveStorage(ctx context.Context, sector abi.SectorID, types storiface.SectorFileType) error {
	ssize, err := l.scfg.SealProofType.SectorSize()
	if err != nil {
		return err
	}
	if err := l.storage.MoveStorage(ctx, sector, ssize, types); err != nil {
		return xerrors.Errorf("moving sealed data to storage: %w", err)
	}

	return nil
}

func (l *LocalWorker) UnsealPiece(ctx context.Context, sector abi.SectorID, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, randomness abi.SealRandomness, cid cid.Cid) error {
	sb, err := l.sb()
	if err != nil {
		return err
	}

	if err := sb.UnsealPiece(ctx, sector, index, size, randomness, cid); err != nil {
		return xerrors.Errorf("unsealing sector: %w", err)
	}

	if err := l.storage.RemoveCopies(ctx, sector, storiface.FTSealed); err != nil {
		return xerrors.Errorf("removing source data: %w", err)
	}

	if err := l.storage.RemoveCopies(ctx, sector, storiface.FTCache); err != nil {
		return xerrors.Errorf("removing source data: %w", err)
	}

	return nil
}

func (l *LocalWorker) ReadPiece(ctx context.Context, writer io.Writer, sector abi.SectorID, index storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (bool, error) {
	sb, err := l.sb()
	if err != nil {
		return false, err
	}

	return sb.ReadPiece(ctx, writer, sector, index, size)
}

func (l *LocalWorker) TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error) {
	return l.acceptTasks, nil
}

func (l *LocalWorker) Paths(ctx context.Context) ([]stores.StoragePath, error) {
	return l.localStore.Local(ctx)
}

func (l *LocalWorker) Info(context.Context) (storiface.WorkerInfo, error) {
	hostname, err := os.Hostname() // TODO: allow overriding from config
	if err != nil {
		panic(err)
	}

	gpus, err := ffi.GetGPUDevices()
	if err != nil {
		log.Errorf("getting gpu devices failed: %+v", err)
	}

	h, err := sysinfo.Host()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting host info: %w", err)
	}

	mem, err := h.Memory()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting memory info: %w", err)
	}

	memSwap := mem.VirtualTotal
	if l.noSwap {
		memSwap = 0
	}

	return storiface.WorkerInfo{
		Hostname: hostname,
		Resources: storiface.WorkerResources{
			MemPhysical: mem.Total,
			MemSwap:     memSwap,
			// 物理内存使用情况,设置一个假内存
			MemReserved: 2 << 30,
			//MemReserved: mem.VirtualUsed + mem.Total - mem.Available, // TODO: sub this process
			CPUs: uint64(runtime.NumCPU()),
			GPUs: gpus,
		},
	}, nil
}

func (l *LocalWorker) CheckCanDoTaskByAvaliableDisk(sector abi.SectorID, typ uint8) bool {
	l.taskContralLk.Lock() // 加锁防止多线程并行判断
	defer l.taskContralLk.Unlock()

	paths, err := l.Paths(context.TODO())
	if err != nil || len(paths) < 1 {
		log.Errorf("CheckCanDoTaskByAvaliableDisk can not found local path:%+v", err)
		return true
	}

	fsst, err := fsutil.Statfs(paths[0].LocalPath)
	if err != nil {
		log.Errorf("CheckCanDoTaskByAvaliableDisk get disk info err:%+v", err)
		return true
	}

	var needSize int64
	avaliableDiskSize := fsst.Available
	switch typ {
	case 1: // addpiece
		needSize, _ = l.getSectorUseDiskSize(ADDPIECE_COMPUTING)
		log.Debugf("sector(%+v) try to do addpiece task,needsize %d, avaliable size:%d GB", sector, needSize>>30, avaliableDiskSize>>30)
	case 2: // p1
		needSize, _ = l.getSectorUseDiskSize(PRE1_COMPUTING)
		log.Debugf("sector(%+v) try to do p1 task,needsize %d, avaliable size:%d GB", sector, needSize>>30, avaliableDiskSize>>30)
	case 3: // p2
		needSize, _ = l.getSectorUseDiskSize(COMMIT1_COMPUTING)
		log.Debugf("sector(%+v) try to do p2 task,needsize %d, avaliable size:%d GB", sector, needSize>>30, avaliableDiskSize>>30)
	}

	if needSize < avaliableDiskSize {
		return true
	}
	return false

	// 通过记录的任务获取可用size
	//for sid, localTask := range l.localTasksRecord {
	//	useSize, print := l.getSectorUseDiskSize(localTask.taskType)
	//	log.Debug("sector(%+v)--->%s--->GB", print, useSize)
	//}
}

// 获取对应任务对应的磁盘空间
func (l *LocalWorker) getSectorUseDiskSize(taskType CurrentTaskStatus) (int64, string) {
	switch taskType {
	case ADDPIECE_WAITING, ADDPIECE_COMPUTING, PRE1_WAITTING:
		return 32 << 30, "addpiece_waitting,addpiece_computing,p1_waitting"
	case PRE1_COMPUTING, PRE2_WAITING:
		return 416 << 30, "p1_computing,p2_waitting"
	case COMMIT1_WAITTING, COMMIT1_COMPUTING:
		return 40 << 30, "c1_waiting,c1_computing"
	default:
		return 0, "未知任务类型"
	}
}

func (l *LocalWorker) GetWorkerTaskCount(typ TaskType, path string) uint8 {

	switch typ {
	case TT_ADDPIECE:
		return 1
	case TT_PRECOMMIT2:
		return 3
	case TT_COMMIT1:
		return 2
	}

	fsst, err := fsutil.Statfs(path)
	if err != nil {
		log.Errorf("GetWorkerTaskCount get disk info err:%+v", err)
		return 0
	}

	diskSize := fsst.Capacity

	info, err := info(context.TODO())
	if err != nil {
		log.Errorf("GetWorkerTaskCount get source info err:%+v", err)
		return 0
	}
	tc := CalculateResources(info.Resources, diskSize)
	return tc.Pre1CommitSize
}

func info(context.Context) (storiface.WorkerInfo, error) {
	hostname, err := os.Hostname() // TODO: allow overriding from config
	if err != nil {
		panic(err)
	}

	gpus, err := ffi.GetGPUDevices()
	if err != nil {
		logrus.SchedLogger.Errorf("getting gpu devices failed: %+v", err)
	}

	h, err := sysinfo.Host()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting host info: %w", err)
	}

	mem, err := h.Memory()
	if err != nil {
		return storiface.WorkerInfo{}, xerrors.Errorf("getting memory info: %w", err)
	}

	return storiface.WorkerInfo{
		Hostname: hostname,
		Resources: storiface.WorkerResources{
			MemPhysical: mem.Total,
			MemSwap:     mem.VirtualTotal,
			// 物理内存使用情况,设置一个假内存
			MemReserved: 2 << 30,
			//MemReserved: mem.VirtualUsed + mem.Total - mem.Available, // TODO: sub this process
			CPUs: uint64(runtime.NumCPU()),
			GPUs: gpus,
		},
	}, nil
}

// 获取worker对应的sector
func (l *LocalWorker) GetBindSectors(ctx context.Context) ([]abi.SectorID, error) {
	return l.localStore.GetBindSectors(ctx)
}

func (l *LocalWorker) Closing(ctx context.Context) (<-chan struct{}, error) {
	return make(chan struct{}), nil
}

func (l *LocalWorker) Close() error {
	return nil
}

func (l *LocalWorker) AddPiece(ctx context.Context, sector abi.SectorID, epcs []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, filePath, fileName, apType string) (abi.PieceInfo, error) {
	sb, err := l.sb()
	if err != nil {
		return abi.PieceInfo{}, err
	}

	// check disk size
	for {
		if l.CheckCanDoTaskByAvaliableDisk(sector, 1) {
			break
		}
		log.Warnf("Disk avaliable size is low, and can not do addpiece for sector(%+v)", sector)
		time.Sleep(time.Minute * 5)
	}

	startAt := time.Now()
	pi, err := sb.AddPiece(ctx, sector, epcs, sz, filePath, fileName, apType)
	logrus.SchedLogger.Infof("===== worker  finished %+v  [AddPiece] start at:%s end at:%s cost time:%s",
		sector, startAt.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"), time.Now().Sub(startAt))

	return pi, err
}

var _ Worker = &LocalWorker{}
