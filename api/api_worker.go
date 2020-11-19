package api

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
	"github.com/filecoin-project/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/build"
)

type WorkerAPI interface {
	Version(context.Context) (build.Version, error)
	// TODO: Info() (name, ...) ?

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error) // TaskType -> Weight
	Paths(context.Context) ([]stores.StoragePath, error)
	Info(context.Context) (storiface.WorkerInfo, error)

	//storiface.WorkerCalls
	// Storage / Other
	//Remove(ctx context.Context, sector abi.SectorID) error

	//AddPiece(ctx context.Context, sector abi.SectorID, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data, apType string) (abi.PieceInfo, error)
	AddPiece(ctx context.Context, sector abi.SectorID, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, filePath string, fileName string, apType string) (abi.PieceInfo, error)

	storage.Sealer

	MoveStorage(ctx context.Context, sector abi.SectorID, types storiface.SectorFileType) error

	UnsealPiece(context.Context, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error
	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize) (bool, error)

	StorageAddLocal(ctx context.Context, path string) error

	// SetEnabled marks the worker as enabled/disabled. Not that this setting
	// may take a few seconds to propagate to task scheduler
	//SetEnabled(ctx context.Context, enabled bool) error
	//
	//Enabled(ctx context.Context) (bool, error)
	FetchRealData(ctx context.Context, id abi.SectorID) error
	Fetch(context.Context, abi.SectorID, storiface.SectorFileType, storiface.PathType, storiface.AcquireMode) error
	PushDataToStorage(ctx context.Context, sid abi.SectorID, dest string) error
	GetBindSectors(ctx context.Context) ([]abi.SectorID, error)

	Closing(context.Context) (<-chan struct{}, error)

	// WaitQuiet blocks until there are no tasks running
	//WaitQuiet(ctx context.Context) error

	// returns a random UUID of worker session, generated randomly when worker
	// process starts
	//ProcessSession(context.Context) (uuid.UUID, error)
	//
	//// Like ProcessSession, but returns an error when worker is disabled
	//Session(context.Context) (uuid.UUID, error)
}
