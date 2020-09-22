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

	//AddPiece(ctx context.Context, sector abi.SectorID, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data, apType string) (abi.PieceInfo, error)
	AddPiece(ctx context.Context, sector abi.SectorID, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, filePath string, fileName string, apType string) (abi.PieceInfo, error)

	storage.Sealer

	MoveStorage(ctx context.Context, sector abi.SectorID, types stores.SectorFileType) error

	UnsealPiece(context.Context, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error
	ReadPiece(context.Context, io.Writer, abi.SectorID, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize) (bool, error)

	StorageAddLocal(ctx context.Context, path string) error

	FetchRealData(ctx context.Context, id abi.SectorID) error
	Fetch(context.Context, abi.SectorID, stores.SectorFileType, stores.PathType, stores.AcquireMode) error
	PushDataToStorage(ctx context.Context, sid abi.SectorID, dest string) error
	GetBindSectors(ctx context.Context) ([]abi.SectorID, error)

	Closing(context.Context) (<-chan struct{}, error)
}
