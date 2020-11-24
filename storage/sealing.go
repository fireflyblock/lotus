package storage

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	gr "github.com/filecoin-project/sector-storage/go-redis"
)

// TODO: refactor this to be direct somehow

func (m *Miner) Address() address.Address {
	return m.sealing.Address()
}

//func (m *Miner) AddPieceToAnySector(ctx context.Context, size abi.UnpaddedPieceSize, filePath, fileName string, d sealing.DealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
func (m *Miner) AddPieceToAnySector(ctx context.Context, size abi.UnpaddedPieceSize, filePath, fileName string, d sealing.DealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {

	return m.sealing.AddPieceToAnySector(ctx, size, filePath, fileName, d)
}

func (m *Miner) StartPackingSector(sectorNum abi.SectorNumber) error {
	return m.sealing.StartPacking(sectorNum)
}

func (m *Miner) ListSectors() ([]sealing.SectorInfo, error) {
	return m.sealing.ListSectors()
}

func (m *Miner) GetSectorInfo(sid abi.SectorNumber) (sealing.SectorInfo, error) {
	return m.sealing.GetSectorInfo(sid)
}

func (m *Miner) PledgeSector() error {
	return m.sealing.PledgeSector()
}

func (m *Miner) ForceSectorState(ctx context.Context, id abi.SectorNumber, state sealing.SectorState) error {
	return m.sealing.ForceSectorState(ctx, id, state)
}

func (m *Miner) CleanAllSectorDataInRedis(ctx context.Context) ([]gr.RedisField, error) {
	return m.sealing.CleanAllSectorDataInRedis(ctx)
}

func (m *Miner) RemoveSector(ctx context.Context, id abi.SectorNumber) error {
	return m.sealing.Remove(ctx, id)
}

func (m *Miner) MarkForUpgrade(id abi.SectorNumber) error {
	return m.sealing.MarkForUpgrade(id)
}

func (m *Miner) IsMarkedForUpgrade(id abi.SectorNumber) bool {
	return m.sealing.IsMarkedForUpgrade(id)
}
