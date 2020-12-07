package sealing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	gr "github.com/filecoin-project/sector-storage/go-redis"
	logrus "github.com/filecoin-project/sector-storage/log"
	"github.com/filecoin-project/sector-storage/sealtasks"
	"strconv"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-statemachine"
	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/actors/policy"
)

var DealSectorPriority = 1024
var MaxTicketAge = abi.ChainEpoch(builtin0.EpochsInDay * 2)

func (m *Sealing) handlePacking(ctx statemachine.Context, sector SectorInfo) error {
	log.Infow("performing filling up rest of the sector...", "sector", sector.SectorNumber)

	var allocated abi.UnpaddedPieceSize
	for _, piece := range sector.Pieces {
		allocated += piece.Piece.Size.Unpadded()
	}

	ssize, err := sector.SectorType.SectorSize()
	if err != nil {
		return err
	}

	ubytes := abi.PaddedPieceSize(ssize).Unpadded()

	if allocated > ubytes {
		return xerrors.Errorf("too much data in sector: %d > %d", allocated, ubytes)
	}

	fillerSizes, err := fillersFromRem(ubytes - allocated)
	if err != nil {
		return err
	}

	if len(fillerSizes) > 0 {
		log.Warnf("Creating %d filler pieces for sector %d", len(fillerSizes), sector.SectorNumber)
	}

	fillerPieces, err := m.pledgeSector(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.existingPieceSizes(), fillerSizes...)
	if err != nil {
		return xerrors.Errorf("filling up the sector (%v): %w", fillerSizes, err)
	}

	return ctx.Send(SectorPacked{FillerPieces: fillerPieces})
}

func (m *Sealing) getTicket(ctx statemachine.Context, sector SectorInfo) (abi.SealRandomness, abi.ChainEpoch, error) {
	tok, epoch, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handlePreCommit1: api error, not proceeding: %+v", err)
		return nil, 0, nil
	}

	ticketEpoch := epoch - policy.SealRandomnessLookback
	buf := new(bytes.Buffer)
	if err := m.maddr.MarshalCBOR(buf); err != nil {
		return nil, 0, err
	}

	pci, err := m.api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		return nil, 0, xerrors.Errorf("getting precommit info: %w", err)
	}

	if pci != nil {
		ticketEpoch = pci.Info.SealRandEpoch
	}

	rand, err := m.api.ChainGetRandomnessFromTickets(ctx.Context(), tok, crypto.DomainSeparationTag_SealRandomness, ticketEpoch, buf.Bytes())
	if err != nil {
		return nil, 0, err
	}

	return abi.SealRandomness(rand), ticketEpoch, nil
}

func (m *Sealing) handleGetTicket(ctx statemachine.Context, sector SectorInfo) error {
	ticketValue, ticketEpoch, err := m.getTicket(ctx, sector)
	if err != nil {
		allocated, aerr := m.api.StateMinerSectorAllocated(ctx.Context(), m.maddr, sector.SectorNumber, nil)
		if aerr == nil {
			log.Errorf("error checking if sector is allocated: %+v", err)
		}

		if allocated {
			if sector.CommitMessage != nil {
				// Some recovery paths with unfortunate timing lead here
				return ctx.Send(SectorCommitFailed{xerrors.Errorf("sector %s is committed but got into the GetTicket state", sector.SectorNumber)})
			}

			log.Errorf("Sector %s precommitted but expired", sector.SectorNumber)
			return ctx.Send(SectorRemove{})
		}

		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("getting ticket failed: %w", err)})
	}

	return ctx.Send(SectorTicket{
		TicketValue: ticketValue,
		TicketEpoch: ticketEpoch,
	})
}

func (m *Sealing) handlePreCommit1(ctx statemachine.Context, sector SectorInfo) error {
	err := m.SelectWorkersToSetFaulty(sector.SectorNumber)
	if err == nil {
		return nil
	}

	if err := checkPieces(ctx.Context(), m.maddr, sector, m.api); err != nil { // Sanity check state
		switch err.(type) {
		case *ErrApi:
			log.Errorf("handlePreCommit1: api error, not proceeding: %+v", err)
			return nil
		case *ErrInvalidDeals:
			log.Warnf("invalid deals in sector %d: %v", sector.SectorNumber, err)
			return ctx.Send(SectorInvalidDealIDs{Return: RetPreCommit1})
		case *ErrExpiredDeals: // Probably not much we can do here, maybe re-pack the sector?
			return ctx.Send(SectorDealsExpired{xerrors.Errorf("expired dealIDs in sector: %w", err)})
		default:
			return xerrors.Errorf("checkPieces sanity check error: %w", err)
		}
	}

	tok, height, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handlePreCommit1: api error, not proceeding: %+v", err)
		return nil
	}

	if height-sector.TicketEpoch > MaxTicketAge {
		pci, err := m.api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
		if err != nil {
			log.Errorf("getting precommit info: %+v", err)
		}

		if pci == nil {
			return ctx.Send(SectorOldTicket{}) // go get new ticket
		}

		// TODO: allow configuring expected seal durations, if we're here, it's
		//  pretty unlikely that we'll precommit on time (unless the miner
		//  process has just restarted and the worker had the result ready)
	}

	p1Field := gr.SplicingBackupPubAndParamsField(sector.SectorNumber, sealtasks.TTPreCommit1, 0)
	// 如果ticketEpoch存在，或许曾经做个P1，
	// 判断Epoch是否会超时，预计我们一轮Seal过程耗时720个Epoch
	//height-(si.TicketEpoch+SealRandomnessLookback) > SealRandomnessLookbackLimit(si.SectorType)
	recover := false
	//if sector.TicketEpoch.String() != "" && len(sector.TicketValue) > 0 &&
	//	ticketEpoch-(sector.TicketEpoch+SealRandomnessLookback)+abi.ChainEpoch(720) > abi.ChainEpoch(10000) {
	//	ticketValue = sector.TicketValue
	//	ticketEpoch = sector.TicketEpoch
	//	recover = true
	//	log.Infof("sector(%+v) recovering do p1 use TicketEpoch %+v, TicketValue %+v", m.minerSector(sector.SectorNumber), sector.TicketEpoch, sector.TicketValue)
	//}

	p1RecoverDate := gr.PreCommit1RD{
		TicketEpoch: sector.SeedEpoch,
	}
	var pc1o storage.PreCommit1Out

	//backup params
	exist, err := m.rc.HExist(gr.RecoverName, p1Field)
	if err != nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
	}
	if exist {
		newCtx := context.WithValue(sector.sealingCtx(ctx.Context()), "p1RecoverDate", p1RecoverDate)
		//pc1o, err = m.sealer.SealPreCommit1(newCtx, m.minerSector(sector.SectorNumber), sector.TicketValue, sector.pieceInfos(), recover)
		pc1o, err = m.sealer.SealPreCommit1(newCtx, m.minerSector(sector.SectorType, sector.SectorNumber), sector.TicketValue, sector.pieceInfos(), recover)
		if err != nil {
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
		}

		err = m.rc.HGet(gr.RecoverName, p1Field, &p1RecoverDate)
		if err != nil {
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
		}
	} else {
		err = m.rc.HSet(gr.RecoverName, p1Field, p1RecoverDate)
		if err != nil {
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
		}

		newCtx := sector.sealingCtx(ctx.Context())
		//pc1o, err = m.sealer.SealPreCommit1(newCtx, m.minerSector(sector.SectorNumber), sector.TicketValue, sector.pieceInfos(), recover)
		pc1o, err = m.sealer.SealPreCommit1(newCtx, m.minerSector(sector.SectorType, sector.SectorNumber), sector.TicketValue, sector.pieceInfos(), recover)

		if err != nil {
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
		}

	}

	return ctx.Send(SectorPreCommit1{
		PreCommit1Out: pc1o,
	})
}

func (m *Sealing) handlePreCommit2(ctx statemachine.Context, sector SectorInfo) error {
	err := m.SelectWorkersToSetFaulty(sector.SectorNumber)
	if err == nil {
		return nil
	}

	cids, err := m.sealer.SealPreCommit2(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.PreCommit1Out)
	if err != nil {
		return ctx.Send(SectorSealPreCommit2Failed{xerrors.Errorf("seal pre commit(2) failed: %w", err)})
	}

	if cids.Unsealed == cid.Undef {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(2) returned undefined CommD")})
	}

	return ctx.Send(SectorPreCommit2{
		Unsealed: cids.Unsealed,
		Sealed:   cids.Sealed,
	})
}

// TODO: We should probably invoke this method in most (if not all) state transition failures after handlePreCommitting
func (m *Sealing) remarkForUpgrade(sid abi.SectorNumber) {
	err := m.MarkForUpgrade(sid)
	if err != nil {
		log.Errorf("error re-marking sector %d as for upgrade: %+v", sid, err)
	}
}

func (m *Sealing) handlePreCommitting(ctx statemachine.Context, sector SectorInfo) error {
	tok, height, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	waddr, err := m.api.StateMinerWorkerAddress(ctx.Context(), m.maddr, tok)
	if err != nil {
		log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	if err := checkPrecommit(ctx.Context(), m.Address(), sector, tok, height, m.api); err != nil {
		switch err := err.(type) {
		case *ErrApi:
			log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
			return nil
		case *ErrBadCommD: // TODO: Should this just back to packing? (not really needed since handlePreCommit1 will do that too)
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("bad CommD error: %w", err)})
		case *ErrExpiredTicket:
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("ticket expired: %w", err)})
		case *ErrBadTicket:
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("bad ticket: %w", err)})
		case *ErrInvalidDeals:
			log.Warnf("invalid deals in sector %d: %v", sector.SectorNumber, err)
			return ctx.Send(SectorInvalidDealIDs{Return: RetPreCommitting})
		case *ErrExpiredDeals:
			return ctx.Send(SectorDealsExpired{xerrors.Errorf("sector deals expired: %w", err)})
		case *ErrPrecommitOnChain:
			return ctx.Send(SectorPreCommitLanded{TipSet: tok}) // we re-did precommit
		case *ErrSectorNumberAllocated:
			log.Errorf("handlePreCommitFailed: sector number already allocated, not proceeding: %+v", err)
			// TODO: check if the sector is committed (not sure how we'd end up here)
			return nil
		default:
			return xerrors.Errorf("checkPrecommit sanity check error: %w", err)
		}
	}

	expiration, err := m.pcp.Expiration(ctx.Context(), sector.Pieces...)
	if err != nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("handlePreCommitting: failed to compute pre-commit expiry: %w", err)})
	}

	// Sectors must last _at least_ MinSectorExpiration + MaxSealDuration.
	// TODO: The "+10" allows the pre-commit to take 10 blocks to be accepted.
	nv, err := m.api.StateNetworkVersion(ctx.Context(), tok)
	if err != nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("failed to get network version: %w", err)})
	}

	msd := policy.GetMaxProveCommitDuration(actors.VersionForNetwork(nv), sector.SectorType)

	if minExpiration := height + msd + miner.MinSectorExpiration + 10; expiration < minExpiration {
		expiration = minExpiration
	}
	// TODO: enforce a reasonable _maximum_ sector lifetime?

	params := &miner.SectorPreCommitInfo{
		Expiration:   expiration,
		SectorNumber: sector.SectorNumber,
		SealProof:    sector.SectorType,

		SealedCID:     *sector.CommR,
		SealRandEpoch: sector.TicketEpoch,
		DealIDs:       sector.dealIDs(),
	}

	depositMinimum := m.tryUpgradeSector(ctx.Context(), params)

	enc := new(bytes.Buffer)
	if err := params.MarshalCBOR(enc); err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("could not serialize pre-commit sector parameters: %w", err)})
	}

	collateral, err := m.api.StateMinerPreCommitDepositForPower(ctx.Context(), m.maddr, *params, tok)
	if err != nil {
		return xerrors.Errorf("getting initial pledge collateral: %w", err)
	}

	deposit := big.Max(depositMinimum, collateral)

	deposit = big.NewInt(0)
	log.Infof("submitting precommit for sector %d (deposit: %s): ", sector.SectorNumber, deposit)
	mcid, err := m.api.SendMsg(ctx.Context(), waddr, m.maddr, miner.Methods.PreCommitSector, deposit, m.feeCfg.MaxPreCommitGasFee, enc.Bytes())
	if err != nil {
		if params.ReplaceCapacity {
			m.remarkForUpgrade(params.ReplaceSectorNumber)
		}
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}

	return ctx.Send(SectorPreCommitted{Message: mcid, PreCommitDeposit: deposit, PreCommitInfo: *params})
}

func (m *Sealing) handlePreCommitWait(ctx statemachine.Context, sector SectorInfo) error {
	if sector.PreCommitMessage == nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("precommit message was nil")})
	}

	// would be ideal to just use the events.Called handler, but it wouldn't be able to handle individual message timeouts
	log.Info("Sector precommitted: ", sector.SectorNumber)
	mw, err := m.api.StateWaitMsg(ctx.Context(), *sector.PreCommitMessage)
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{err})
	}

	switch mw.Receipt.ExitCode {
	case exitcode.Ok:
		// this is what we expect
	case exitcode.SysErrOutOfGas:
		// gas estimator guessed a wrong number
		return ctx.Send(SectorRetryPreCommit{})
	default:
		log.Error("sector precommit failed: ", mw.Receipt.ExitCode)
		err := xerrors.Errorf("sector precommit failed: %d", mw.Receipt.ExitCode)
		return ctx.Send(SectorChainPreCommitFailed{err})
	}

	log.Info("precommit message landed on chain: ", sector.SectorNumber)

	return ctx.Send(SectorPreCommitLanded{TipSet: mw.TipSetTok})
}

func (m *Sealing) handleWaitSeed(ctx statemachine.Context, sector SectorInfo) error {
	tok, _, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleWaitSeed: api error, not proceeding: %+v", err)
		return nil
	}

	pci, err := m.api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		return xerrors.Errorf("getting precommit info: %w", err)
	}
	if pci == nil {
		return ctx.Send(SectorChainPreCommitFailed{error: xerrors.Errorf("precommit info not found on chain")})
	}

	randHeight := pci.PreCommitEpoch + policy.GetPreCommitChallengeDelay()

	err = m.events.ChainAt(func(ectx context.Context, _ TipSetToken, curH abi.ChainEpoch) error {
		// in case of null blocks the randomness can land after the tipset we
		// get from the events API
		tok, _, err := m.api.ChainHead(ctx.Context())
		if err != nil {
			log.Errorf("handleCommitting: api error, not proceeding: %+v", err)
			return nil
		}

		buf := new(bytes.Buffer)
		if err := m.maddr.MarshalCBOR(buf); err != nil {
			return err
		}
		rand, err := m.api.ChainGetRandomnessFromBeacon(ectx, tok, crypto.DomainSeparationTag_InteractiveSealChallengeSeed, randHeight, buf.Bytes())
		if err != nil {
			err = xerrors.Errorf("failed to get randomness for computing seal proof (ch %d; rh %d; tsk %x): %w", curH, randHeight, tok, err)

			_ = ctx.Send(SectorChainPreCommitFailed{error: err})
			return err
		}

		_ = ctx.Send(SectorSeedReady{SeedValue: abi.InteractiveSealRandomness(rand), SeedEpoch: randHeight})

		return nil
	}, func(ctx context.Context, ts TipSetToken) error {
		log.Warn("revert in interactive commit sector step")
		// TODO: need to cancel running process and restart...
		return nil
	}, InteractivePoRepConfidence, randHeight)
	if err != nil {
		log.Warn("waitForPreCommitMessage ChainAt errored: ", err)
	}

	return nil
}

func (m *Sealing) handleCommitting(ctx statemachine.Context, sector SectorInfo) error {
	err := m.SelectWorkersToSetFaulty(sector.SectorNumber)
	if err == nil {
		return nil
	}

	if sector.CommitMessage != nil {
		log.Warnf("sector %d entered committing state with a commit message cid", sector.SectorNumber)

		ml, err := m.api.StateSearchMsg(ctx.Context(), *sector.CommitMessage)
		if err != nil {
			log.Warnf("sector %d searching existing commit message %s: %+v", sector.SectorNumber, *sector.CommitMessage, err)
		}

		if ml != nil {
			// some weird retry paths can lead here
			return ctx.Send(SectorRetryCommitWait{})
		}
	}

	log.Info("scheduling seal proof computation...")

	log.Infof("KOMIT %d %x(%d); %x(%d); %v; r:%x; d:%x", sector.SectorNumber, sector.TicketValue, sector.TicketEpoch, sector.SeedValue, sector.SeedEpoch, sector.pieceInfos(), sector.CommR, sector.CommD)

	if sector.CommD == nil || sector.CommR == nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("sector had nil commR or commD")})
	}

	cids := storage.SectorCids{
		Unsealed: *sector.CommD,
		Sealed:   *sector.CommR,
	}
	c2in, err := m.sealer.SealCommit1(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.TicketValue, sector.SeedValue, sector.pieceInfos(), cids)
	if err != nil {
		return ctx.Send(SectorComputeProofFailed{xerrors.Errorf("computing seal proof failed(1): %w", err)})
	}

	go func() {
		log.Infof("====== send turnOnCh c1, sectorID %+V", sector.SectorNumber)
		m.turnOnCh <- gr.SplicingBackupPubAndParamsField(sector.SectorNumber, sealtasks.TTCommit1, 0)
	}()

	proof, err := m.sealer.SealCommit2(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), c2in)
	if err != nil {
		return ctx.Send(SectorComputeProofFailed{xerrors.Errorf("computing seal proof failed(2): %w", err)})
	}

	return ctx.Send(SectorCommitted{
		Proof: proof,
	})
}

func (m *Sealing) handleSubmitCommit(ctx statemachine.Context, sector SectorInfo) error {
	tok, _, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	if err := m.checkCommit(ctx.Context(), sector, sector.Proof, tok); err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("commit check error: %w", err)})
	}

	enc := new(bytes.Buffer)
	params := &miner.ProveCommitSectorParams{
		SectorNumber: sector.SectorNumber,
		Proof:        sector.Proof,
	}

	if err := params.MarshalCBOR(enc); err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("could not serialize commit sector parameters: %w", err)})
	}

	waddr, err := m.api.StateMinerWorkerAddress(ctx.Context(), m.maddr, tok)
	if err != nil {
		log.Errorf("handleCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	pci, err := m.api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		return xerrors.Errorf("getting precommit info: %w", err)
	}
	if pci == nil {
		return ctx.Send(SectorCommitFailed{error: xerrors.Errorf("precommit info not found on chain")})
	}

	collateral, err := m.api.StateMinerInitialPledgeCollateral(ctx.Context(), m.maddr, pci.Info, tok)
	if err != nil {
		return xerrors.Errorf("getting initial pledge collateral: %w", err)
	}

	collateral = big.Sub(collateral, pci.PreCommitDeposit)
	if collateral.LessThan(big.Zero()) {
		collateral = big.Zero()
	}

	collateral = big.NewInt(0)
	// TODO: check seed / ticket / deals are up to date
	mcid, err := m.api.SendMsg(ctx.Context(), waddr, m.maddr, miner.Methods.ProveCommitSector, collateral, m.feeCfg.MaxCommitGasFee, enc.Bytes())
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}

	return ctx.Send(SectorCommitSubmitted{
		Message: mcid,
	})
}

func (m *Sealing) handleCommitWait(ctx statemachine.Context, sector SectorInfo) error {
	if sector.CommitMessage == nil {
		log.Errorf("sector %d entered commit wait state without a message cid", sector.SectorNumber)
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("entered commit wait with no commit cid")})
	}

	mw, err := m.api.StateWaitMsg(ctx.Context(), *sector.CommitMessage)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("failed to wait for porep inclusion: %w", err)})
	}

	switch mw.Receipt.ExitCode {
	case exitcode.Ok:
		// this is what we expect
	case exitcode.SysErrOutOfGas:
		// gas estimator guessed a wrong number
		return ctx.Send(SectorRetrySubmitCommit{})
	default:
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("submitting sector proof failed (exit=%d, msg=%s) (t:%x; s:%x(%d); p:%x)", mw.Receipt.ExitCode, sector.CommitMessage, sector.TicketValue, sector.SeedValue, sector.SeedEpoch, sector.Proof)})
	}

	si, err := m.api.StateSectorGetInfo(ctx.Context(), m.maddr, sector.SectorNumber, mw.TipSetTok)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("proof validation failed, calling StateSectorGetInfo: %w", err)})
	}
	if si == nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("proof validation failed, sector not found in sector set after cron")})
	}

	return ctx.Send(SectorProving{})
}

func (m *Sealing) handleFinalizeSector(ctx statemachine.Context, sector SectorInfo) error {
	// TODO: Maybe wait for some finality

	if err := m.sealer.FinalizeSector(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorType, sector.SectorNumber), sector.keepUnsealedRanges(false)); err != nil {
		return ctx.Send(SectorFinalizeFailed{xerrors.Errorf("finalize sector: %w", err)})
	}

	return ctx.Send(SectorFinalized{})
}

func (m *Sealing) handleProvingSector(ctx statemachine.Context, sector SectorInfo) error {
	// TODO: track sector health / expiration
	log.Infof("Proving sector %d", sector.SectorNumber)

	if err := m.sealer.ReleaseUnsealed(ctx.Context(), m.minerSector(sector.SectorType, sector.SectorNumber), sector.keepUnsealedRanges(true)); err != nil {
		log.Error(err)
	}

	// TODO: Watch termination
	// TODO: Auto-extend if set

	return nil
}

func (m *Sealing) DeleteDataForSid(sectorID abi.SectorNumber) {
	log.Infof("===== rd restart DeleteDataForSid, sectorID %+v", sectorID)
	sealKey := gr.RedisKey(fmt.Sprintf("seal_ap_%d", sectorID))
	taskList := make([]sealtasks.TaskType, 0)
	hostname := ""

	res, err := m.rc.Exist(sealKey)
	if err != nil {
		log.Errorf("===== rd get sealKey err, sectorID %+v err %+v", sectorID, err)
		return
	}
	if res > 0 {
		list := []sealtasks.TaskType{sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1}
		taskList = append(taskList, list...)
	} else {
		list := []sealtasks.TaskType{sealtasks.TTAddPiecePl, sealtasks.TTPreCommit1, sealtasks.TTPreCommit2, sealtasks.TTCommit1}
		taskList = append(taskList, list...)
	}

	for _, v := range taskList {
		f := gr.SplicingBackupPubAndParamsField(sectorID, v, 0)
		ex, err := m.rc.HExist(gr.PubName, f)
		if err != nil {
			log.Errorf("===== rd HExist hostname for DeleteDataForSid, sectorID %+v err %+v", sectorID, err)
			continue
		}
		if ex {
			err = m.rc.HGet(gr.PubName, f, &hostname)
			if err != nil {
				logrus.SchedLogger.Errorf("===== rd DeleteDataForSid, hget pledge pub err %+v sectorID %+v, field %+v\n", err, sectorID, f)
			}

			//1 worker count
			m.DeleteWorkerCountAndTaskStatus(f, hostname)
			//2 retry count
			m.FreeRetryCount(f, hostname)
			//3 cache file count
			m.FreeCacheFileCount(f, hostname)
		}

		//4 pub
		m.rc.HDel(gr.ParamsName, f)
		//5 params
		m.rc.HDel(gr.PubName, f)
		//6 res
		m.rc.HDel(gr.PubResName, f)
		//7 res params
		m.rc.HDel(gr.ParamsResName, f)
		//8 pub time
		m.rc.HDel(gr.PubTime, f)
		if v == sealtasks.TTPreCommit1 {
			//9 recovery
			m.rc.HDel(gr.RecoverName, f)
		}
		//10 wait time
		m.rc.HDel(gr.RecoveryWaitTime, f)
	}

	if res == 0 {
		return
	}

	var count int64
	err = m.rc.Get(sealKey, &count)
	if err != nil {
		log.Errorf("===== rd get sealKey err, sectorID %+v err %+v", sectorID, err)
		return
	}

	m.rc.Del(sealKey)

	var i int64 = 1
	for i = 1; i <= count; i++ {
		f := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPieceSe, uint64(i))
		ex, err := m.rc.HExist(gr.PubName, f)
		if err != nil {
			log.Errorf("===== rd HExist hostname for DeleteDataForSid, sectorID %+v err %+v", sectorID, err)
			continue
		}
		if ex {
			err = m.rc.HGet(gr.PubName, f, &hostname)
			if err != nil {
				logrus.SchedLogger.Errorf("===== rd DeleteDataForSid, hget seal pub err %+v sectorID %+v, field %+v\n", err, sectorID, f)
			}

			//1 worker count
			m.DeleteWorkerCountAndTaskStatus(f, hostname)
			//2 retry count
			m.FreeRetryCount(f, hostname)
			//3 cache file count
			m.FreeCacheFileCount(f, hostname)
		}
		//4 pub
		m.rc.HDel(gr.ParamsName, f)
		//5 res params
		m.rc.HDel(gr.PubName, f)
		//6 res
		m.rc.HDel(gr.PubResName, f)
		//7 res params
		m.rc.HDel(gr.ParamsResName, f)
		//8 pub time
		m.rc.HDel(gr.PubTime, f)
		//9 wait time
		m.rc.HDel(gr.RecoveryWaitTime, f)
	}
}

func (m *Sealing) FreeRetryCount(retryField gr.RedisField, hostname string) error {
	_, err := m.rc.HDel(gr.RETRY, retryField)
	//log.Infof("===== rd free retry count hostname %+v sectorID %+v taskType %+v field %+v", hostname, sid, tt, retryField)
	if err != nil {
		log.Errorf("===== rd free retry count field %+v err %+v", retryField, err)
		return err
	}
	return nil
}

func (m *Sealing) FreeCacheFileCount(retryField gr.RedisField, hostname string) error {
	newField := gr.TypeToType(retryField, "p1")

	m.rc.HDel(gr.SplicingCounterP1Key(hostname), newField)

	return nil
}

func (m *Sealing) DeleteWorkerCountAndTaskStatus(field gr.RedisField, hostname string) {
	sid, tt, _, _ := field.TailoredPubAndParamsfield()

	wCKeyList, err := m.rc.HKeys(gr.SplicingTaskCounntKey(hostname))
	if err != nil {
		log.Errorf("===== rd HKeys hostname for DeleteWorkerCountForTask, sectorID %+v taskType %s err %+v", sid, tt, err)
		return
	}
	for _, v := range wCKeyList {
		if field == v {
			log.Infof("===== rd del workerCount for hostname %s sectorID %+v taskType %s ", gr.SplicingTaskCounntKey(hostname), sid, tt)
			m.rc.HDel(gr.SplicingTaskCounntKey(hostname), v)
		}
	}

	tSKeyList, err := m.rc.HKeys(gr.RedisKey(hostname))
	for _, key := range tSKeyList {
		if key.ToString() == strconv.Itoa(int(sid)) {
			log.Infof("===== rd del task status, key %+v", key)
			m.rc.HDel(gr.RedisKey(hostname), key)
		}
	}
}

func (m *Sealing) SelectWorkersToSetFaulty(sectorID abi.SectorNumber) error {
	pledgeField := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPiecePl, 0)
	plExist, err := m.rc.HExist(gr.PubName, pledgeField)
	if err != nil {
		log.Errorf("===== rd HExist hostname for SelectWorkersToSetFaulty, sectorID %+v taskType %s err %+v", sectorID, sealtasks.TTAddPiecePl, err)
		return err
	}

	sealField := gr.SplicingBackupPubAndParamsField(sectorID, sealtasks.TTAddPieceSe, 0)
	seExist, err := m.rc.HExist(gr.PubName, sealField)
	if err != nil {
		log.Errorf("===== rd HExist hostname for SelectWorkersToSetFaulty, sectorID %+v taskType %s err %+v", sectorID, sealtasks.TTAddPieceSe, err)
		return err
	}

	if !plExist && !seExist {
		err = m.sectors.Send(sectorID, SectorForceState{Faulty})
		if err != nil {
			return err
		}
		log.Infof("===== rd select sector %+v to set Faulty\n", sectorID)
		return nil
	}

	return errors.New(fmt.Sprintf("failed to select sector %+v to set Faulty", sectorID))
}
