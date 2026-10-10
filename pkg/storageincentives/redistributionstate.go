// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20"
	"github.com/ethersphere/bee/v2/pkg/storage"
	storer "github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/transaction"
)

const loggerNameNode = "nodestatus"

const (
	redistributionStatusKey = "redistribution_state"
	purgeStaleDataThreshold = 10
)

type RedistributionState struct {
	mtx sync.Mutex

	stateStore     storage.StateStorer
	erc20Service   erc20.Service
	logger         log.Logger
	ethAddress     common.Address
	status         *Status
	currentBalance *big.Int
	txService      transaction.Service
}

// Status provide internal status of the nodes in the redistribution game
type Status struct {
	Phase             PhaseType
	IsFrozen          bool
	IsFullySynced     bool
	Round             uint64
	LastWonRound      uint64
	LastPlayedRound   uint64
	LastFrozenRound   uint64
	LastSelectedRound uint64
	Block             uint64
	Reward            *big.Int
	Fees              *big.Int
	RoundData         map[uint64]RoundData
	SampleDuration    time.Duration
	IsHealthy         bool
	// BlockSeenAt is the wall-clock time (unix seconds) at which Block was
	// read, so the phase can be estimated after a restart while the chain
	// backend is not synced yet.
	BlockSeenAt int64
	// Counts kept across restarts: /metrics is gone by the time a stop
	// knows its outcome.
	StopWaitedForReveal uint64
	RevealMissedOnStop  uint64
	RevealAfterRestart  uint64
}

type RoundData struct {
	CommitKey   []byte
	SampleData  *SampleData
	HasRevealed bool
	// CommitTx is the commit's signed transaction hash, stored before the
	// transaction is sent; zero while not known.
	CommitTx common.Hash
	// RevealTxs lists every reveal transaction hash sent in the round, each
	// stored before its send.
	RevealTxs []common.Hash
	// RevealSentBlock is the block at which the newest of RevealTxs was
	// listed.
	RevealSentBlock uint64
}

type SampleData struct {
	Anchor1            []byte
	ReserveSampleItems []storer.SampleItem
	ReserveSampleHash  swarm.Address
	StorageRadius      uint8
}

func NewStatus() *Status {
	return &Status{
		Reward:    big.NewInt(0),
		Fees:      big.NewInt(0),
		RoundData: make(map[uint64]RoundData),
	}
}

func NewRedistributionState(logger log.Logger, ethAddress common.Address, stateStore storage.StateStorer, erc20Service erc20.Service, contract transaction.Service) (*RedistributionState, error) {
	s := &RedistributionState{
		ethAddress:     ethAddress,
		stateStore:     stateStore,
		erc20Service:   erc20Service,
		logger:         logger.WithName(loggerNameNode).Register(),
		currentBalance: big.NewInt(0),
		txService:      contract,
		status:         NewStatus(),
	}

	status, err := s.Status()
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		status = NewStatus()
	}

	s.status = status
	return s, nil
}

// Status returns the node status
func (r *RedistributionState) Status() (*Status, error) {
	status := NewStatus()
	if err := r.stateStore.Get(redistributionStatusKey, status); err != nil {
		return nil, err
	}
	return status, nil
}

func (r *RedistributionState) save() {
	err := r.stateStore.Put(redistributionStatusKey, r.status)
	if err != nil {
		r.logger.Error(err, "saving redistribution status")
	}
}

// saveSync writes the status with a synced write and returns its error.
func (r *RedistributionState) saveSync() error {
	return r.stateStore.PutSync(redistributionStatusKey, r.status)
}

// SetERC20Service sets the token service, which the node creates after the
// state (it needs the chain sync wait).
func (r *RedistributionState) SetERC20Service(erc20Service erc20.Service) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.erc20Service = erc20Service
}

func (r *RedistributionState) SetCurrentBlock(block uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.Block = block
	r.status.BlockSeenAt = time.Now().Unix()
	r.save()
}

// lastBlock returns the last block read and when it was read.
func (r *RedistributionState) lastBlock() (uint64, time.Time) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.status.Block, time.Unix(r.status.BlockSeenAt, 0)
}

func (r *RedistributionState) SetCurrentEvent(phase PhaseType, round uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.Phase = phase
	r.status.Round = round
	r.save()
}

func (r *RedistributionState) IsFrozen() bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.status.IsFrozen
}

func (r *RedistributionState) SetFrozen(isFrozen bool, round uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if isFrozen && !r.status.IsFrozen { // record fronzen round if not set already
		r.status.LastFrozenRound = round
	}
	r.status.IsFrozen = isFrozen
	r.save()
}

func (r *RedistributionState) SetLastWonRound(round uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.LastWonRound = round
	r.save()
}

func (r *RedistributionState) IsFullySynced() bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.status.IsFullySynced
}

func (r *RedistributionState) SetFullySynced(isSynced bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.IsFullySynced = isSynced
	r.save()
}

func (r *RedistributionState) SetLastPlayedRound(round uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.LastPlayedRound = round
	r.save()
}

func (r *RedistributionState) SetLastSelectedRound(round uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.LastSelectedRound = round
	r.save()
}

// AddFee sets the internal node status
func (r *RedistributionState) AddFee(ctx context.Context, txHash common.Hash) {
	fee, err := r.txService.TransactionFee(ctx, txHash)
	if err != nil {
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.status.Fees.Add(r.status.Fees, fee)
	r.save()
}

// CalculateWinnerReward calculates the reward for the winner
func (r *RedistributionState) CalculateWinnerReward(ctx context.Context) error {
	currentBalance, err := r.erc20Service.BalanceOf(ctx, r.ethAddress)
	if err != nil {
		r.logger.Debug("error getting balance", "error", err)
		return err
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.status.Reward.Add(r.status.Reward, currentBalance.Sub(currentBalance, r.currentBalance))
	r.save()

	return nil
}

func (r *RedistributionState) SetBalance(ctx context.Context) error {
	// get current balance
	currentBalance, err := r.erc20Service.BalanceOf(ctx, r.ethAddress)
	if err != nil {
		r.logger.Debug("error getting balance", "error", err)
		return err
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.currentBalance.Set(currentBalance)
	r.save()

	return nil
}

func (r *RedistributionState) SampleData(round uint64) (SampleData, bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	rd, ok := r.status.RoundData[round]
	if !ok || rd.SampleData == nil {
		return SampleData{}, false
	}

	return *rd.SampleData, true
}

func (r *RedistributionState) SetSampleData(round uint64, sd SampleData, dur time.Duration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	rd := r.status.RoundData[round]
	rd.SampleData = &sd
	r.status.RoundData[round] = rd
	r.status.SampleDuration = dur

	r.save()
}

func (r *RedistributionState) CommitKey(round uint64) ([]byte, bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	rd, ok := r.status.RoundData[round]
	if !ok || rd.CommitKey == nil {
		return nil, false
	}

	return rd.CommitKey, true
}

// SetCommitKey stores the round's commit key with a synced write. On error
// nothing is changed and the commit must not be sent.
func (r *RedistributionState) SetCommitKey(round uint64, commitKey []byte) error {
	return r.updateRoundSync(round, func(rd *RoundData) {
		rd.CommitKey = commitKey
		rd.CommitTx = common.Hash{}
	})
}

// SetCommitTx stores the commit's signed hash with a synced write.
func (r *RedistributionState) SetCommitTx(round uint64, txHash common.Hash) error {
	return r.updateRoundSync(round, func(rd *RoundData) {
		rd.CommitTx = txHash
	})
}

// RemoveCommitKey removes the round's commit key and commit hash: the commit
// was not broadcast, or it reverted.
func (r *RedistributionState) RemoveCommitKey(round uint64) error {
	return r.updateRoundSync(round, func(rd *RoundData) {
		rd.CommitKey = nil
		rd.CommitTx = common.Hash{}
	})
}

// AddRevealTx lists a reveal hash with a synced write, before its send.
func (r *RedistributionState) AddRevealTx(round uint64, txHash common.Hash, block uint64) error {
	return r.updateRoundSync(round, func(rd *RoundData) {
		rd.RevealTxs = append(append([]common.Hash(nil), rd.RevealTxs...), txHash)
		rd.RevealSentBlock = block
	})
}

// CommitTx returns the commit's signed hash; zero when not known.
func (r *RedistributionState) CommitTx(round uint64) common.Hash {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.status.RoundData[round].CommitTx
}

// RevealTxs returns the listed reveal hashes, oldest first, and the block
// at which the newest was listed.
func (r *RedistributionState) RevealTxs(round uint64) ([]common.Hash, uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	rd := r.status.RoundData[round]
	return append([]common.Hash(nil), rd.RevealTxs...), rd.RevealSentBlock
}

// updateRoundSync applies f to the round's data and writes the status with
// a synced write; on a write error the change is undone.
func (r *RedistributionState) updateRoundSync(round uint64, f func(*RoundData)) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	prev, existed := r.status.RoundData[round]
	rd := prev
	f(&rd)
	r.status.RoundData[round] = rd

	if err := r.saveSync(); err != nil {
		if existed {
			r.status.RoundData[round] = prev
		} else {
			delete(r.status.RoundData, round)
		}
		return err
	}
	return nil
}

// countStop adds to the persisted stop counts.
func (r *RedistributionState) countStop(waited, missed, afterRestart uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.StopWaitedForReveal += waited
	r.status.RevealMissedOnStop += missed
	r.status.RevealAfterRestart += afterRestart
	if err := r.saveSync(); err != nil {
		r.logger.Error(err, "saving redistribution status")
	}
}

func (r *RedistributionState) HasRevealed(round uint64) bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	rd := r.status.RoundData[round]
	return rd.HasRevealed
}

func (r *RedistributionState) SetHealthy(isHealthy bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.status.IsHealthy = isHealthy
	r.save()
}

func (r *RedistributionState) IsHealthy() bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.status.IsHealthy
}

func (r *RedistributionState) SetHasRevealed(round uint64) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	rd := r.status.RoundData[round]
	rd.HasRevealed = true
	r.status.RoundData[round] = rd

	r.save()
}

func (r *RedistributionState) currentRoundAndPhase() (uint64, PhaseType) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.status.Round, r.status.Phase
}

func (r *RedistributionState) currentBlock() uint64 {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.status.Block
}

func (r *RedistributionState) purgeStaleRoundData() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	currentRound := r.status.Round

	if currentRound <= purgeStaleDataThreshold {
		return
	}

	thresholdRound := currentRound - purgeStaleDataThreshold
	hasChanged := false

	for round := range r.status.RoundData {
		if round < thresholdRound {
			delete(r.status.RoundData, round)
			hasChanged = true
		}
	}

	if hasChanged {
		r.save()
	}
}
