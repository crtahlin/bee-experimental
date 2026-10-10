// Copyright 2022 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/postage/postagecontract"
	"github.com/ethersphere/bee/v2/pkg/safe"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/staking"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	"resenje.org/singleflight"
)

const loggerName = "storageincentives"

const (
	DefaultBlocksPerRound = 152
	DefaultBlocksPerPhase = DefaultBlocksPerRound / 4

	// min # of transactions our wallet should be able to cover
	minTxCountToCover = 15

	// average tx gas used by transactions issued from agent
	avgTxGas = 250_000
)

type ChainBackend interface {
	BlockNumber(context.Context) (uint64, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	BalanceAt(ctx context.Context, address common.Address, block *big.Int) (*big.Int, error)
	SuggestedFeeAndTip(ctx context.Context, gasPrice *big.Int, boostPercent int) (*big.Int, *big.Int, error)
}

type Health interface {
	IsHealthy() bool
}

type Agent struct {
	logger                 log.Logger
	metrics                metrics
	backend                ChainBackend
	blocksPerRound         uint64
	contract               redistribution.Contract
	batchExpirer           postagecontract.PostageBatchExpirer
	redistributionStatuser staking.RedistributionStatuser
	store                  storer.Reserve
	reserveProofMode       string
	fullSyncedFunc         func() bool
	overlay                swarm.Address
	quit                   chan struct{}
	wg                     sync.WaitGroup
	state                  *RedistributionState
	chainStateGetter       postage.ChainStateGetter
	commitLock             sync.Mutex
	health                 Health
	sampleFlight           singleflight.Group[string, sampleResult]
}

func New(overlay swarm.Address,
	ethAddress common.Address,
	backend ChainBackend,
	contract redistribution.Contract,
	batchExpirer postagecontract.PostageBatchExpirer,
	redistributionStatuser staking.RedistributionStatuser,
	store storer.Reserve,
	fullSyncedFunc func() bool,
	blockTime func() time.Duration,
	blocksPerRound,
	blocksPerPhase uint64,
	stateStore storage.StateStorer,
	chainStateGetter postage.ChainStateGetter,
	erc20Service erc20.Service,
	tranService transaction.Service,
	health Health,
	logger log.Logger,
	reserveProofMode string,
) (*Agent, error) {
	a := &Agent{
		overlay:                overlay,
		metrics:                newMetrics(),
		backend:                backend,
		logger:                 logger.WithName(loggerName).Register(),
		contract:               contract,
		batchExpirer:           batchExpirer,
		store:                  store,
		reserveProofMode:       reserveProofMode,
		fullSyncedFunc:         fullSyncedFunc,
		blocksPerRound:         blocksPerRound,
		quit:                   make(chan struct{}),
		redistributionStatuser: redistributionStatuser,
		health:                 health,
		chainStateGetter:       chainStateGetter,
	}

	state, err := NewRedistributionState(logger, ethAddress, stateStore, erc20Service, tranService)
	if err != nil {
		return nil, err
	}

	a.state = state

	a.wg.Add(1)
	go a.start(blockTime, a.blocksPerRound, blocksPerPhase)

	return a, nil
}

// start polls the current block number, calculates, and publishes only once the current phase.
// Each round is blocksPerRound long and is divided into three blocksPerPhase long phases: commit, reveal, claim.
// The sample phase is triggered upon entering the claim phase and may run until the end of the commit phase.
// If our neighborhood is selected to participate, a sample is created during the sample phase. In the commit phase,
// the sample is submitted, and in the reveal phase, the obfuscation key from the commit phase is submitted.
// Next, in the claim phase, we check if we've won, and the cycle repeats. The cycle must occur in the length of one round.
// blockTime is read each time it is needed, so a change in the chain's block
// time is followed without a restart (#540).
func (a *Agent) start(blockTime func() time.Duration, blocksPerRound, blocksPerPhase uint64) {
	defer a.wg.Done()

	phaseEvents := newEvents()
	defer phaseEvents.Close()

	logErr := func(phase PhaseType, round uint64, err error) {
		if err != nil {
			a.logger.Error(err, "phase failed", "phase", phase, "round", round)
		}
	}

	phaseEvents.On(commit, func(ctx context.Context) {
		phaseEvents.Cancel(claim)

		round, _ := a.state.currentRoundAndPhase()
		err := a.handleCommit(ctx, round)
		logErr(commit, round, err)
	})

	phaseEvents.On(reveal, func(ctx context.Context) {
		phaseEvents.Cancel(commit, sample)
		round, _ := a.state.currentRoundAndPhase()
		logErr(reveal, round, a.handleReveal(ctx, round))
	})

	phaseEvents.On(claim, func(ctx context.Context) {
		phaseEvents.Cancel(reveal)
		phaseEvents.Publish(sample)

		round, _ := a.state.currentRoundAndPhase()
		logErr(claim, round, a.handleClaim(ctx, round))
	})

	phaseEvents.On(sample, func(ctx context.Context) {
		round, _ := a.state.currentRoundAndPhase()
		isPhasePlayed, err := a.handleSample(ctx, round)
		logErr(sample, round, err)

		// Sample handled could potentially take long time, therefore it could overlap with commit
		// phase of next round. When that case happens commit event needs to be triggered once more
		// in order to handle commit phase with delay.
		currentRound, currentPhase := a.state.currentRoundAndPhase()
		if isPhasePlayed &&
			currentPhase == commit &&
			currentRound-1 == round {
			phaseEvents.Publish(commit)
		}
	})

	var (
		prevPhase    PhaseType = -1
		currentPhase PhaseType
	)

	phaseCheck := func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, blockTime()*time.Duration(blocksPerRound))
		defer cancel()

		a.metrics.BackendCalls.Inc()
		block, err := a.backend.BlockNumber(ctx)
		if err != nil {
			a.metrics.BackendErrors.Inc()
			a.logger.Error(err, "getting block number")
			return
		}

		a.state.SetCurrentBlock(block)

		round := block / blocksPerRound

		a.metrics.Round.Set(float64(round))

		p := block % blocksPerRound
		if p < blocksPerPhase {
			currentPhase = commit // [0, 37]
		} else if p >= blocksPerPhase && p < 2*blocksPerPhase { // [38, 75]
			currentPhase = reveal
		} else if p >= 2*blocksPerPhase {
			currentPhase = claim // [76, 151]
		}

		// write the current phase only once
		if currentPhase == prevPhase {
			return
		}

		prevPhase = currentPhase
		a.metrics.CurrentPhase.Set(float64(currentPhase))

		a.logger.Info("entered new phase", "phase", currentPhase.String(), "round", round, "block", block)

		a.state.SetCurrentEvent(currentPhase, round)
		a.state.SetFullySynced(a.fullSyncedFunc())
		a.state.SetHealthy(a.health.IsHealthy())
		safe.Go(a.logger, "storageincentives-purge-stale-round-data", func() {
			a.state.purgeStaleRoundData()
		})

		// check if node is frozen starting from the next block
		isFrozen, err := a.redistributionStatuser.IsOverlayFrozen(ctx, block+1)
		if err != nil {
			a.logger.Error(err, "error checking if stake is frozen")
		} else {
			a.state.SetFrozen(isFrozen, round)
		}

		phaseEvents.Publish(currentPhase)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-a.quit
		cancel()
	}()

	// manually invoke phaseCheck initially in order to set initial data asap
	phaseCheck(ctx)

	for {
		phaseCheckInterval := blockTime()
		// optimization, we do not need to check the phase change at every new block
		if blocksPerPhase > 10 {
			phaseCheckInterval *= 5
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(phaseCheckInterval):
			phaseCheck(ctx)
		}
	}
}

func (a *Agent) handleCommit(ctx context.Context, round uint64) error {
	// commit event handler has to be guarded with lock to avoid
	// race conditions when handler is triggered again from sample phase
	a.commitLock.Lock()
	defer a.commitLock.Unlock()

	if _, exists := a.state.CommitKey(round); exists {
		// already committed on this round, phase is skipped
		return nil
	}

	// the sample has to come from previous round to be able to commit it
	sample, exists := a.state.SampleData(round - 1)
	if !exists {
		// In absence of sample, phase is skipped
		return nil
	}

	// The node computes the round from its compiled-in round length. If the
	// contract disagrees, the round length has changed under it (a redeployed
	// contract, #540) and a commit would land in the wrong phase and waste
	// gas; not committing costs nothing. A failed call is not a disagreement:
	// the node plays as it always has.
	//
	// A difference of one round is tolerated. The node's block number is an
	// estimate that can run a block ahead of the chain, so at the first block
	// of a round the contract, asked at the latest block, still reports the
	// previous round, and the commit would succeed once mined. A changed
	// round length makes the two differ by far more than one.
	contractRound, err := a.contract.CurrentRound(ctx)
	switch {
	case err != nil:
		a.logger.Debug("could not read the contract's round, committing on the node's own", "round", round, "error", err)
	case roundsDiffer(contractRound, round):
		a.metrics.RoundMismatch.Inc()
		a.logger.Error(nil, "the node's round disagrees with the redistribution contract's; not committing in this round", "node_round", round, "contract_round", contractRound)
		return nil
	}

	err = a.commit(ctx, sample, round)
	if err != nil {
		return err
	}

	a.state.SetLastPlayedRound(round)

	return nil
}

// roundsDiffer reports whether the contract's round and the node's differ by
// more than the one round an estimated block number can account for.
func roundsDiffer(contractRound, nodeRound uint64) bool {
	if contractRound > nodeRound {
		return contractRound-nodeRound > 1
	}
	return nodeRound-contractRound > 1
}

func (a *Agent) handleReveal(ctx context.Context, round uint64) error {
	// reveal requires the commitKey from the same round
	commitKey, exists := a.state.CommitKey(round)
	if !exists {
		// In absence of commitKey, phase is skipped
		return nil
	}

	// reveal requires sample from previous round
	sample, exists := a.state.SampleData(round - 1)
	if !exists {
		// Sample must have been saved so far
		return fmt.Errorf("sample not found in reveal phase")
	}

	a.metrics.RevealPhase.Inc()

	rsh := sample.ReserveSampleHash.Bytes()
	txHash, err := a.contract.Reveal(ctx, sample.StorageRadius, rsh, commitKey)
	if err != nil {
		a.metrics.ErrReveal.Inc()
		return err
	}
	a.state.AddFee(ctx, txHash)

	a.state.SetHasRevealed(round)

	return nil
}

func (a *Agent) handleClaim(ctx context.Context, round uint64) error {
	hasRevealed := a.state.HasRevealed(round)
	if !hasRevealed {
		// When there was no reveal in same round, phase is skipped
		return nil
	}

	a.metrics.ClaimPhase.Inc()

	isWinner, err := a.contract.IsWinner(ctx)
	if err != nil {
		a.metrics.ErrWinner.Inc()
		return err
	}

	if !isWinner {
		a.logger.Info("not a winner")
		// When there is nothing to claim (node is not a winner), phase is played
		return nil
	}

	a.state.SetLastWonRound(round)
	a.metrics.Winner.Inc()

	// In case when there are too many expired batches, Claim trx could runs out of gas.
	// To prevent this, node should first expire batches before Claiming a reward.
	err = a.batchExpirer.ExpireBatches(ctx)
	if err != nil {
		a.logger.Info("expire batches failed", "err", err)
		// Even when error happens, proceed with claim handler
		// because this should not prevent node from claiming a reward
	}

	errBalance := a.state.SetBalance(ctx)
	if errBalance != nil {
		a.logger.Info("could not set balance", "err", err)
	}

	sampleData, exists := a.state.SampleData(round - 1)
	if !exists {
		return fmt.Errorf("sample not found")
	}

	anchor2, err := a.contract.ReserveSalt(ctx)
	if err != nil {
		a.logger.Info("failed getting anchor after second reveal", "err", err)
	}

	proofs, err := makeInclusionProofs(sampleData.ReserveSampleItems, sampleData.Anchor1, anchor2)
	if err != nil {
		return fmt.Errorf("making inclusion proofs: %w", err)
	}

	txHash, err := a.contract.Claim(ctx, proofs)
	if err != nil {
		a.metrics.ErrClaim.Inc()
		return fmt.Errorf("claiming win: %w", err)
	}

	a.logger.Info("claimed win")

	if errBalance == nil {
		errReward := a.state.CalculateWinnerReward(ctx)
		if errReward != nil {
			a.logger.Info("calculate winner reward", "err", err)
		}
	}

	a.state.AddFee(ctx, txHash)

	return nil
}

func (a *Agent) handleSample(ctx context.Context, round uint64) (bool, error) {
	// minimum proximity between the anchor and the stored chunks. The depth
	// is derived from the same read as the radius increase count, so the
	// check after the sample compares against exactly this state (#658).
	readRadius, readIncreases := a.store.RadiusState()
	committedDepth := readRadius + a.store.CapacityDoubling()

	if a.state.IsFrozen() {
		a.logger.Info("skipping round because node is frozen")
		return false, nil
	}

	isPlaying, err := a.contract.IsPlaying(ctx, committedDepth)
	if err != nil {
		a.metrics.ErrCheckIsPlaying.Inc()
		return false, err
	}
	if !isPlaying {
		a.logger.Info("not playing in this round")
		return false, nil
	}
	a.state.SetLastSelectedRound(round + 1)
	a.metrics.NeighborhoodSelected.Inc()
	a.logger.Info("neighbourhood chosen", "round", round)

	// A node that has been evicting for a while sits out the round: a
	// sample would compete with the eviction and miss its deadline, and
	// during a network-wide event neighbours evict at different paces
	// (#649). Read live, not cached per phase: one atomic read.
	if d := a.store.EvictingFor(); d >= storer.EvictingMinAge {
		a.logger.Info("skipping round because node is evicting", "round", round, "evicting_for", d.Round(time.Second))
		a.metrics.SkippedWhileEvicting.Inc()
		return false, nil
	}

	if !a.state.IsFullySynced() {
		a.logger.Info("skipping round because node is not fully synced")
		return false, nil
	}

	if !a.state.IsHealthy() {
		a.logger.Info("skipping round because node is unhealthy", "round", round)
		return false, nil
	}

	_, hasFunds, err := a.HasEnoughFundsToPlay(ctx)
	if err != nil {
		return false, fmt.Errorf("has enough funds to play: %w", err)
	} else if !hasFunds {
		a.logger.Info("insufficient funds to play in next round", "round", round)
		a.metrics.InsufficientFundsToPlay.Inc()
		return false, nil
	}

	now := time.Now()
	sample, err := a.makeSample(ctx, committedDepth)
	if err != nil {
		return false, err
	}
	dur := time.Since(now)
	a.metrics.SampleDuration.Set(dur.Seconds())

	// A radius increase since the depth was read can change the sampled
	// content (unreserve deletes in the bin the range starts at, or the
	// range is clipped away), so the round is not played. The count, not
	// the radius, is compared, so an increase followed by a decrease is
	// caught. A decrease cannot reach the sampled range, so the round is
	// played (#658).
	if radius, increases := a.store.RadiusState(); increases != readIncreases {
		a.logger.Info("skipping round because the storage radius increased since the round's depth was read", "round", round, "sample_depth", committedDepth, "committed_depth", radius+a.store.CapacityDoubling())
		a.metrics.SkippedRadiusIncrease.Inc()
		return false, nil
	} else if radius < readRadius {
		a.logger.Info("radius decreased since the round's depth was read; playing at the depth read", "round", round, "sample_depth", committedDepth, "committed_depth", radius+a.store.CapacityDoubling())
		a.metrics.PlayedAfterDecrease.Inc()
	}

	a.logger.Info("produced sample", "hash", sample.ReserveSampleHash, "radius", committedDepth, "round", round)

	a.state.SetSampleData(round, sample, dur)

	return true, nil
}

// sampleFlightKey keys the sample singleflight by anchor, depth and kind:
// a lottery sample and an /rchash call with the round's salt must not
// merge, because they pause eviction differently (wasp #659).
func sampleFlightKey(ctx context.Context, anchor []byte, depth uint8) string {
	return fmt.Sprintf("%x_%d_%t", anchor, depth, storer.IsLotterySample(ctx))
}

type sampleResult struct {
	Items []storer.SampleItem
	Hash  swarm.Address
}

// reserveSampleAndHash runs getPreviousRoundTime, ReserveSample, and sampleHash
// as a singleflight keyed by anchor, depth and kind to deduplicate concurrent
// calls. The kind keeps a lottery sample from merging with an /rchash call
// that uses the round's salt: they pause eviction differently (wasp #659).
func (a *Agent) reserveSampleAndHash(ctx context.Context, anchor []byte, depth uint8) (sampleResult, error) {
	key := sampleFlightKey(ctx, anchor, depth)

	res, _, err := a.sampleFlight.Do(ctx, key, func(ctx context.Context) (sampleResult, error) {
		timeLimiter, err := a.getPreviousRoundTime(ctx)
		if err != nil {
			return sampleResult{}, err
		}

		// Windowed proof (#273): when the operator has opted into windowed
		// mode, take the windowed sample. The commitment and proofs downstream
		// are built in the identical format, so a windowed-aware contract would
		// accept them; the live contract does not, which is why windowed is
		// experimental and off by default.
		var rSample storer.Sample
		if a.reserveProofMode == storer.ReserveProofModeWindowed {
			rSample, err = a.store.WindowedSample(ctx, anchor, depth, uint64(timeLimiter), a.minBatchBalance())
		} else {
			rSample, err = a.store.ReserveSample(ctx, anchor, depth, uint64(timeLimiter), a.minBatchBalance())
		}
		if err != nil {
			return sampleResult{}, err
		}

		hash, err := sampleHash(rSample.Items)
		if err != nil {
			return sampleResult{}, err
		}

		return sampleResult{Items: rSample.Items, Hash: hash}, nil
	})

	return res, err
}

func (a *Agent) makeSample(ctx context.Context, committedDepth uint8) (SampleData, error) {
	salt, err := a.contract.ReserveSalt(ctx)
	if err != nil {
		return SampleData{}, err
	}

	// The lottery's sample pauses eviction for its whole duration; other
	// samples, such as /rchash, share a bounded budget (wasp #659).
	res, err := a.reserveSampleAndHash(storer.WithLotterySample(ctx), salt, committedDepth)
	if err != nil {
		return SampleData{}, err
	}

	return SampleData{
		Anchor1:            salt,
		ReserveSampleItems: res.Items,
		ReserveSampleHash:  res.Hash,
		StorageRadius:      committedDepth,
	}, nil
}

func (a *Agent) minBatchBalance() *big.Int {
	cs := a.chainStateGetter.GetChainState()
	nextRoundBlockNumber := ((a.state.currentBlock() / a.blocksPerRound) + 2) * a.blocksPerRound
	difference := nextRoundBlockNumber - cs.Block
	minBalance := new(big.Int).Add(cs.TotalAmount, new(big.Int).Mul(cs.CurrentPrice, big.NewInt(int64(difference))))

	return minBalance
}

func (a *Agent) getPreviousRoundTime(ctx context.Context) (time.Duration, error) {
	previousRoundBlockNumber := ((a.state.currentBlock() / a.blocksPerRound) - 1) * a.blocksPerRound

	a.metrics.BackendCalls.Inc()
	timeLimiterBlock, err := a.backend.HeaderByNumber(ctx, new(big.Int).SetUint64(previousRoundBlockNumber))
	if err != nil {
		a.metrics.BackendErrors.Inc()
		return 0, err
	}

	return time.Duration(timeLimiterBlock.Time) * time.Second / time.Nanosecond, nil
}

func (a *Agent) commit(ctx context.Context, sample SampleData, round uint64) error {
	a.metrics.CommitPhase.Inc()

	key := make([]byte, swarm.HashSize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return err
	}

	rsh := sample.ReserveSampleHash.Bytes()
	obfuscatedHash, err := a.wrapCommit(sample.StorageRadius, rsh, key)
	if err != nil {
		return err
	}

	txHash, err := a.contract.Commit(ctx, obfuscatedHash, round)
	if err != nil {
		a.metrics.ErrCommit.Inc()
		return err
	}
	a.state.AddFee(ctx, txHash)

	a.state.SetCommitKey(round, key)

	return nil
}

func (a *Agent) Close() error {
	close(a.quit)

	stopped := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("stopping incentives with ongoing worker goroutine")
	}
}

func (a *Agent) wrapCommit(storageRadius uint8, sample []byte, key []byte) ([]byte, error) {
	storageRadiusByte := []byte{storageRadius}

	data := append(a.overlay.Bytes(), storageRadiusByte...)
	data = append(data, sample...)
	data = append(data, key...)

	return crypto.LegacyKeccak256(data)
}

// Status returns the node status
func (a *Agent) Status() (*Status, error) {
	return a.state.Status()
}

type SampleWithProofs struct {
	Hash     swarm.Address                       `json:"hash"`
	Proofs   redistribution.ChunkInclusionProofs `json:"proofs"`
	Duration time.Duration                       `json:"duration"`
}

// SampleWithProofs is called only by rchash API
func (a *Agent) SampleWithProofs(
	ctx context.Context,
	anchor1 []byte,
	anchor2 []byte,
	storageRadius uint8,
) (SampleWithProofs, error) {
	sampleStartTime := time.Now()

	res, err := a.reserveSampleAndHash(ctx, anchor1, storageRadius)
	if err != nil {
		return SampleWithProofs{}, err
	}

	proofs, err := makeInclusionProofs(res.Items, anchor1, anchor2)
	if err != nil {
		return SampleWithProofs{}, fmt.Errorf("make proofs: %w", err)
	}

	return SampleWithProofs{
		Hash:     res.Hash,
		Proofs:   proofs,
		Duration: time.Since(sampleStartTime),
	}, nil
}

func (a *Agent) HasEnoughFundsToPlay(ctx context.Context) (*big.Int, bool, error) {
	balance, err := a.backend.BalanceAt(ctx, a.state.ethAddress, nil)
	if err != nil {
		return nil, false, err
	}

	price, _, err := a.backend.SuggestedFeeAndTip(ctx, nil, redistribution.BoostTipPercent)
	if err != nil {
		return nil, false, err
	}

	avgTxFee := new(big.Int).Mul(big.NewInt(avgTxGas), price)
	minBalance := new(big.Int).Mul(avgTxFee, big.NewInt(minTxCountToCover))

	return minBalance, balance.Cmp(minBalance) >= 1, nil
}
