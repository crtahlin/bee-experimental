// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	"resenje.org/singleflight"
)

const (
	// syncMaxDelay is how far the chain backend's latest block may lag
	// the wall clock for the backend to count as synced; the node's chain
	// sync wait uses the same bound (pkg/node/chain.go).
	syncMaxDelay = time.Minute

	// revealReplaceAfterBlocks is how many blocks after it was listed a
	// reveal that is still pending, or not found, is replaced with raised
	// fees instead of waited for or rebroadcast unchanged.
	revealReplaceAfterBlocks = 6

	// commitSettleBlocks is how many blocks past the start of the reveal
	// phase the backend's head must be before a "not found" or
	// "cancelled" commit counts as not on chain. The contract accepts
	// commits only in the commit phase, so a commit that a head this far
	// into the reveal phase does not know never landed; a lagging head
	// is checked again at the next block. The answer must also hold at
	// two consecutive heads, because the commit's status and the head come
	// from separate calls, which can reach different chain nodes.
	commitSettleBlocks = 2

	// stopWaitMargin is added to WaitBudget to bound the stop wait, so a
	// wait whose block number reads keep failing still ends.
	stopWaitMargin = 30 * time.Second
)

var (
	errCommitRefused    = errors.New("node is stopping, commit refused")
	errAlreadyCommitted = errors.New("already committed in this round")
	errRevealReverted   = errors.New("reveal reverted")
)

// RevealBackend is the part of the chain backend the Revealer reads.
type RevealBackend interface {
	BlockNumber(context.Context) (uint64, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
}

// RevealGuard lets a stop wait for a pending reveal.
type RevealGuard interface {
	// Pending reports a round with a stored commit key, or a commit in
	// progress, whose reveal has not been sent and whose reveal phase
	// has not ended at the last block seen; deadlineBlock is the first
	// block of the round's claim phase.
	Pending() (round, deadlineBlock uint64, ok bool)
	// Wait sends the pending reveal as soon as the reveal phase opens,
	// retrying every block, and returns once it is mined, the claim phase
	// starts, or ctx is done.
	Wait(ctx context.Context) error
}

// Revealer owns the node's storage-lottery reveals: the agent's reveal
// phase, the stop wait and the early reveal after a restart all go through
// it, so a round is revealed once. It also gates commits, so that a stop
// either refuses a commit or sees it in Pending.
type Revealer struct {
	state          *RedistributionState
	contract       redistribution.Contract
	txService      transaction.Service
	backend        RevealBackend
	blockTime      func() time.Duration
	blocksPerRound uint64
	blocksPerPhase uint64
	logger         log.Logger

	flight singleflight.Group[uint64, bool]
	// stepMu keeps a step that singleflight has given up on (all its
	// callers left) from overlapping the next one.
	stepMu sync.Mutex

	commitGate   sync.Mutex
	shuttingDown bool
	inProgress   map[uint64]struct{} // rounds with a commit being sent

	// rounds that had a commit key when the node started: a reveal for
	// one of them counts as a reveal after a restart
	startupRounds map[uint64]struct{}

	// commitGoneAt is the head at which a round's commit was last seen
	// missing from a settled head; a second such answer at a later head
	// removes the key. Guarded by stepMu.
	commitGoneAt map[uint64]uint64

	earlyCancel context.CancelFunc
	wg          sync.WaitGroup
}

var _ RevealGuard = (*Revealer)(nil)

// NewRevealer builds the node's Revealer on the shared state.
func NewRevealer(
	state *RedistributionState,
	contract redistribution.Contract,
	txService transaction.Service,
	backend RevealBackend,
	blockTime func() time.Duration,
	blocksPerRound, blocksPerPhase uint64,
	logger log.Logger,
) *Revealer {
	r := &Revealer{
		state:          state,
		contract:       contract,
		txService:      txService,
		backend:        backend,
		blockTime:      blockTime,
		blocksPerRound: blocksPerRound,
		blocksPerPhase: blocksPerPhase,
		logger:         logger.WithName(loggerName).Register(),
		inProgress:     make(map[uint64]struct{}),
		startupRounds:  make(map[uint64]struct{}),
		commitGoneAt:   make(map[uint64]uint64),
		earlyCancel:    func() {},
	}

	state.mtx.Lock()
	for round, rd := range state.status.RoundData {
		if rd.CommitKey != nil && !rd.HasRevealed {
			r.startupRounds[round] = struct{}{}
		}
	}
	state.mtx.Unlock()

	return r
}

// State returns the shared redistribution state.
func (r *Revealer) State() *RedistributionState {
	return r.state
}

func (r *Revealer) revealStart(round uint64) uint64 {
	return round*r.blocksPerRound + r.blocksPerPhase
}

func (r *Revealer) claimStart(round uint64) uint64 {
	return round*r.blocksPerRound + 2*r.blocksPerPhase
}

// beginCommit stores the commit key before the commit is sent and marks
// the round's commit in progress, unless the node is stopping or the round
// already has a key.
func (r *Revealer) beginCommit(round uint64, key []byte) error {
	r.commitGate.Lock()
	defer r.commitGate.Unlock()

	if r.shuttingDown {
		return errCommitRefused
	}
	if _, exists := r.state.CommitKey(round); exists {
		return errAlreadyCommitted
	}
	if err := r.state.SetCommitKey(round, key); err != nil {
		return fmt.Errorf("storing commit key: %w", err)
	}
	r.inProgress[round] = struct{}{}
	return nil
}

// endCommit settles the commit key after the commit's send and receipt
// wait: removed when the commit cannot have been broadcast or reverted,
// kept otherwise.
func (r *Revealer) endCommit(round uint64, err error) {
	r.commitGate.Lock()
	defer r.commitGate.Unlock()

	if err != nil && (errors.Is(err, transaction.ErrNotBroadcast) || errors.Is(err, transaction.ErrTransactionReverted)) {
		if rmErr := r.state.RemoveCommitKey(round); rmErr != nil {
			r.logger.Error(rmErr, "removing commit key", "round", round)
		}
	}
	delete(r.inProgress, round)
}

// BeginShutdown refuses commits from now on.
func (r *Revealer) BeginShutdown() {
	r.commitGate.Lock()
	defer r.commitGate.Unlock()
	r.shuttingDown = true
}

// Pending implements RevealGuard.
func (r *Revealer) Pending() (round, deadlineBlock uint64, ok bool) {
	r.commitGate.Lock()
	defer r.commitGate.Unlock()

	block, _ := r.state.lastBlock()
	round = block / r.blocksPerRound
	deadlineBlock = r.claimStart(round)
	if block >= deadlineBlock {
		return 0, 0, false
	}
	_, inProgress := r.inProgress[round]
	_, hasKey := r.state.CommitKey(round)
	if (hasKey || inProgress) && !r.state.HasRevealed(round) {
		return round, deadlineBlock, true
	}
	return 0, 0, false
}

// WaitBudget is how long a pending reveal can still take, from the last
// block seen to the end of its reveal phase; 0 when none is pending.
func (r *Revealer) WaitBudget() time.Duration {
	_, deadline, ok := r.Pending()
	if !ok {
		return 0
	}
	block, _ := r.state.lastBlock()
	return time.Duration(deadline-block+1) * r.blockTime()
}

// Wait implements RevealGuard.
func (r *Revealer) Wait(ctx context.Context) error {
	round, deadline, ok := r.Pending()
	if !ok {
		return nil
	}

	// One signal never cancels ctx, so the wait is bounded by the time
	// left in the reveal phase: it ends even if block reads keep failing.
	limit := r.WaitBudget() + stopWaitMargin
	waitCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()

	if block, err := r.backend.BlockNumber(waitCtx); err == nil && block >= deadline {
		// the last block seen is stale: the phase is over already
		return nil
	}

	r.logger.Warning("stop waits for the storage-lottery reveal", "round", round, "up_to_block", deadline, "at_most", limit)

	revealed := r.revealUntil(waitCtx, round)
	if !revealed && ctx.Err() == nil && errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
		r.logger.Warning("stop: the storage-lottery reveal wait reached its time limit", "round", round, "limit", limit)
	}
	if revealed {
		r.logger.Info("stop: storage-lottery reveal mined, stopping", "round", round)
		r.state.countStop(1, 0, 0)
	} else {
		r.logger.Info("stop: storage-lottery reveal missed, stopping", "round", round)
		r.state.countStop(1, 1, 0)
	}
	return ctx.Err()
}

// Close stops the early reveal and waits for it.
func (r *Revealer) Close() error {
	r.earlyCancel()
	r.wg.Wait()
	return nil
}

// revealUntil sends the round's reveal once its reveal phase opens and
// retries every block until it is mined, the claim phase starts or ctx is
// done. It reports whether the round is revealed.
func (r *Revealer) revealUntil(ctx context.Context, round uint64) bool {
	for {
		if r.state.HasRevealed(round) {
			return true
		}

		block, err := r.backend.BlockNumber(ctx)
		switch {
		case err != nil:
			r.logger.Debug("reveal: reading block number", "error", err)
		case block >= r.claimStart(round):
			return r.state.HasRevealed(round)
		case block >= r.revealStart(round):
			// a receipt wait ends at the claim phase
			stepCtx, cancel := context.WithTimeout(ctx, time.Duration(r.claimStart(round)-block)*r.blockTime())
			done, err := r.Reveal(stepCtx, round)
			cancel()
			if err != nil {
				r.logger.Error(err, "reveal failed, retrying at the next block", "round", round)
			}
			if done {
				return r.state.HasRevealed(round)
			}
		}

		select {
		case <-ctx.Done():
			return r.state.HasRevealed(round)
		case <-time.After(r.blockTime()):
		}
	}
}

// Reveal takes one step towards the round's reveal, at most one at a time
// per round. done is true when nothing more is to be done in the round:
// revealed, nothing to reveal, or a definite failure.
func (r *Revealer) Reveal(ctx context.Context, round uint64) (done bool, err error) {
	done, _, err = r.flight.Do(ctx, round, func(ctx context.Context) (bool, error) {
		return r.revealStep(ctx, round)
	})
	return done, err
}

func (r *Revealer) revealStep(ctx context.Context, round uint64) (bool, error) {
	r.stepMu.Lock()
	defer r.stepMu.Unlock()

	if r.state.HasRevealed(round) {
		return true, nil
	}
	key, ok := r.state.CommitKey(round)
	if !ok {
		return true, nil
	}
	sample, ok := r.state.SampleData(round - 1)
	if !ok {
		return true, errors.New("sample not found in reveal phase")
	}

	block, err := r.backend.BlockNumber(ctx)
	if err != nil {
		return false, err
	}

	txs, sentBlock := r.state.RevealTxs(round)

	// any listed reveal mined: done, whichever it is
	for _, txHash := range txs {
		if st, err := r.txService.TransactionStatus(ctx, txHash); err == nil && st == transaction.TxMined {
			r.revealed(ctx, round, txHash)
			return true, nil
		}
	}

	listRevealTx := func(txHash common.Hash) error {
		return r.state.AddRevealTx(round, txHash, block)
	}

	if len(txs) > 0 {
		newest := txs[len(txs)-1]
		st, err := r.txService.TransactionStatus(ctx, newest)
		if err != nil {
			return false, err
		}
		aged := block >= sentBlock+revealReplaceAfterBlocks
		switch st {
		case transaction.TxMined:
			r.revealed(ctx, round, newest)
			return true, nil
		case transaction.TxReverted:
			return true, fmt.Errorf("%w: %s", errRevealReverted, newest)
		case transaction.TxPending:
			if !aged && !r.feeTooLow(ctx, newest) {
				return false, nil
			}
			r.logger.Info("reveal: replacing a pending reveal with raised fees", "round", round, "tx", newest)
			_, err := r.txService.ReplaceTransaction(ctx, newest, listRevealTx)
			return false, err
		case transaction.TxNotFound:
			if !r.synced(ctx) {
				return false, nil
			}
			if aged || r.feeTooLow(ctx, newest) {
				r.logger.Info("reveal: replacing a lost reveal with raised fees", "round", round, "tx", newest)
				_, err := r.txService.ReplaceTransaction(ctx, newest, listRevealTx)
				return false, err
			}
			r.logger.Info("reveal: rebroadcasting a lost reveal", "round", round, "tx", newest)
			return false, r.txService.RebroadcastTransaction(ctx, newest)
		case transaction.TxCancelled:
			if !r.synced(ctx) {
				return false, nil
			}
			r.logger.Info("reveal: the reveal's nonce was used by another transaction, sending a new one", "round", round, "tx", newest)
		}
	} else {
		switch r.commitOnChain(ctx, round) {
		case commitGone:
			r.logger.Info("reveal: the round's commit reverted or never landed; not revealing", "round", round)
			if err := r.state.RemoveCommitKey(round); err != nil {
				r.logger.Error(err, "removing commit key", "round", round)
			}
			return true, nil
		case commitRecheck:
			r.logger.Debug("reveal: commit not seen by a head close to the reveal phase start; checking again at the next block", "round", round)
			return false, nil
		}
	}

	txHash, err := r.contract.Reveal(ctx, sample.StorageRadius, sample.ReserveSampleHash.Bytes(), key, listRevealTx)
	if err != nil {
		if errors.Is(err, transaction.ErrTransactionReverted) {
			return true, err
		}
		return false, err
	}
	r.revealed(ctx, round, txHash)
	return true, nil
}

func (r *Revealer) revealed(ctx context.Context, round uint64, txHash common.Hash) {
	r.state.SetHasRevealed(round)
	r.state.AddFee(ctx, txHash)
	if _, ok := r.startupRounds[round]; ok {
		r.state.countStop(0, 0, 1)
	}
}

type commitCheck int

const (
	commitPossible commitCheck = iota // reveal
	commitGone                        // definitely not on chain
	commitRecheck                     // not seen yet; check at the next block
)

// commitOnChain checks the round's commit before its first reveal. It is
// gone when its receipt reverted, or when a synced backend whose head is
// commitSettleBlocks into the reveal phase does not know it (or reports
// its nonce used by another transaction) at two consecutive heads. The
// same answer from a head closer to the phase start, or the first such
// answer, is checked again at the next block. A lookup error, an unsynced
// backend or an unknown commit hash all mean "possibly committed", and
// clear an earlier "missing" answer.
func (r *Revealer) commitOnChain(ctx context.Context, round uint64) commitCheck {
	commitTx := r.state.CommitTx(round)
	if commitTx == (common.Hash{}) {
		return commitPossible
	}
	st, err := r.txService.TransactionStatus(ctx, commitTx)
	if err != nil {
		delete(r.commitGoneAt, round)
		return commitPossible
	}
	switch st {
	case transaction.TxReverted:
		delete(r.commitGoneAt, round)
		return commitGone
	case transaction.TxNotFound, transaction.TxCancelled:
		header, err := r.backend.HeaderByNumber(ctx, nil)
		if err != nil || !headerFresh(header) {
			delete(r.commitGoneAt, round)
			return commitPossible
		}
		if header.Number == nil || header.Number.Uint64() < r.revealStart(round)+commitSettleBlocks {
			return commitRecheck
		}
		head := header.Number.Uint64()
		if first, ok := r.commitGoneAt[round]; ok && head > first {
			delete(r.commitGoneAt, round)
			return commitGone
		}
		if _, ok := r.commitGoneAt[round]; !ok {
			r.commitGoneAt[round] = head
		}
		return commitRecheck
	}
	delete(r.commitGoneAt, round)
	return commitPossible
}

// feeTooLow reports whether the stored fee cap of the transaction is below
// the latest block's base fee, so it cannot be mined as it is.
func (r *Revealer) feeTooLow(ctx context.Context, txHash common.Hash) bool {
	stored, err := r.txService.StoredTransaction(txHash)
	if err != nil || stored.GasFeeCap == nil {
		return false
	}
	header, err := r.backend.HeaderByNumber(ctx, nil)
	if err != nil || header.BaseFee == nil {
		return false
	}
	return stored.GasFeeCap.Cmp(header.BaseFee) < 0
}

// synced reports whether the chain backend's latest block is recent, as
// transaction.IsSynced does.
func (r *Revealer) synced(ctx context.Context) bool {
	header, err := r.backend.HeaderByNumber(ctx, nil)
	if err != nil {
		return false
	}
	return headerFresh(header)
}

func headerFresh(header *types.Header) bool {
	return header != nil && time.Unix(int64(header.Time), 0).After(time.Now().Add(-syncMaxDelay))
}

// StartEarlyReveal sends, in the background, a reveal that is pending from
// before a restart, before the rest of the node is up. It never commits.
// Close stops it.
func (r *Revealer) StartEarlyReveal() {
	ctx, cancel := context.WithCancel(context.Background())
	r.earlyCancel = cancel
	r.wg.Go(func() {
		r.earlyReveal(ctx)
	})
}

func (r *Revealer) earlyReveal(ctx context.Context) {
	for {
		if r.synced(ctx) {
			block, err := r.backend.BlockNumber(ctx)
			if err == nil {
				round := block / r.blocksPerRound
				if _, ok := r.state.CommitKey(round); !ok || r.state.HasRevealed(round) || block >= r.claimStart(round) {
					return
				}
				r.logger.Info("revealing early after a restart", "round", round)
				r.revealUntil(ctx, round)
				return
			}
		} else if !r.estimatedPending() {
			r.logger.Debug("early reveal: chain backend not synced and no reveal can be pending; giving up")
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(r.blockTime()):
		}
	}
}

// estimatedPending estimates, from the last block seen and when it was
// seen, whether the reveal phase of the round with a stored key can still
// be open.
func (r *Revealer) estimatedPending() bool {
	block, seenAt := r.state.lastBlock()
	if seenAt.Unix() <= 0 {
		return false
	}
	round := block / r.blocksPerRound
	if _, ok := r.state.CommitKey(round); !ok || r.state.HasRevealed(round) {
		return false
	}
	bt := r.blockTime()
	if bt <= 0 {
		return false
	}
	estimate := block + uint64(time.Since(seenAt)/bt)
	return estimate < r.claimStart(round)
}
