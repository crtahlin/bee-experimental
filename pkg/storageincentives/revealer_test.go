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
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	erc20mock "github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20/mock"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
)

const (
	testBlocksPerRound = 12
	testBlocksPerPhase = 4 // commit 0-3, reveal 4-7, claim 8-11
	testBlockTime      = time.Second
)

// fakeChain is a chain whose block number follows the (synctest) clock.
type fakeChain struct {
	mu        sync.Mutex
	start     time.Time
	baseBlock uint64
	lag       time.Duration // how far the latest block's time lags the clock
	headLag   uint64        // how many blocks the latest header lags block()
	failBlock bool          // BlockNumber fails
	baseFee   *big.Int
}

func newFakeChain(block uint64) *fakeChain {
	return &fakeChain{start: time.Now(), baseBlock: block, baseFee: big.NewInt(1)}
}

func (c *fakeChain) block() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.baseBlock + uint64(time.Since(c.start)/testBlockTime)
}

func (c *fakeChain) BlockNumber(context.Context) (uint64, error) {
	c.mu.Lock()
	fail := c.failBlock
	c.mu.Unlock()
	if fail {
		return 0, errors.New("block number unavailable")
	}
	return c.block(), nil
}

func (c *fakeChain) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	number := c.block()
	c.mu.Lock()
	defer c.mu.Unlock()
	return &types.Header{
		Number:  new(big.Int).SetUint64(number - min(c.headLag, number)),
		Time:    uint64(time.Now().Add(-c.lag).Unix()),
		BaseFee: c.baseFee,
	}, nil
}

func (c *fakeChain) setHeadLag(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headLag = n
}

func (c *fakeChain) setFailBlock(fail bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failBlock = fail
}

func (c *fakeChain) setLag(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lag = d
}

// fakeTxs is the chain's view of transactions, behind a transaction mock.
type fakeTxs struct {
	mu          sync.Mutex
	states      map[common.Hash]transaction.TxState
	feeCaps     map[common.Hash]*big.Int
	lookupErr   error
	rebroadcast []common.Hash
	replaced    []common.Hash
	next        byte
	onReplace   func(newHash common.Hash) // called between hook and "send"
}

func newFakeTxs() *fakeTxs {
	return &fakeTxs{states: make(map[common.Hash]transaction.TxState), feeCaps: make(map[common.Hash]*big.Int)}
}

func (f *fakeTxs) newHash() common.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	return common.Hash{0xaa, f.next}
}

func (f *fakeTxs) set(h common.Hash, st transaction.TxState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[h] = st
}

func (f *fakeTxs) service() transaction.Service {
	return transactionmock.New(
		transactionmock.WithTransactionStatusFunc(func(ctx context.Context, h common.Hash) (transaction.TxState, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lookupErr != nil {
				return 0, f.lookupErr
			}
			st, ok := f.states[h]
			if !ok {
				return transaction.TxNotFound, nil
			}
			return st, nil
		}),
		transactionmock.WithRebroadcastTransactionFunc(func(ctx context.Context, h common.Hash) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.rebroadcast = append(f.rebroadcast, h)
			return nil
		}),
		transactionmock.WithReplaceTransactionFunc(func(ctx context.Context, h common.Hash, hook transaction.BeforeBroadcastFunc) (common.Hash, error) {
			n := f.newHash()
			if err := hook(n); err != nil {
				return common.Hash{}, err
			}
			if f.onReplace != nil {
				f.onReplace(n)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			f.replaced = append(f.replaced, n)
			f.states[n] = transaction.TxPending
			return n, nil
		}),
		transactionmock.WithStoredTransactionFunc(func(h common.Hash) (*transaction.StoredTransaction, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			fc, ok := f.feeCaps[h]
			if !ok {
				fc = big.NewInt(100)
			}
			return &transaction.StoredTransaction{GasFeeCap: fc}, nil
		}),
	)
}

// fakeContract sends commits and reveals: it calls the hook with a fresh
// hash, then returns what the test sets.
type fakeContract struct {
	redistribution.Contract
	txs   *fakeTxs
	chain *fakeChain

	mu           sync.Mutex
	attemptAt    []uint64          // block of every reveal attempt
	revealPre    func(n int) error // fails attempt n before the hook
	commits      int
	reveals      int
	revealKeys   [][]byte
	commitResult func(ctx context.Context, h common.Hash) error
	revealResult func(ctx context.Context, h common.Hash) error
}

func (c *fakeContract) Commit(ctx context.Context, _ []byte, _ uint64, hook transaction.BeforeBroadcastFunc) (common.Hash, error) {
	h := c.txs.newHash()
	if err := hook(h); err != nil {
		return common.Hash{}, fmt.Errorf("commit: %w: %w", transaction.ErrNotBroadcast, err)
	}
	c.mu.Lock()
	c.commits++
	res := c.commitResult
	c.mu.Unlock()
	if res != nil {
		return h, res(ctx, h)
	}
	c.txs.set(h, transaction.TxMined)
	return h, nil
}

func (c *fakeContract) CurrentRound(context.Context) (uint64, error) {
	return 0, errors.New("not set")
}

func (c *fakeContract) Reveal(ctx context.Context, _ uint8, _ []byte, key []byte, hook transaction.BeforeBroadcastFunc) (common.Hash, error) {
	c.mu.Lock()
	c.attemptAt = append(c.attemptAt, c.chain.block())
	n := len(c.attemptAt)
	pre := c.revealPre
	c.mu.Unlock()
	if pre != nil {
		if err := pre(n); err != nil {
			return common.Hash{}, fmt.Errorf("reveal: %w: %w", transaction.ErrNotBroadcast, err)
		}
	}
	h := c.txs.newHash()
	if err := hook(h); err != nil {
		return common.Hash{}, fmt.Errorf("reveal: %w: %w", transaction.ErrNotBroadcast, err)
	}
	c.mu.Lock()
	c.reveals++
	c.revealKeys = append(c.revealKeys, key)
	res := c.revealResult
	c.mu.Unlock()
	if res != nil {
		if err := res(ctx, h); err != nil {
			return h, err
		}
	}
	c.txs.set(h, transaction.TxMined)
	return h, nil
}

func (c *fakeContract) counts() (commits, reveals int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commits, c.reveals
}

// failingSyncStore fails the first synced write only, so a commit that
// ignored the failed key write would get through its later writes.
type failingSyncStore struct {
	storage.StateStorer
	failed bool
}

func (s *failingSyncStore) PutSync(key string, obj any) error {
	if !s.failed {
		s.failed = true
		return errors.New("disk full")
	}
	return s.StateStorer.PutSync(key, obj)
}

type revealFixture struct {
	store    storage.StateStorer
	chain    *fakeChain
	txs      *fakeTxs
	contract *fakeContract
	revealer *Revealer
	agent    *Agent
}

func newRevealFixture(t *testing.T, store storage.StateStorer, chain *fakeChain, txs *fakeTxs) *revealFixture {
	t.Helper()
	f := &revealFixture{store: store, chain: chain, txs: txs, contract: &fakeContract{txs: txs, chain: chain}}
	f.restart(t)
	return f
}

// restart builds the state and Revealer again from the same state store, as
// a node start does.
func (f *revealFixture) restart(t *testing.T) {
	t.Helper()
	txService := f.txs.service()
	state, err := NewRedistributionState(log.Noop, common.Address{}, f.store, erc20mock.New(), txService)
	if err != nil {
		t.Fatal(err)
	}
	f.revealer = NewRevealer(state, f.contract, txService, f.chain, func() time.Duration { return testBlockTime }, testBlocksPerRound, testBlocksPerPhase, log.Noop)
	f.agent = &Agent{
		logger:   log.Noop,
		metrics:  newMetrics(),
		contract: f.contract,
		revealer: f.revealer,
		state:    state,
		overlay:  swarm.RandAddress(t),
	}
}

func (f *revealFixture) state() *RedistributionState { return f.revealer.state }

func (f *revealFixture) setSample(round uint64) {
	f.state().SetSampleData(round-1, SampleData{ReserveSampleHash: swarm.NewAddress(make([]byte, swarm.HashSize)), StorageRadius: 1}, 0)
}

// The key is stored before the commit is sent: a stop while the commit's
// receipt is awaited, then a restart, still reveals with that key (test 1).
// The reveal phase cancelling a commit whose receipt has not come keeps
// the key, and the reveal is sent (test 2).
func TestCommitKeyStoredBeforeSend(t *testing.T) {
	t.Parallel()

	for _, restart := range []bool{true, false} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(13), newFakeTxs())
				const round = 1
				f.setSample(round)

				f.contract.commitResult = func(ctx context.Context, h common.Hash) error {
					f.txs.set(h, transaction.TxPending) // broadcast, receipt not in yet
					<-ctx.Done()
					return ctx.Err()
				}
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					synctest.Wait()
					cancel() // stop, or the reveal phase cancelling the commit
				}()
				sample, _ := f.state().SampleData(round - 1)
				if err := f.agent.commit(ctx, sample, round); !errors.Is(err, context.Canceled) {
					t.Fatalf("commit: got %v", err)
				}
				key, ok := f.state().CommitKey(round)
				if !ok {
					t.Fatal("commit key lost")
				}
				if f.state().CommitTx(round) == (common.Hash{}) {
					t.Fatal("commit hash not stored")
				}

				if restart {
					f.restart(t)
				}
				if !f.revealer.revealUntil(context.Background(), round) {
					t.Fatal("not revealed")
				}
				if string(f.contract.revealKeys[0]) != string(key) {
					t.Fatal("revealed with another key")
				}
			})
		})
	}
}

// A failed key write sends no commit (test 3).
func TestCommitKeyWriteFails(t *testing.T) {
	t.Parallel()

	f := newRevealFixture(t, &failingSyncStore{StateStorer: mock.NewStateStore()}, newFakeChain(13), newFakeTxs())
	const round = 1
	f.setSample(round)
	sample, _ := f.state().SampleData(round - 1)

	if err := f.agent.commit(context.Background(), sample, round); err == nil {
		t.Fatal("want the key-write error")
	}
	if commits, _ := f.contract.counts(); commits != 0 {
		t.Fatalf("%d commits sent with no stored key", commits)
	}
	if _, ok := f.state().CommitKey(round); ok {
		t.Fatal("key kept in memory after a failed write")
	}
}

// Not broadcast: key removed and not pending; possibly broadcast: key and
// hash kept; reverted: key removed (tests 4 and 17).
func TestCommitErrorsAndKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		keepKey bool
	}{
		{"not broadcast", fmt.Errorf("x: %w", transaction.ErrNotBroadcast), false},
		{"send error", errors.New("connection reset"), true},
		{"receipt wait cancelled", context.Canceled, true},
		{"reverted", fmt.Errorf("commit: %w", transaction.ErrTransactionReverted), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(13), newFakeTxs())
			const round = 1
			f.setSample(round)
			f.state().SetCurrentBlock(13)
			f.contract.commitResult = func(context.Context, common.Hash) error { return tc.err }
			sample, _ := f.state().SampleData(round - 1)

			_ = f.agent.commit(context.Background(), sample, round)

			_, hasKey := f.state().CommitKey(round)
			if hasKey != tc.keepKey {
				t.Fatalf("key kept %v, want %v", hasKey, tc.keepKey)
			}
			if tc.keepKey && f.state().CommitTx(round) == (common.Hash{}) {
				t.Fatal("commit hash not kept")
			}
			if _, _, pending := f.revealer.Pending(); pending != tc.keepKey {
				t.Fatalf("pending %v, want %v", pending, tc.keepKey)
			}
		})
	}
}

// A stored key blocks a second commit in the round, also after a restart
// (test 5).
func TestNoSecondCommit(t *testing.T) {
	t.Parallel()

	f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(13), newFakeTxs())
	const round = 1
	f.setSample(round)
	f.contract.commitResult = func(context.Context, common.Hash) error { return errors.New("connection reset") }
	sample, _ := f.state().SampleData(round - 1)
	_ = f.agent.commit(context.Background(), sample, round)

	f.restart(t)
	f.contract.commitResult = nil
	if err := f.agent.handleCommit(context.Background(), round); err != nil {
		t.Fatal(err)
	}
	if commits, _ := f.contract.counts(); commits != 1 {
		t.Fatalf("%d commits, want 1", commits)
	}
}

// commitAndAdvance stores a key with a mined commit for round 1 and moves
// the chain into its reveal phase.
func revealFixtureInRevealPhase(t *testing.T) *revealFixture {
	t.Helper()
	f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(17), newFakeTxs())
	f.setSample(1)
	if err := f.state().SetCommitKey(1, []byte("key")); err != nil {
		t.Fatal(err)
	}
	h := f.txs.newHash()
	f.txs.set(h, transaction.TxMined)
	if err := f.state().SetCommitTx(1, h); err != nil {
		t.Fatal(err)
	}
	return f
}

// The early path and the agent race on a round: one reveal, and HasRevealed
// survives the agent's SetCurrentBlock on the shared state (test 11).
func TestOneReveal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := revealFixtureInRevealPhase(t)
		release := make(chan struct{})
		f.contract.revealResult = func(context.Context, common.Hash) error { <-release; return nil }

		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() { f.revealer.revealUntil(context.Background(), 1) })
		}
		synctest.Wait()
		close(release)
		wg.Wait()

		if _, reveals := f.contract.counts(); reveals != 1 {
			t.Fatalf("%d reveals, want 1", reveals)
		}

		f.agent.state.SetCurrentBlock(18)
		f.restart(t)
		if !f.state().HasRevealed(1) {
			t.Fatal("HasRevealed lost")
		}
	})
}

// A listed reveal still pending: after a restart nothing is sent (test 15).
func TestRevealNotResentWhilePending(t *testing.T) {
	t.Parallel()

	f := revealFixtureInRevealPhase(t)
	h := f.txs.newHash()
	f.txs.set(h, transaction.TxPending)
	if err := f.state().AddRevealTx(1, h, 17); err != nil {
		t.Fatal(err)
	}
	f.restart(t)

	done, err := f.revealer.Reveal(context.Background(), 1)
	if err != nil || done {
		t.Fatalf("got done %v err %v, want a wait", done, err)
	}
	if _, reveals := f.contract.counts(); reveals != 0 {
		t.Fatal("a reveal was sent while a listed one is pending")
	}
	if len(f.txs.rebroadcast)+len(f.txs.replaced) != 0 {
		t.Fatal("a pending reveal was rebroadcast or replaced")
	}
}

// The reveal is skipped only on a definite answer about the commit
// (test 16).
func TestRevealSkipOnlyWhenDefinite(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		state  transaction.TxState
		err    error
		lag    time.Duration
		reveal bool
	}{
		{"reverted", transaction.TxReverted, nil, 0, false},
		{"not found, synced", transaction.TxNotFound, nil, 0, false},
		{"not found, not synced", transaction.TxNotFound, nil, time.Hour, true},
		{"lookup error", 0, errors.New("rpc down"), 0, true},
		{"pending", transaction.TxPending, nil, 0, true},
		{"mined", transaction.TxMined, nil, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := revealFixtureInRevealPhase(t)
			f.txs.set(f.state().CommitTx(1), tc.state)
			f.chain.setLag(tc.lag)
			if tc.err != nil {
				// the lookup fails for the commit only
				commitTx := f.state().CommitTx(1)
				f.revealer.txService = transactionmock.New(transactionmock.WithTransactionStatusFunc(func(ctx context.Context, h common.Hash) (transaction.TxState, error) {
					if h == commitTx {
						return 0, tc.err
					}
					return transaction.TxNotFound, nil
				}))
			}

			_, _ = f.revealer.Reveal(context.Background(), 1)

			if _, reveals := f.contract.counts(); (reveals == 1) != tc.reveal {
				t.Fatalf("reveals %d, want reveal %v", reveals, tc.reveal)
			}
		})
	}
}

// A listed reveal not found on a synced backend is rebroadcast unchanged;
// with its fee below the base fee it is replaced, and the replacement is
// listed before it is sent (test 19).
func TestRevealRebroadcastAndReplace(t *testing.T) {
	t.Parallel()

	t.Run("rebroadcast", func(t *testing.T) {
		t.Parallel()
		f := revealFixtureInRevealPhase(t)
		h := f.txs.newHash() // not known to the chain
		if err := f.state().AddRevealTx(1, h, 17); err != nil {
			t.Fatal(err)
		}
		if done, err := f.revealer.Reveal(context.Background(), 1); done || err != nil {
			t.Fatalf("done %v err %v", done, err)
		}
		if len(f.txs.rebroadcast) != 1 || f.txs.rebroadcast[0] != h {
			t.Fatalf("rebroadcast %v, want [%s]", f.txs.rebroadcast, h)
		}
		if _, reveals := f.contract.counts(); reveals != 0 || len(f.txs.replaced) != 0 {
			t.Fatal("re-signed instead of rebroadcast")
		}
	})

	t.Run("not synced waits", func(t *testing.T) {
		t.Parallel()
		f := revealFixtureInRevealPhase(t)
		f.chain.setLag(time.Hour)
		h := f.txs.newHash()
		if err := f.state().AddRevealTx(1, h, 17); err != nil {
			t.Fatal(err)
		}
		_, _ = f.revealer.Reveal(context.Background(), 1)
		if len(f.txs.rebroadcast)+len(f.txs.replaced) != 0 {
			t.Fatal("acted on not-found from an unsynced backend")
		}
	})

	t.Run("replace", func(t *testing.T) {
		t.Parallel()
		f := revealFixtureInRevealPhase(t)
		h := f.txs.newHash()
		f.txs.feeCaps[h] = big.NewInt(1)
		f.chain.baseFee = big.NewInt(1000)
		if err := f.state().AddRevealTx(1, h, 17); err != nil {
			t.Fatal(err)
		}
		listedBeforeSend := false
		f.txs.onReplace = func(n common.Hash) {
			// read back what a restart would see
			st, err := f.state().Status()
			if err != nil {
				t.Error(err)
				return
			}
			txs := st.RoundData[1].RevealTxs
			listedBeforeSend = len(txs) == 2 && txs[1] == n
		}
		if done, err := f.revealer.Reveal(context.Background(), 1); done || err != nil {
			t.Fatalf("done %v err %v", done, err)
		}
		if len(f.txs.replaced) != 1 || !listedBeforeSend {
			t.Fatalf("replaced %v, listed before send %v", f.txs.replaced, listedBeforeSend)
		}
	})

	t.Run("pending too long", func(t *testing.T) {
		t.Parallel()
		f := revealFixtureInRevealPhase(t)
		h := f.txs.newHash()
		f.txs.set(h, transaction.TxPending)
		if err := f.state().AddRevealTx(1, h, 17-revealReplaceAfterBlocks); err != nil {
			t.Fatal(err)
		}
		_, _ = f.revealer.Reveal(context.Background(), 1)
		if len(f.txs.replaced) != 1 {
			t.Fatalf("replaced %v, want one replacement", f.txs.replaced)
		}
	})
}

// A cancelled reveal gets a fresh send, listed (test 20).
func TestRevealCancelledSendsNew(t *testing.T) {
	t.Parallel()

	f := revealFixtureInRevealPhase(t)
	h := f.txs.newHash()
	f.txs.set(h, transaction.TxCancelled)
	if err := f.state().AddRevealTx(1, h, 17); err != nil {
		t.Fatal(err)
	}
	if done, err := f.revealer.Reveal(context.Background(), 1); !done || err != nil {
		t.Fatalf("done %v err %v", done, err)
	}
	txs, _ := f.state().RevealTxs(1)
	if _, reveals := f.contract.counts(); reveals != 1 || len(txs) != 2 {
		t.Fatalf("reveals %d, listed %d; want 1 and 2", reveals, len(txs))
	}
}

// An older reveal mined after a re-send sets HasRevealed; nothing more is
// sent (test 21).
func TestRevealOlderHashMined(t *testing.T) {
	t.Parallel()

	f := revealFixtureInRevealPhase(t)
	older, newer := f.txs.newHash(), f.txs.newHash()
	f.txs.set(older, transaction.TxMined)
	if err := f.state().AddRevealTx(1, older, 17); err != nil {
		t.Fatal(err)
	}
	if err := f.state().AddRevealTx(1, newer, 17); err != nil {
		t.Fatal(err)
	}
	if done, err := f.revealer.Reveal(context.Background(), 1); !done || err != nil {
		t.Fatalf("done %v err %v", done, err)
	}
	if !f.state().HasRevealed(1) {
		t.Fatal("HasRevealed not set from the older hash")
	}
	if _, reveals := f.contract.counts(); reveals != 0 || len(f.txs.rebroadcast)+len(f.txs.replaced) != 0 {
		t.Fatal("sent after a listed reveal was mined")
	}
}

// Shutdown racing a commit: the commit is refused, or Pending reports it
// (test 18).
func TestNoWindowBetweenCommitAndShutdown(t *testing.T) {
	t.Parallel()

	for range 200 {
		f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(13), newFakeTxs())
		f.state().SetCurrentBlock(13)
		const round = 1
		f.setSample(round)
		sample, _ := f.state().SampleData(round - 1)

		release := make(chan struct{})
		f.contract.commitResult = func(context.Context, common.Hash) error { <-release; return nil }

		var commitErr error
		committed := make(chan struct{})
		go func() {
			commitErr = f.agent.commit(context.Background(), sample, round)
			close(committed)
		}()

		f.revealer.BeginShutdown()
		_, _, pending := f.revealer.Pending()
		close(release)
		<-committed

		commits, _ := f.contract.counts()
		switch {
		case commits == 1 && !pending:
			t.Fatal("a commit was sent without Pending reporting it")
		case commits == 0 && !errors.Is(commitErr, errSkipCommit):
			t.Fatalf("commit neither sent nor refused: %v", commitErr)
		}
	}
}

// A commit past the gate when the stop starts finishes its send and keeps
// its hash; no new commit starts (test 10).
func TestShutdownDuringCommitSend(t *testing.T) {
	t.Parallel()

	f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(13), newFakeTxs())
	const round = 1
	f.setSample(round)
	f.setSample(round + 1)
	sample, _ := f.state().SampleData(round - 1)

	inSend := make(chan struct{})
	release := make(chan struct{})
	f.contract.commitResult = func(context.Context, common.Hash) error {
		close(inSend)
		<-release
		return nil
	}
	errC := make(chan error, 1)
	go func() { errC <- f.agent.commit(context.Background(), sample, round) }()
	<-inSend
	f.revealer.BeginShutdown()
	close(release)
	if err := <-errC; err != nil {
		t.Fatal(err)
	}
	if f.state().CommitTx(round) == (common.Hash{}) {
		t.Fatal("commit hash not stored")
	}

	f.contract.commitResult = nil
	sample2, _ := f.state().SampleData(round)
	if err := f.agent.commit(context.Background(), sample2, round+1); !errors.Is(err, errSkipCommit) {
		t.Fatalf("new commit after the stop started: %v", err)
	}
	if commits, _ := f.contract.counts(); commits != 1 {
		t.Fatalf("%d commits, want 1", commits)
	}
}

// The stop wait: a commit at block 2 and a stop at block 3; the reveal is
// sent when the reveal phase opens, a failure is retried at the next
// block (test 7).
func TestWaitRevealsAndRetries(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(testBlocksPerRound+2), newFakeTxs())
		const round = 1
		f.setSample(round)
		if err := f.state().SetCommitKey(round, []byte("key")); err != nil {
			t.Fatal(err)
		}
		f.state().SetCurrentBlock(f.chain.block())
		time.Sleep(testBlockTime) // the stop comes at block 3

		f.contract.revealPre = func(n int) error {
			if n == 1 {
				return errors.New("nonce lookup failed")
			}
			return nil
		}

		if err := f.revealer.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		revealStart := uint64(round*testBlocksPerRound + testBlocksPerPhase)
		sentAt := f.contract.attemptAt
		if len(sentAt) != 2 || sentAt[0] != revealStart || sentAt[1] != revealStart+1 {
			t.Fatalf("reveal sent at blocks %v, want [%d %d]", sentAt, revealStart, revealStart+1)
		}
		st, _ := f.state().Status()
		if st.StopWaitedForReveal != 1 || st.RevealMissedOnStop != 0 {
			t.Fatalf("counts waited %d missed %d", st.StopWaitedForReveal, st.RevealMissedOnStop)
		}
	})
}

// A reveal that keeps failing: the wait ends at the claim phase and the
// miss is counted (test 7).
func TestWaitEndsAtClaimPhase(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(testBlocksPerRound+2), newFakeTxs())
		const round = 1
		f.setSample(round)
		if err := f.state().SetCommitKey(round, []byte("key")); err != nil {
			t.Fatal(err)
		}
		f.state().SetCurrentBlock(f.chain.block())
		f.contract.revealResult = func(context.Context, common.Hash) error { return errors.New("endpoint down") }

		if err := f.revealer.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got, want := f.chain.block(), uint64(round*testBlocksPerRound+2*testBlocksPerPhase); got != want {
			t.Fatalf("wait ended at block %d, want %d", got, want)
		}
		st, _ := f.state().Status()
		if st.StopWaitedForReveal != 1 || st.RevealMissedOnStop != 1 {
			t.Fatalf("counts waited %d missed %d, want 1 and 1", st.StopWaitedForReveal, st.RevealMissedOnStop)
		}
		f.restart(t)
		if st, _ := f.state().Status(); st.RevealMissedOnStop != 1 {
			t.Fatal("missed count not persisted")
		}
	})
}

// The wait ends at once when ctx is cancelled (the second signal).
func TestWaitCancelled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(testBlocksPerRound+2), newFakeTxs())
		f.setSample(1)
		if err := f.state().SetCommitKey(1, []byte("key")); err != nil {
			t.Fatal(err)
		}
		f.state().SetCurrentBlock(f.chain.block())

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(testBlockTime / 2)
			cancel()
		}()
		start := time.Now()
		if err := f.revealer.Wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
		if time.Since(start) > testBlockTime {
			t.Fatal("wait did not end at the cancel")
		}
	})
}

// No pending reveal: no wait, nothing counted (test 8).
func TestWaitNothingPending(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(testBlocksPerRound+2), newFakeTxs())
		f.state().SetCurrentBlock(f.chain.block())
		start := time.Now()
		if err := f.revealer.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 0 {
			t.Fatal("waited with nothing pending")
		}
		if st, _ := f.state().Status(); st.StopWaitedForReveal != 0 {
			t.Fatal("counted a wait")
		}
	})
}

// Early reveal after a restart: the backend is not synced at first, then
// is; the reveal is sent; Close leaves no goroutine (test 12, the
// Revealer's part).
func TestEarlyReveal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		chain := newFakeChain(testBlocksPerRound + 2)
		f := newRevealFixture(t, mock.NewStateStore(), chain, newFakeTxs())
		f.setSample(1)
		if err := f.state().SetCommitKey(1, []byte("key")); err != nil {
			t.Fatal(err)
		}
		f.state().SetCurrentBlock(chain.block())
		f.restart(t)

		chain.setLag(time.Hour)
		f.revealer.StartEarlyReveal()
		time.Sleep(3 * testBlockTime)
		synctest.Wait()
		if _, reveals := f.contract.counts(); reveals != 0 {
			t.Fatal("revealed on an unsynced backend")
		}
		chain.setLag(0)
		time.Sleep(testBlocksPerPhase * testBlockTime)
		synctest.Wait()
		if !f.state().HasRevealed(1) {
			t.Fatal("early reveal not sent")
		}
		if st, _ := f.state().Status(); st.RevealAfterRestart != 1 {
			t.Fatalf("revealAfterRestart %d, want 1", st.RevealAfterRestart)
		}
		if err := f.revealer.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

// The early reveal gives up once the estimate from the last block seen is
// past the reveal phase, while the backend stays unsynced.
func TestEarlyRevealGivesUp(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		chain := newFakeChain(testBlocksPerRound + 2)
		f := newRevealFixture(t, mock.NewStateStore(), chain, newFakeTxs())
		f.setSample(1)
		if err := f.state().SetCommitKey(1, []byte("key")); err != nil {
			t.Fatal(err)
		}
		f.state().SetCurrentBlock(chain.block())
		chain.setLag(time.Hour)

		f.revealer.StartEarlyReveal()
		time.Sleep(testBlocksPerRound * testBlockTime)
		done := make(chan struct{})
		go func() { f.revealer.wg.Wait(); close(done) }()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("early reveal still running")
		}
		_ = f.revealer.Close()
	})
}

// A revealed round sends nothing more, even with no listed reveal hash
// (a reveal recorded before the hashes were kept).
func TestRevealSkipsWhenRevealed(t *testing.T) {
	t.Parallel()

	f := revealFixtureInRevealPhase(t)
	f.state().SetHasRevealed(1)
	if done, err := f.revealer.Reveal(context.Background(), 1); !done || err != nil {
		t.Fatalf("done %v err %v", done, err)
	}
	if _, reveals := f.contract.counts(); reveals != 0 {
		t.Fatal("revealed twice")
	}
}

// A stop during an early reveal waits for it instead of cancelling it
// (test 11).
func TestWaitCoversEarlyReveal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := revealFixtureInRevealPhase(t)
		f.state().SetCurrentBlock(f.chain.block())
		f.restart(t)
		release := make(chan struct{})
		f.contract.revealResult = func(context.Context, common.Hash) error { <-release; return nil }

		f.revealer.StartEarlyReveal()
		synctest.Wait() // the early reveal is sending

		waited := make(chan error, 1)
		go func() { waited <- f.revealer.Wait(context.Background()) }()
		synctest.Wait()
		select {
		case <-waited:
			t.Fatal("the stop did not wait for the early reveal")
		default:
		}
		close(release)
		if err := <-waited; err != nil {
			t.Fatal(err)
		}
		if _, reveals := f.contract.counts(); reveals != 1 || !f.state().HasRevealed(1) {
			t.Fatalf("reveals %d, revealed %v", reveals, f.state().HasRevealed(1))
		}
		if st, _ := f.state().Status(); st.RevealMissedOnStop != 0 {
			t.Fatal("counted as missed")
		}
		_ = f.revealer.Close()
	})
}

// Pending is read by the API at any time, while the agent commits: it
// takes commitGate, so the race detector sees no unguarded access.
func TestPendingConcurrentWithCommits(t *testing.T) {
	t.Parallel()

	f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(13), newFakeTxs())
	f.state().SetCurrentBlock(13)
	f.contract.commitResult = func(context.Context, common.Hash) error {
		return fmt.Errorf("x: %w", transaction.ErrNotBroadcast) // clears the mark
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for round := uint64(1); round <= 200; round++ {
			_ = f.revealer.beginCommit(round, []byte("key"))
			f.revealer.endCommit(round, transaction.ErrNotBroadcast)
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
			_, _, _ = f.revealer.Pending()
		}
	}
}

// A commit that is not found, or whose nonce reads as used by another
// transaction, counts as never landed only from a head commitSettleBlocks
// into the reveal phase. A head closer to the phase start keeps the key
// and checks again at the next block.
func TestCommitMissingNeedsSettledHead(t *testing.T) {
	t.Parallel()

	revealStart := uint64(testBlocksPerRound + testBlocksPerPhase)
	for _, tc := range []struct {
		name    string
		block   uint64
		headLag uint64
		state   transaction.TxState
		gone    bool
	}{
		{"not found, head settled", revealStart + commitSettleBlocks, 0, transaction.TxNotFound, true},
		{"cancelled, head settled", revealStart + commitSettleBlocks, 0, transaction.TxCancelled, true},
		{"not found, head at the phase start", revealStart, 0, transaction.TxNotFound, false},
		{"not found, head lags the block number", revealStart + 3, 3, transaction.TxNotFound, false},
		{"cancelled, head lags the block number", revealStart + 3, 3, transaction.TxCancelled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(tc.block), newFakeTxs())
				const round = 1
				f.setSample(round)
				if err := f.state().SetCommitKey(round, []byte("key")); err != nil {
					t.Fatal(err)
				}
				commitTx := f.txs.newHash()
				if err := f.state().SetCommitTx(round, commitTx); err != nil {
					t.Fatal(err)
				}
				f.txs.set(commitTx, tc.state)
				f.chain.setHeadLag(tc.headLag)

				done, err := f.revealer.Reveal(context.Background(), round)
				if err != nil {
					t.Fatal(err)
				}
				_, hasKey := f.state().CommitKey(round)
				if _, reveals := f.contract.counts(); reveals != 0 {
					t.Fatalf("revealed %d times", reveals)
				}
				if tc.gone {
					if !done || hasKey {
						t.Fatalf("done %v, key kept %v; want the key removed", done, hasKey)
					}
					return
				}
				if done || !hasKey {
					t.Fatalf("done %v, key kept %v; want the key kept and a later check", done, hasKey)
				}

				// the lagging endpoint catches up: the commit is mined, and
				// the next block's step reveals
				f.txs.set(commitTx, transaction.TxMined)
				f.chain.setHeadLag(0)
				time.Sleep(testBlockTime)
				if done, err := f.revealer.Reveal(context.Background(), round); err != nil || !done {
					t.Fatalf("retry: done %v, err %v", done, err)
				}
				if _, reveals := f.contract.counts(); reveals != 1 {
					t.Fatalf("reveals %d after the retry, want 1", reveals)
				}
			})
		})
	}
}

// One signal never cancels the stop's context. When block number reads
// keep failing, the wait still ends within WaitBudget plus the margin,
// and the stop goes on (the miss is counted).
func TestWaitBoundedWhenBlockReadsFail(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		f := newRevealFixture(t, mock.NewStateStore(), newFakeChain(testBlocksPerRound+2), newFakeTxs())
		const round = 1
		f.setSample(round)
		if err := f.state().SetCommitKey(round, []byte("key")); err != nil {
			t.Fatal(err)
		}
		f.state().SetCurrentBlock(f.chain.block())
		f.chain.setFailBlock(true)

		limit := f.revealer.WaitBudget() + stopWaitMargin
		if limit <= stopWaitMargin {
			t.Fatal("no reveal pending")
		}

		// the test's own deadline: a wait still running after it is
		// cancelled, which a bounded wait never needs
		ctx, cancel := context.WithTimeout(context.Background(), 2*limit)
		defer cancel()

		start := time.Now()
		if err := f.revealer.Wait(ctx); err != nil {
			t.Fatalf("wait ended by the test's deadline (%v) after %v, want it bounded by %v", err, time.Since(start), limit)
		}
		if took := time.Since(start); took > limit {
			t.Fatalf("wait took %v, limit %v", took, limit)
		}
		st, _ := f.state().Status()
		if st.StopWaitedForReveal != 1 || st.RevealMissedOnStop != 1 {
			t.Fatalf("counts waited %d missed %d, want 1 and 1", st.StopWaitedForReveal, st.RevealMissedOnStop)
		}
	})
}
