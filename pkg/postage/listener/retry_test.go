// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

var errLogQuery = errors.New("log query failed")

// failingLogsFilterer is far ahead of the listener, so every pass is paged,
// and fails each FilterLogs call listed in fail. It records when each call
// was made.
type failingLogsFilterer struct {
	blockNumber uint64
	fail        func(call int) bool

	mu    sync.Mutex
	calls []time.Time
}

func (f *failingLogsFilterer) BlockNumber(context.Context) (uint64, error) {
	return f.blockNumber, nil
}

func (f *failingLogsFilterer) FilterLogs(ctx context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
	f.mu.Lock()
	f.calls = append(f.calls, time.Now())
	n := len(f.calls)
	f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.fail(n) {
		return nil, errLogQuery
	}
	return nil, nil
}

func (f *failingLogsFilterer) callTimes() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.calls...)
}

// quietUpdater accepts every update without blocking.
type quietUpdater struct{}

func (quietUpdater) Create([]byte, []byte, *big.Int, *big.Int, uint8, uint8, bool, common.Hash) error {
	return nil
}
func (quietUpdater) TopUp([]byte, *big.Int, *big.Int, common.Hash) error    { return nil }
func (quietUpdater) UpdateDepth([]byte, uint8, *big.Int, common.Hash) error { return nil }
func (quietUpdater) UpdatePrice(*big.Int, common.Hash) error                { return nil }
func (quietUpdater) UpdateBlockNumber(uint64) error                         { return nil }
func (quietUpdater) Start(context.Context, uint64) error                    { return nil }
func (quietUpdater) TransactionStart() error                                { return nil }
func (quietUpdater) TransactionEnd() error                                  { return nil }

func startRetryListener(t *testing.T, f *failingLogsFilterer, backoff time.Duration) time.Time {
	t.Helper()

	// Cleanups run last first: the listener closes before the reader below
	// stops, so a listener waiting to report that it caught up is still read.
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })

	l := listener.New(
		nil,
		log.Noop,
		f,
		postageStampContractAddress,
		postageStampContractABI,
		// A block time of backoff makes the wait after a successful page as
		// long as after a failure, so the page test can tell them apart.
		func() time.Duration { return backoff },
		listener.DefaultConfirmationDepth,
		stallingTimeout,
		backoff,
	)
	testutil.CleanupCloser(t, l)

	start := time.Now()
	// Once caught up, the listener sends on the returned channel, at most
	// once, and waits for a reader.
	errC := l.Listen(context.Background(), 0, quietUpdater{})
	go func() {
		select {
		case <-errC:
		case <-done:
		}
	}()
	return start
}

// TestListenerPausesAfterFailedLogQuery: during catch-up, a failed log query
// is retried after backoffTime, not at once. See #578.
func TestListenerPausesAfterFailedLogQuery(t *testing.T) {
	t.Parallel()

	const backoff = 200 * time.Millisecond
	f := &failingLogsFilterer{blockNumber: 200000, fail: func(int) bool { return true }}
	start := startRetryListener(t, f, backoff)

	time.Sleep(time.Second)
	calls := len(f.callTimes())
	elapsed := time.Since(start)

	if calls < 2 {
		t.Fatalf("got %d log queries in %v, want at least 2: a failed query must be retried", calls, elapsed)
	}
	if limit := 1 + int(elapsed/backoff); calls > limit {
		t.Fatalf("got %d log queries in %v, want at most %d: a failed query during catch-up must be retried after %v, not at once", calls, elapsed, limit, backoff)
	}
}

// TestListenerPagesAtOnceAfterRecovery: after a failed query is followed by a
// successful page, the next page is fetched without a pause. This also passes
// without the fix; it guards against clearing paged after every query.
func TestListenerPagesAtOnceAfterRecovery(t *testing.T) {
	t.Parallel()

	const backoff = 200 * time.Millisecond
	f := &failingLogsFilterer{blockNumber: 200000, fail: func(n int) bool { return n <= 2 }}
	startRetryListener(t, f, backoff)

	deadline := time.Now().Add(5 * time.Second)
	for len(f.callTimes()) < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("got %d log queries in 5 s, want 4", len(f.callTimes()))
		}
		time.Sleep(10 * time.Millisecond)
	}
	calls := f.callTimes()
	if gap := calls[3].Sub(calls[2]); gap >= backoff/2 {
		t.Fatalf("the page after a successful page came %v later, want under %v: paging must continue at once", gap, backoff/2)
	}
}
