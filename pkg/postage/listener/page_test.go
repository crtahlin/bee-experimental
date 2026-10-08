// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
)

var errRangeTooLarge = errors.New("query returned more than 10000 results")

// rangeLimitFilterer is far ahead of the listener and refuses log queries
// over more than limit blocks, like a provider that limits eth_getLogs. It
// records the size of every query and whether it was refused.
type rangeLimitFilterer struct {
	limit     uint64
	blockDown atomic.Bool // BlockNumber fails, so FilterLogs is never asked

	mu      sync.Mutex
	sizes   []uint64
	refused []bool
}

func (f *rangeLimitFilterer) BlockNumber(context.Context) (uint64, error) {
	if f.blockDown.Load() {
		return 0, errEndpointDown
	}
	return 1_000_000_000, nil
}

func (f *rangeLimitFilterer) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	size := q.ToBlock.Uint64() - q.FromBlock.Uint64() + 1
	refuse := size > f.limit
	f.mu.Lock()
	f.sizes = append(f.sizes, size)
	f.refused = append(f.refused, refuse)
	f.mu.Unlock()
	if refuse {
		return nil, errRangeTooLarge
	}
	return nil, nil
}

func (f *rangeLimitFilterer) log() ([]uint64, []bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.sizes...), append([]bool(nil), f.refused...)
}

func startPaging(t *testing.T, f listener.BlockHeightContractFilterer) {
	t.Helper()
	startStale(t, f, log.Noop)
}

// TestPageHalvesAndGrowsBack checks that a refused log query halves the next
// page, and that pages applied in a row at the smaller size grow it back, so
// against a provider that limits the range the page settles in a cycle
// around the limit (#583).
func TestPageHalvesAndGrowsBack(t *testing.T) {
	defer listener.SetStaleMaxBackoff(20 * time.Millisecond)()

	f := &rangeLimitFilterer{limit: 1000}
	startPaging(t, f)

	waitFor(t, "enough queries", func() bool { s, _ := f.log(); return len(s) >= 30 })
	sizes, refused := f.log()

	if sizes[0] != listener.BlockPage {
		t.Fatalf("first query covered %d blocks, want the standard page %d", sizes[0], listener.BlockPage)
	}
	// 5000, 2500, 1250 refused, then 625 applies.
	for i, want := range []uint64{5000, 2500, 1250, 625} {
		if sizes[i] != want {
			t.Fatalf("query %d covered %d blocks, want %d; sizes %v", i, sizes[i], want, sizes[:8])
		}
	}
	grew := false
	for i := 4; i < len(sizes); i++ {
		if sizes[i] < 625 {
			t.Fatalf("query %d covered %d blocks, below the cycle around the limit; sizes %v", i, sizes[i], sizes)
		}
		if sizes[i] == 1250 {
			grew = true
			// It grows only after three applied pages at 625.
			for j := i - 3; j < i; j++ {
				if sizes[j] != 625 || refused[j] {
					t.Fatalf("page grew at query %d without three applied pages before it; sizes %v", i, sizes)
				}
			}
		}
	}
	if !grew {
		t.Fatalf("page never grew back after applied pages; sizes %v", sizes)
	}
}

// TestPageNotBelowMinimum checks that the page does not shrink below
// MinBlockPage, even when every query is refused.
func TestPageNotBelowMinimum(t *testing.T) {
	defer listener.SetStaleMaxBackoff(20 * time.Millisecond)()

	f := &rangeLimitFilterer{limit: 10}
	startPaging(t, f)

	waitFor(t, "the page to reach the minimum", func() bool {
		s, _ := f.log()
		return len(s) >= 12
	})
	sizes, _ := f.log()
	for i, s := range sizes {
		if s < listener.MinBlockPage {
			t.Fatalf("query %d covered %d blocks, below the minimum %d", i, s, listener.MinBlockPage)
		}
	}
	if last := sizes[len(sizes)-1]; last != listener.MinBlockPage {
		t.Fatalf("page settled at %d, want the minimum %d", last, listener.MinBlockPage)
	}
}

// TestBlockNumberFailureKeepsPage checks that a failed block number query
// does not change the page: only a refused log query does.
func TestBlockNumberFailureKeepsPage(t *testing.T) {
	defer listener.SetStaleMaxBackoff(20 * time.Millisecond)()

	f := &rangeLimitFilterer{limit: 1_000_000}
	f.blockDown.Store(true)
	startPaging(t, f)

	time.Sleep(100 * time.Millisecond)
	f.blockDown.Store(false)
	waitFor(t, "log queries", func() bool { s, _ := f.log(); return len(s) >= 3 })
	sizes, _ := f.log()
	for i, s := range sizes[:3] {
		if s != listener.BlockPage {
			t.Fatalf("query %d covered %d blocks after block number failures, want the standard page %d", i, s, listener.BlockPage)
		}
	}
}

// timedFilterer fails every block number query and records when each was
// made.
type timedFilterer struct {
	mu    sync.Mutex
	calls []time.Time
}

func (f *timedFilterer) BlockNumber(context.Context) (uint64, error) {
	f.mu.Lock()
	f.calls = append(f.calls, time.Now())
	f.mu.Unlock()
	return 0, errEndpointDown
}

func (*timedFilterer) FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
	return nil, nil
}

func (f *timedFilterer) gaps() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	var g []time.Duration
	for i := 1; i < len(f.calls); i++ {
		g = append(g, f.calls[i].Sub(f.calls[i-1]))
	}
	return g
}

// TestWaitGrowsOnlyWhileStale checks that the wait after a failed call stays
// at backoffTime before the batch store is stale, then doubles up to its cap
// while stale (#583).
func TestWaitGrowsOnlyWhileStale(t *testing.T) {
	defer listener.SetStaleTimings(5*time.Millisecond, time.Hour)()
	defer listener.SetStaleMaxBackoff(160 * time.Millisecond)()

	f := &timedFilterer{}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	l := listener.New(
		nil,
		log.Noop,
		f,
		postageStampContractAddress,
		postageStampContractABI,
		func() time.Duration { return time.Millisecond },
		listener.DefaultConfirmationDepth,
		300*time.Millisecond,
		10*time.Millisecond,
	)
	t.Cleanup(func() { _ = l.Close() })
	synced := l.Listen(context.Background(), 0, quietUpdater{})
	go func() {
		select {
		case <-synced:
		case <-done:
		}
	}()

	time.Sleep(1200 * time.Millisecond)
	gaps := f.gaps()
	if len(gaps) < 10 {
		t.Fatalf("only %d calls in 1.2 s", len(gaps)+1)
	}
	// Before the store is stale (300 ms) the wait stays near backoffTime.
	if gaps[0] > 100*time.Millisecond {
		t.Fatalf("first wait %v, want about backoffTime before the store is stale", gaps[0])
	}
	// While stale it reaches the cap.
	maxGap := time.Duration(0)
	for _, g := range gaps {
		if g > maxGap {
			maxGap = g
		}
	}
	if maxGap < 120*time.Millisecond {
		t.Fatalf("longest wait %v while stale, want it to grow towards the 160 ms cap; gaps %v", maxGap, gaps)
	}
}
