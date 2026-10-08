// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
	"github.com/ethersphere/bee/v2/pkg/util/syncutil"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

var errEndpointDown = errors.New("endpoint down")

// switchFilterer answers like an endpoint whose state a test changes while
// the listener runs: down, far ahead (every pass paged), or close to the
// listener (one pass reaches the head).
type switchFilterer struct {
	down atomic.Bool
	head atomic.Uint64

	mu     sync.Mutex
	lastTo uint64
	pages  int
}

func (f *switchFilterer) BlockNumber(context.Context) (uint64, error) {
	if f.down.Load() {
		return 0, errEndpointDown
	}
	return f.head.Load(), nil
}

func (f *switchFilterer) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.lastTo = q.ToBlock.Uint64()
	f.pages++
	f.mu.Unlock()
	return nil, nil
}

func (f *switchFilterer) progress() (lastTo uint64, pages int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTo, f.pages
}

// startStale starts a listener on f with a short stall timeout and returns it
// with its stop signaler. The synced channel is read in the background, as
// the batch service does at startup.
func startStale(t *testing.T, f listener.BlockHeightContractFilterer, logger log.Logger, opts ...listener.Option) (postage.Listener, *syncutil.Signaler) {
	t.Helper()

	done := make(chan struct{})
	t.Cleanup(func() { close(done) })

	c := syncutil.NewSignaler()
	l := listener.New(
		c,
		logger,
		f,
		postageStampContractAddress,
		postageStampContractABI,
		func() time.Duration { return time.Millisecond },
		listener.DefaultConfirmationDepth,
		50*time.Millisecond,
		time.Millisecond,
		opts...,
	)
	testutil.CleanupCloser(t, l)
	synced := l.Listen(context.Background(), 0, quietUpdater{})
	go func() {
		select {
		case <-synced:
		case <-done:
		}
	}()
	return l, c
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestStaleEndsOnlyWhenCaughtUp checks that pages applied while far behind do
// not end the stale state, and a page that reaches the head does (#583).
func TestStaleEndsOnlyWhenCaughtUp(t *testing.T) {
	defer listener.SetStaleTimings(5*time.Millisecond, time.Hour)()

	f := &switchFilterer{}
	f.down.Store(true)
	l, c := startStale(t, f, log.Noop)
	health := l.(postage.SyncHealth)

	waitFor(t, "the batch store to go stale", health.Stale)

	// Far ahead: every pass is paged. Pages are applied, so progress is
	// made, but the listener is not caught up.
	f.head.Store(1_000_000_000)
	f.down.Store(false)
	waitFor(t, "paged progress", func() bool { _, n := f.progress(); return n >= 20 })
	if !health.Stale() {
		t.Fatal("stale state ended on pages applied while far behind")
	}

	// Close to the listener: the next pass reaches the head. Take the
	// endpoint down first and let the paging settle, so the head is set
	// ahead of where the listener really is.
	f.down.Store(true)
	var lastTo uint64
	waitFor(t, "paging to settle", func() bool {
		before, n := f.progress()
		time.Sleep(50 * time.Millisecond)
		after, m := f.progress()
		lastTo = after
		return before == after && n == m
	})
	f.head.Store(lastTo + listener.BlockPage/2)
	f.down.Store(false)
	waitFor(t, "the stale state to end once caught up", func() bool { return !health.Stale() })

	// The watcher records the end of the stale state on its next tick.
	waitFor(t, "the end of the stale state to be recorded", func() bool { return !health.StaleEndedAt().IsZero() })
	select {
	case <-c.C:
		t.Fatal("the node was stopped")
	default:
	}
}

// hangOnce answers block numbers, except that every call hangs until its own
// context ends.
type hangOnce struct{}

func (hangOnce) BlockNumber(ctx context.Context) (uint64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func (hangOnce) FilterLogs(ctx context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestStaleWhileCallHangs checks that the stale state turns true while a
// chain call is still in progress: it is computed when read, not in the loop
// (#583).
func TestStaleWhileCallHangs(t *testing.T) {
	defer listener.SetStaleTimings(5*time.Millisecond, time.Hour)()
	defer listener.SetCallTimeouts(time.Hour, time.Hour)()

	l, _ := startStale(t, hangOnce{}, log.Noop)
	waitFor(t, "the batch store to go stale while a call hangs", l.(postage.SyncHealth).Stale)
}

// TestStallShutdown checks that postage-stall-shutdown stops the node once
// the batch store has been stale that long, with the stall as the cause.
func TestStallShutdown(t *testing.T) {
	defer listener.SetStaleTimings(5*time.Millisecond, time.Hour)()

	f := &switchFilterer{}
	f.down.Store(true)
	_, c := startStale(t, f, log.Noop, listener.WithStallShutdown(100*time.Millisecond))

	select {
	case <-c.C:
	case <-time.After(5 * time.Second):
		t.Fatal("postage-stall-shutdown did not stop the node")
	}
	if !errors.Is(c.Err(), listener.ErrPostageSyncingStalled) {
		t.Fatalf("stop cause %v, want %v", c.Err(), listener.ErrPostageSyncingStalled)
	}
}

// syncBuffer is a bytes.Buffer safe for the logger's writes and the test's
// reads.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) count(sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Count(s.b.String(), sub)
}

// TestStaleWarningRepeatsAndRecovers checks that the stale Warning repeats
// while stale, and that the recovery is logged (#583).
func TestStaleWarningRepeatsAndRecovers(t *testing.T) {
	defer listener.SetStaleTimings(5*time.Millisecond, 40*time.Millisecond)()

	buf := &syncBuffer{}
	logger := log.NewLogger("stale-test", log.WithSink(buf), log.WithVerbosity(log.VerbosityInfo))

	f := &switchFilterer{}
	f.down.Store(true)
	l, _ := startStale(t, f, logger)

	waitFor(t, "the stale Warning to repeat", func() bool { return buf.count("postage sync stalled") >= 3 })

	f.head.Store(listener.BlockPage / 2)
	f.down.Store(false)
	waitFor(t, "the stale state to end", func() bool { return !l.(postage.SyncHealth).Stale() })
	waitFor(t, "the recovery to be logged", func() bool { return buf.count("postage sync caught up") == 1 })
}
