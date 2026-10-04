// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/puller"
	mockps "github.com/ethersphere/bee/v2/pkg/pullsync/mock"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	"github.com/ethersphere/bee/v2/pkg/statestore/leveldb"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

var errStreamReset = errors.New("stream reset")

// newRetryPuller is newPuller with the retry wait bounds set before Start.
// One bin, one peer at PO 0 and cursor 1, so a single historical worker
// syncs from start 1; the live worker asks for start 2, which has no replies
// and waits on its context.
func newRetryPuller(t *testing.T, addr swarm.Address, base, maxWait time.Duration, replies ...mockps.SyncReply) (*puller.Puller, *kadMock.Mock, *mockps.PullSyncMock) {
	t.Helper()

	s, err := leveldb.NewStateStore(t.TempDir(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	ps := mockps.NewPullSync(mockps.WithCursors([]uint64{1}, 0), mockps.WithReplies(replies...))
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: addr, PO: 0}))
	p := puller.New(swarm.RandAddress(t), s, kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: 1})
	p.SetRetryBackoff(base, maxWait)
	p.Start(context.Background())
	testutil.CleanupCloser(t, p, s)

	time.Sleep(50 * time.Millisecond)
	kad.Trigger()
	return p, kad, ps
}

func failing(addr swarm.Address, start uint64, n int) []mockps.SyncReply {
	r := make([]mockps.SyncReply, n)
	for i := range r {
		r[i] = mockps.SyncReply{Peer: addr, Start: start, Topmost: 0, Err: errStreamReset}
	}
	return r
}

// TestPullerPausesAfterFailedSync: a peer whose every call fails without
// progress is retried after a growing wait, not at once. See #573.
func TestPullerPausesAfterFailedSync(t *testing.T) {
	t.Parallel()

	addr := swarm.RandAddress(t)
	_, _, ps := newRetryPuller(t, addr, 100*time.Millisecond, time.Minute, failing(addr, 1, 50)...)

	time.Sleep(time.Second)
	// Calls at about 0, 0.1, 0.3 and 0.7 s (plus up to 20 percent each).
	if got := len(ps.SyncCalls(addr)); got < 2 || got > 5 {
		t.Fatalf("got %d sync calls in 1 s, want 2 to 5: a failing peer must be retried after a growing wait, not at once", got)
	}
}

// TestPullerNoPauseAfterPartialSuccess: a call that advanced the interval but
// returned an error (some chunks failed) continues at once, as before.
func TestPullerNoPauseAfterPartialSuccess(t *testing.T) {
	t.Parallel()

	addr := swarm.RandAddress(t)
	replies := []mockps.SyncReply{
		{Peer: addr, Start: 1, Topmost: 1, Err: errStreamReset},
		{Peer: addr, Start: 2, Topmost: 2, Err: errStreamReset},
		{Peer: addr, Start: 3, Topmost: 3, Err: errStreamReset},
	}
	// Cursor 3, so the historical worker covers starts 1 to 3.
	s, err := leveldb.NewStateStore(t.TempDir(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	ps := mockps.NewPullSync(mockps.WithCursors([]uint64{3}, 0), mockps.WithReplies(replies...))
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: addr, PO: 0}))
	p := puller.New(swarm.RandAddress(t), s, kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: 1})
	p.SetRetryBackoff(time.Minute, time.Minute)
	p.Start(context.Background())
	testutil.CleanupCloser(t, p, s)
	time.Sleep(50 * time.Millisecond)
	kad.Trigger()

	if err := spinlock.Wait(2*time.Second, func() bool { return len(ps.SyncCalls(addr)) >= 3 }); err != nil {
		t.Fatalf("got %d sync calls, want 3 within 2 s: a call that advanced the interval must not be paused (wait is 1 minute here)", len(ps.SyncCalls(addr)))
	}
}

// TestPullerRetryWaitResetsAfterProgress: two failures, progress, then a
// failure. The wait after that last failure is the base wait again.
func TestPullerRetryWaitResetsAfterProgress(t *testing.T) {
	t.Parallel()

	const base = 200 * time.Millisecond
	addr := swarm.RandAddress(t)
	s, err := leveldb.NewStateStore(t.TempDir(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	replies := []mockps.SyncReply{
		{Peer: addr, Start: 1, Topmost: 0, Err: errStreamReset}, // wait base
		{Peer: addr, Start: 1, Topmost: 0, Err: errStreamReset}, // wait 2 x base
		{Peer: addr, Start: 1, Topmost: 5},                      // progress: reset
		{Peer: addr, Start: 6, Topmost: 0, Err: errStreamReset}, // wait base again (4 x base without the reset)
		{Peer: addr, Start: 6, Topmost: 0, Err: errStreamReset},
	}
	ps := mockps.NewPullSync(mockps.WithCursors([]uint64{10}, 0), mockps.WithReplies(replies...))
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: addr, PO: 0}))
	p := puller.New(swarm.RandAddress(t), s, kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: 1})
	p.SetRetryBackoff(base, time.Minute)
	p.Start(context.Background())
	testutil.CleanupCloser(t, p, s)
	time.Sleep(50 * time.Millisecond)
	kad.Trigger()

	if err := spinlock.Wait(3*time.Second, func() bool { return len(ps.SyncCalls(addr)) >= 4 }); err != nil {
		t.Fatalf("got %d sync calls, want 4 within 3 s", len(ps.SyncCalls(addr)))
	}
	at4 := time.Now()
	if err := spinlock.Wait(3*time.Second, func() bool { return len(ps.SyncCalls(addr)) >= 5 }); err != nil {
		t.Fatalf("no fifth call within 3 s")
	}
	// With the reset: base plus up to 20 percent (240 ms). Without: 4 x base (800 ms or more).
	if gap := time.Since(at4); gap > 600*time.Millisecond {
		t.Fatalf("wait after the failure that followed progress was %v, want about %v: progress must reset the wait", gap, base)
	}
}

// TestPullerRetryWaitEndsWithContext: a worker waiting before a retry exits as
// soon as its peer is removed. Removing a peer waits for its workers while
// holding the puller's locks, so a wait that ignored the context would block
// the manage loop for the whole wait.
func TestPullerRetryWaitEndsWithContext(t *testing.T) {
	t.Parallel()

	addr := swarm.RandAddress(t)
	p, kad, ps := newRetryPuller(t, addr, time.Minute, time.Minute, failing(addr, 1, 5)...)

	if err := spinlock.Wait(2*time.Second, func() bool { return len(ps.SyncCalls(addr)) >= 1 }); err != nil {
		t.Fatal("first sync call not made")
	}
	if !p.IsSyncing(addr) {
		t.Fatal("peer not syncing")
	}

	if err := spinlock.Wait(time.Second, func() bool { return p.SyncWorkers() >= 1 }); err != nil {
		t.Fatal("no sync worker running")
	}

	kad.ResetPeers()
	kad.Trigger()

	// The gauge is read without the puller's locks, so this check cannot
	// itself be held up by a worker that keeps waiting.
	if err := spinlock.Wait(time.Second, func() bool { return p.SyncWorkers() == 0 }); err != nil {
		t.Fatalf("%v sync workers still running 1 s after the peer was removed: the retry wait must end when the worker's context ends", p.SyncWorkers())
	}
}

// TestPullerRetryBackoffBounds: the wait stays between base and max plus 20
// percent for any failure count, including bases large enough that shifting
// them would overflow.
func TestPullerRetryBackoffBounds(t *testing.T) {
	t.Parallel()

	for _, base := range []time.Duration{time.Millisecond, time.Second, 20 * time.Second, time.Minute} {
		p := puller.New(swarm.RandAddress(t), nil, nil, nil, nil, nil, log.Noop, puller.Options{Bins: 1})
		p.SetRetryBackoff(base, 10*time.Minute)
		for n := -1; n <= 70; n++ {
			got := p.RetryBackoff(n)
			if got < base || got > 12*time.Minute {
				t.Fatalf("base %v, %d failures: wait %v, want between %v and 12m", base, n, got, base)
			}
		}
	}
}
