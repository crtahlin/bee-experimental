// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/puller"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
)

type reply struct {
	top uint64
	n   int
	err error
}

// scriptedSync answers bin 0 from a script, one reply per call, recording the
// time of each call, then blocks on the context. Its cursors are all 0, so
// only the live worker runs, from start 1.
type scriptedSync struct {
	mu      sync.Mutex
	script  []reply
	times   []time.Time
	cursors int
	epoch   uint64
}

func (s *scriptedSync) Sync(ctx context.Context, _ swarm.Address, bin uint8, start uint64) (uint64, int, error) {
	s.mu.Lock()
	if bin != 0 || len(s.script) == 0 {
		s.mu.Unlock()
		<-ctx.Done()
		return 0, 0, ctx.Err()
	}
	r := s.script[0]
	s.script = s.script[1:]
	s.times = append(s.times, time.Now())
	s.mu.Unlock()
	if r.top == 1 { // progress: the reply reaches the start
		r.top = start
	}
	return r.top, r.n, r.err
}

func (s *scriptedSync) GetCursors(context.Context, swarm.Address) ([]uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors++
	return []uint64{0}, s.epoch, nil
}

func (s *scriptedSync) calls() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.times...)
}

func newScripted(t *testing.T, base, maxWait time.Duration, script ...reply) (*puller.Puller, *scriptedSync) {
	t.Helper()
	ps := &scriptedSync{script: script}
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: swarm.RandAddress(t), PO: 0}))
	p := puller.New(swarm.RandAddress(t), mock.NewStateStore(), kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: 1})
	p.SetRetryBackoff(base, maxWait)
	p.Start(context.Background())
	t.Cleanup(func() { _ = p.Close() })
	return p, ps
}

var empty = reply{}

// TestNoProgressPauseGrowsAndResets checks that empty offers make the waits
// grow, and that a call with progress resets them.
func TestNoProgressPauseGrowsAndResets(t *testing.T) {
	t.Parallel()
	const base = 40 * time.Millisecond
	_, ps := newScripted(t, base, time.Second, empty, empty, empty, reply{top: 1, n: 1}, empty, empty)

	waitFor(t, 10*time.Second, func() bool { return len(ps.calls()) == 6 }, "script not consumed")
	c := ps.calls()
	gap := func(i int) time.Duration { return c[i].Sub(c[i-1]) }
	// After the first, second and third empty offer: base, 2x, 4x.
	if gap(1) < base || gap(2) < 2*base || gap(3) < 4*base {
		t.Fatalf("gaps %v %v %v after empty offers, want at least %v, %v, %v", gap(1), gap(2), gap(3), base, 2*base, 4*base)
	}
	// The progress reply resets the count: the next call follows at once,
	// and the wait after the next empty offer is the base again.
	if gap(4) >= base {
		t.Fatalf("gap %v after a call with progress, want no pause", gap(4))
	}
	if gap(5) >= 2*base {
		t.Fatalf("gap %v after the first empty offer following progress, want about %v", gap(5), base)
	}
}

// TestNoProgressCounted checks the counter: an empty offer counts; a failed
// call and a call with progress do not.
func TestNoProgressCounted(t *testing.T) {
	t.Parallel()
	p, ps := newScripted(t, time.Millisecond, 5*time.Millisecond, empty, reply{err: errors.New("boom")}, reply{top: 1, n: 1})
	waitFor(t, 10*time.Second, func() bool { return len(ps.calls()) == 3 }, "script not consumed")
	if n := p.NoProgress(); n != 1 {
		t.Fatalf("no-progress count %v, want 1", n)
	}
}

// TestCursorsAskedAgainAfterEpochFailure checks that a failed epoch check
// leaves the cursors unset, so the next recalculation asks again.
func TestCursorsAskedAgainAfterEpochFailure(t *testing.T) {
	t.Parallel()
	peer := swarm.RandAddress(t)
	epochKey := fmt.Sprintf("%s_epoch_%s", puller.IntervalPrefix, peer.ByteString())
	st := &failFirstEpochRead{StateStorer: mock.NewStateStore(), key: epochKey}
	ps := &scriptedSync{}
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: peer, PO: 0}))
	p := puller.New(swarm.RandAddress(t), st, kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: 1})
	p.Start(context.Background())
	t.Cleanup(func() { _ = p.Close() })

	waitFor(t, 10*time.Second, func() bool {
		ps.mu.Lock()
		n := ps.cursors
		ps.mu.Unlock()
		if n >= 2 {
			return true
		}
		kad.Trigger()
		return false
	}, "cursors not asked again after the failed epoch check")
}
