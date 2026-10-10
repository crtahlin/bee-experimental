// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/puller"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
)

// silentCursors is a pullsync stub whose cursors request never answers for
// one peer and answers at once for every other. It counts the requests per
// peer without locks the puller holds, so a test can read the counts while a
// recalculation is blocked.
type silentCursors struct {
	silent swarm.Address
	bins   int

	mu    sync.Mutex
	calls map[string]*atomic.Int64
}

func newSilentCursors(silent swarm.Address, bins int) *silentCursors {
	return &silentCursors{silent: silent, bins: bins, calls: make(map[string]*atomic.Int64)}
}

func (s *silentCursors) counter(peer swarm.Address) *atomic.Int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.calls[peer.ByteString()]
	if !ok {
		c = new(atomic.Int64)
		s.calls[peer.ByteString()] = c
	}
	return c
}

func (s *silentCursors) cursorsCalls(peer swarm.Address) int64 { return s.counter(peer).Load() }

func (s *silentCursors) Sync(ctx context.Context, _ swarm.Address, _ uint8, _ uint64) (uint64, int, error) {
	<-ctx.Done()
	return 0, 0, ctx.Err()
}

func (s *silentCursors) GetCursors(ctx context.Context, peer swarm.Address) ([]uint64, uint64, error) {
	s.counter(peer).Add(1)
	if peer.Equal(s.silent) {
		<-ctx.Done()
		return nil, 0, ctx.Err()
	}
	return make([]uint64, s.bins), 0, nil
}

// TestRecalcNotBlockedBySilentPeer reproduces #695: a peer that never answers
// the cursors request held the whole recalculation, so no later
// recalculation ran. Peer B answers and is asked once; peer A's request count
// shows whether later recalculations run. Uses only what upstream has, apart
// from setCursorsTimeout, the per-tree shim.
func TestRecalcNotBlockedBySilentPeer(t *testing.T) {
	t.Parallel()

	const bins = 3
	var (
		a = swarm.RandAddress(t)
		b = swarm.RandAddress(t)
	)
	ps := newSilentCursors(a, bins)
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(
		kadMock.AddrTuple{Addr: a, PO: 1},
		kadMock.AddrTuple{Addr: b, PO: 1},
	))
	p := puller.New(swarm.RandAddress(t), mock.NewStateStore(), kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: bins})
	setCursorsTimeout(p, 100*time.Millisecond)
	p.Start(context.Background())
	t.Cleanup(func() { _ = p.Close() })

	waitFor(t, 5*time.Second, func() bool { return ps.cursorsCalls(a) >= 1 }, "peer A never asked")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ps.cursorsCalls(a) >= 2 {
			if got := ps.cursorsCalls(b); got != 1 {
				t.Fatalf("peer B asked %d times, want 1", got)
			}
			return
		}
		kad.Trigger()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer A asked %d time, want at least 2: the first recalculation never returned", ps.cursorsCalls(a))
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}
