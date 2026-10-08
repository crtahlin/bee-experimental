// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/puller"
	mockps "github.com/ethersphere/bee/v2/pkg/pullsync/mock"
	"github.com/ethersphere/bee/v2/pkg/statestore/leveldb"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// heldReserve is a reserve whose held area can be marked full.
type heldReserve struct {
	*resMock.ReserveStore
	full atomic.Bool
}

func (h *heldReserve) HeldFull() bool { return h.full.Load() }

// TestPauseWhileHeldFull checks that sync workers wait while the held area
// is full, and resume once it has room again (#583).
func TestPauseWhileHeldFull(t *testing.T) {
	t.Parallel()

	addr := swarm.RandAddress(t)
	rs := &heldReserve{ReserveStore: resMock.NewReserve(resMock.WithRadius(1))}
	rs.full.Store(true)

	s, err := leveldb.NewStateStore(t.TempDir(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	ps := mockps.NewPullSync(
		mockps.WithCursors([]uint64{1000, 1000, 1000}, 0),
		mockps.WithReplies(mockps.SyncReply{Bin: 1, Start: 1, Topmost: 1000, Peer: addr}),
	)
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: addr, PO: 1}))

	p := puller.New(swarm.RandAddress(t), s, kad, rs, ps, nil, log.Noop, puller.Options{Bins: 3})
	p.Start(context.Background())
	testutil.CleanupCloser(t, p, s)

	time.Sleep(100 * time.Millisecond)
	kad.Trigger()
	waitCursorsCalled(t, ps, addr)

	time.Sleep(time.Second)
	if calls := ps.SyncCalls(addr); len(calls) != 0 {
		t.Fatalf("got %d sync calls while the held area is full, want 0", len(calls))
	}

	rs.full.Store(false)
	waitSyncCalledBins(t, ps, addr, 1)
}
