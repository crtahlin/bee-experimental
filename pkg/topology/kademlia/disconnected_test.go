// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kademlia_test

import (
	"context"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology"
	"github.com/ethersphere/bee/v2/pkg/topology/kademlia"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// connectedCount counts the peers in kademlia's peer list.
func connectedCount(t *testing.T, kad *kademlia.Kad) int {
	t.Helper()
	n := 0
	if err := kad.EachConnectedPeer(func(swarm.Address, uint8) (bool, bool, error) {
		n++
		return false, false, nil
	}, topology.Select{}); err != nil {
		t.Fatal(err)
	}
	return n
}

// waitSignal waits for one topology signal and fails the test if none
// arrives in time.
func waitSignal(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(spinLockWaitTime):
		t.Fatal("timed out waiting for a topology signal")
	}
}

// TestDisconnectedIgnoresUncountedPeer checks that a disconnect of a peer
// kademlia never counted, such as a light peer, sends no topology signal
// and leaves the counted peers alone.
func TestDisconnectedIgnoresUncountedPeer(t *testing.T) {
	t.Parallel()

	base, kad, ab, _, sg := newTestKademlia(t, nil, nil, kademlia.Options{})
	if err := kad.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	testutil.CleanupCloser(t, kad)

	c, unsubscribe := kad.SubscribeTopologyChange()
	defer unsubscribe()

	counted := swarm.RandAddressAt(t, base, 3)
	addOne(t, sg, kad, ab, counted)
	waitSignal(t, c)

	recalculations := kad.DepthRecalculations()

	// A light peer and a full peer kademlia never accepted.
	light := swarm.RandAddressAt(t, base, 3)
	refused := swarm.RandAddressAt(t, base, 5)
	kad.Disconnected(p2p.Peer{Address: light})
	kad.Disconnected(p2p.Peer{Address: refused, FullNode: true})

	// Neither gets a retry entry: nothing dials light peers, and a refused
	// full peer has no recorded connection to count.
	for _, a := range []swarm.Address{light, refused} {
		if _, ok := kad.RetryWaitTryAfter(a); ok {
			t.Fatalf("retry entry created for %s, a peer kademlia never counted", a)
		}
	}
	if got := kad.ShortLivedConnections(); got != 0 {
		t.Fatalf("got %v short-lived connections, want 0", got)
	}

	select {
	case <-c:
		t.Fatal("got a topology signal for a peer kademlia never counted")
	case <-time.After(500 * time.Millisecond):
	}

	if got := connectedCount(t, kad); got != 1 {
		t.Fatalf("got %d connected peers, want 1", got)
	}
	if got := kad.DepthRecalculations(); got != recalculations {
		t.Fatalf("got %v depth recalculations, want %v", got, recalculations)
	}
}

// TestDisconnectedRemovesFullPeerMarkedLight checks that a full peer whose
// disconnect arrives with FullNode false is still removed, because the
// peer registry can lose that flag before it reports the disconnect.
func TestDisconnectedRemovesFullPeerMarkedLight(t *testing.T) {
	t.Parallel()

	base, kad, ab, _, sg := newTestKademlia(t, nil, nil, kademlia.Options{})
	if err := kad.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	testutil.CleanupCloser(t, kad)

	c, unsubscribe := kad.SubscribeTopologyChange()
	defer unsubscribe()

	peer := swarm.RandAddressAt(t, base, 3)
	addOne(t, sg, kad, ab, peer)
	waitSignal(t, c)

	recalculations := kad.DepthRecalculations()

	kad.Disconnected(p2p.Peer{Address: peer, FullNode: false})
	waitSignal(t, c)

	if got := connectedCount(t, kad); got != 0 {
		t.Fatalf("got %d connected peers, want 0", got)
	}
	if got := kad.DepthRecalculations(); got <= recalculations {
		t.Fatalf("got %v depth recalculations, want more than %v", got, recalculations)
	}
}
