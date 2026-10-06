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

	// A light peer and a full peer kademlia never accepted.
	kad.Disconnected(p2p.Peer{Address: swarm.RandAddressAt(t, base, 3)})
	kad.Disconnected(p2p.Peer{Address: swarm.RandAddressAt(t, base, 5), FullNode: true})

	select {
	case <-c:
		t.Fatal("got a topology signal for a peer kademlia never counted")
	case <-time.After(500 * time.Millisecond):
	}

	if got := connectedCount(t, kad); got != 1 {
		t.Fatalf("got %d connected peers, want 1", got)
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

	kad.Disconnected(p2p.Peer{Address: peer, FullNode: false})
	waitSignal(t, c)

	if got := connectedCount(t, kad); got != 0 {
		t.Fatalf("got %d connected peers, want 0", got)
	}
}
