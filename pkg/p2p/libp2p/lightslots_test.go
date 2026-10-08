// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/lightnode"
)

// pickHookNotifier runs a hook inside Pick, before it accepts the peer.
type pickHookNotifier struct {
	p2p.PickyNotifier
	hook func(p2p.Peer)
}

func (n *pickHookNotifier) Pick(p p2p.Peer) bool {
	n.hook(p)
	return n.PickyNotifier.Pick(p)
}

// newLightLimitedNode starts a full node with the given light limit.
func newLightLimitedNode(t *testing.T, limit int, notifier p2p.PickyNotifier, o libp2p.Options) (*libp2p.Service, *lightnode.Container) {
	t.Helper()
	container := lightnode.NewContainer(swarm.RandAddress(t))
	o.LightNodeLimit = limit
	o.FullNode = true
	s, _ := newService(t, 1, libp2pServiceOpts{
		lightNodes: container,
		libp2pOpts: o,
		notifier:   notifier,
	})
	return s, container
}

// newLightNode starts a light node.
func newLightNode(t *testing.T) (*libp2p.Service, swarm.Address) {
	t.Helper()
	return newService(t, 1, libp2pServiceOpts{
		notifier:   mockNotifier(noopCf, noopDf, true),
		libp2pOpts: libp2p.Options{FullNode: false},
	})
}

// expectSlots waits until the container shows the wanted used and reserved
// light slots.
func expectSlots(t *testing.T, c *lightnode.Container, wantUsed, wantReserved int) {
	t.Helper()
	var used, reserved int
	err := spinlock.Wait(10*time.Second, func() bool {
		used, reserved, _, _ = c.Slots()
		return used == wantUsed && reserved == wantReserved
	})
	if err != nil {
		t.Fatalf("got %d used and %d reserved slots, want %d and %d", used, reserved, wantUsed, wantReserved)
	}
}

// TestLightSlotReleasedOnConnectInError checks that a reservation is
// released when a ConnectIn hook fails.
func TestLightSlotReleasedOnConnectInError(t *testing.T) {
	t.Parallel()

	sf, container := newLightLimitedNode(t, 2, mockNotifier(noopCf, noopDf, true), libp2p.Options{})
	proto := newTestProtocol(func(context.Context, p2p.Peer, p2p.Stream) error { return nil })
	proto.ConnectIn = func(context.Context, p2p.Peer) error { return errors.New("connect in refused") }
	if err := sf.AddProtocol(proto); err != nil {
		t.Fatal(err)
	}

	sl, _ := newLightNode(t)
	_, _ = sl.Connect(t.Context(), serviceUnderlayAddress(t, sf))

	expectPeersEventually(t, sf)
	expectSlots(t, container, 0, 0)
}

// TestLightSlotReleasedOnDisconnectDuringSetup checks that a light peer
// that disconnects while its connection is being set up does not keep a
// slot. That relies on the check after the notifier, which removes a
// committed peer that is no longer registered.
func TestLightSlotReleasedOnDisconnectDuringSetup(t *testing.T) {
	t.Parallel()

	sf, container := newLightLimitedNode(t, 2, mockNotifier(noopCf, noopDf, true), libp2p.Options{})
	proto := newTestProtocol(func(context.Context, p2p.Peer, p2p.Stream) error { return nil })
	proto.ConnectIn = func(_ context.Context, p p2p.Peer) error {
		_ = sf.Disconnect(p.Address, "test: disconnect during setup")
		return nil
	}
	if err := sf.AddProtocol(proto); err != nil {
		t.Fatal(err)
	}

	sl, _ := newLightNode(t)
	_, _ = sl.Connect(t.Context(), serviceUnderlayAddress(t, sf))

	expectPeersEventually(t, sf)
	expectSlots(t, container, 0, 0)
}

// TestLightSlotReleasedOnDuplicateConnection checks that a second
// connection from an overlay that is already connected releases its
// reservation and takes no second slot.
func TestLightSlotReleasedOnDuplicateConnection(t *testing.T) {
	t.Parallel()

	sf, container := newLightLimitedNode(t, 3, mockNotifier(noopCf, noopDf, true), libp2p.Options{})
	addr := serviceUnderlayAddress(t, sf)

	swarmKey, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		t.Fatal(err)
	}
	light := func() (*libp2p.Service, swarm.Address) {
		return newService(t, 1, libp2pServiceOpts{
			notifier:   mockNotifier(noopCf, noopDf, true),
			libp2pOpts: libp2p.Options{FullNode: false},
			swarmKey:   swarmKey,
		})
	}

	first, overlay := light()
	if _, err := first.Connect(t.Context(), addr); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf, overlay)
	expectSlots(t, container, 1, 0)

	second, _ := light()
	_, _ = second.Connect(t.Context(), addr)

	expectSlots(t, container, 1, 0)
}

// TestLightPeerRefusedAtReserve checks the reservation after the
// handshake: when another connection takes the last slot after the picker
// has let a light peer through, that peer is refused there.
func TestLightPeerRefusedAtReserve(t *testing.T) {
	t.Parallel()

	var container *lightnode.Container
	notifier := &pickHookNotifier{
		PickyNotifier: mockNotifier(noopCf, noopDf, true),
		hook: func(p p2p.Peer) {
			if !p.FullNode {
				// Another connection takes the only slot.
				if _, err := container.TryReserve(false); err != nil {
					panic(err)
				}
			}
		},
	}
	sf, c := newLightLimitedNode(t, 1, notifier, libp2p.Options{})
	container = c

	sl, _ := newLightNode(t)
	_, _ = sl.Connect(t.Context(), serviceUnderlayAddress(t, sf))

	expectPeersEventually(t, sf)
	expectSlots(t, container, 0, 1)
	if got := sf.LightPeerRefusals(libp2p.LightRefusalReserve); got != 1 {
		t.Fatalf("got %v refusals at the reservation, want 1", got)
	}
}

// TestLightPeerRefusedCanReconnect checks that a refusal leaves no
// blocklist entry: once a slot is free, the refused peer gets in.
func TestLightPeerRefusedCanReconnect(t *testing.T) {
	t.Parallel()

	sf, container := newLightLimitedNode(t, 1, mockNotifier(noopCf, noopDf, true), libp2p.Options{})
	addr := serviceUnderlayAddress(t, sf)

	first, firstOverlay := newLightNode(t)
	if _, err := first.Connect(t.Context(), addr); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf, firstOverlay)

	second, secondOverlay := newLightNode(t)
	_, _ = second.Connect(t.Context(), addr)
	expectPeersEventually(t, second)
	expectPeersEventually(t, sf, firstOverlay)

	if err := sf.Disconnect(firstOverlay, "test: free a slot"); err != nil {
		t.Fatal(err)
	}
	expectSlots(t, container, 0, 0)

	if _, err := second.Connect(t.Context(), addr); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf, secondOverlay)
}

// TestPickerWrapperPassesFullPeersToKademlia checks that the light-slot
// picker leaves full peers to kademlia's own Pick: when kademlia refuses, a
// full peer is refused, whatever the light slots.
func TestPickerWrapperPassesFullPeersToKademlia(t *testing.T) {
	t.Parallel()

	sf, container := newLightLimitedNode(t, 10, mockNotifier(noopCf, noopDf, false), libp2p.Options{})
	addr := serviceUnderlayAddress(t, sf)

	full, _ := newService(t, 1, libp2pServiceOpts{
		notifier:   mockNotifier(noopCf, noopDf, true),
		libp2pOpts: libp2p.Options{FullNode: true},
	})
	if _, err := full.Connect(t.Context(), addr); err == nil {
		// The dialer may only see the refusal when it closes the handshake
		// stream; either way the full peer must not stay connected.
		expectPeersEventually(t, sf)
	}
	expectPeersEventually(t, sf)
	if got := container.Count(); got != 0 {
		t.Fatalf("got %d light peers, want 0", got)
	}
}

// TestLightPeerLimitBootnodeEvicts checks that a bootnode keeps accepting
// light peers over its limit and evicts one to stay at it.
func TestLightPeerLimitBootnodeEvicts(t *testing.T) {
	t.Parallel()

	limit := 2
	sf, container := newLightLimitedNode(t, limit, mockNotifier(noopCf, noopDf, true), libp2p.Options{BootnodeMode: true})
	addr := serviceUnderlayAddress(t, sf)

	for range limit + 1 {
		sl, _ := newLightNode(t)
		if _, err := sl.Connect(t.Context(), addr); err != nil {
			t.Fatal(err)
		}
	}

	err := spinlock.Wait(10*time.Second, func() bool {
		return container.Count() == limit && sf.KickedOutPeers() == 1
	})
	if err != nil {
		t.Fatalf("got %d light peers and %v evictions, want %d and 1", container.Count(), sf.KickedOutPeers(), limit)
	}
	if got := sf.LightPeerRefusals(libp2p.LightRefusalPicker); got != 0 {
		t.Fatalf("got %v refusals at the picker, want 0", got)
	}
}
