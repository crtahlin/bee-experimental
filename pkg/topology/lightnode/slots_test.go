// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lightnode_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/lightnode"
)

func expectSlots(t *testing.T, c *lightnode.Container, used, reserved, usedUltra, reservedUltra int) {
	t.Helper()
	u, r, uu, ru := c.Slots()
	if u != used || r != reserved || uu != usedUltra || ru != reservedUltra {
		t.Fatalf("got slots used=%d reserved=%d ultra used=%d ultra reserved=%d, want %d %d %d %d",
			u, r, uu, ru, used, reserved, usedUltra, reservedUltra)
	}
}

func TestSlotLedger(t *testing.T) {
	t.Parallel()

	t.Run("reserve and commit", func(t *testing.T) {
		t.Parallel()

		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(2, 0)

		tok, err := c.TryReserve(false)
		if err != nil {
			t.Fatal(err)
		}
		expectSlots(t, c, 0, 1, 0, 0)
		if c.AtLimit() {
			t.Fatal("at limit with one free slot")
		}

		peer := swarm.RandAddress(t)
		if !c.Commit(tok, peer) {
			t.Fatal("commit of an open reservation returned false")
		}
		expectSlots(t, c, 1, 0, 0, 0)
		if c.Count() != 1 {
			t.Fatalf("got count %d, want 1", c.Count())
		}

		// A release after the commit is a no-op, so a deferred release
		// is safe.
		c.Release(tok)
		expectSlots(t, c, 1, 0, 0, 0)

		// A second commit of the same token adds nothing.
		if c.Commit(tok, swarm.RandAddress(t)) {
			t.Fatal("second commit of the same token returned true")
		}
		expectSlots(t, c, 1, 0, 0, 0)
	})

	t.Run("reservations count against the limit", func(t *testing.T) {
		t.Parallel()

		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(2, 0)

		tok1, err := c.TryReserve(false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.TryReserve(false); err != nil {
			t.Fatal(err)
		}
		if !c.AtLimit() {
			t.Fatal("not at limit with every slot reserved")
		}
		if _, err := c.TryReserve(false); !errors.Is(err, lightnode.ErrLightNodeLimit) {
			t.Fatalf("got %v, want %v", err, lightnode.ErrLightNodeLimit)
		}

		c.Release(tok1)
		expectSlots(t, c, 0, 1, 0, 0)
		if _, err := c.TryReserve(false); err != nil {
			t.Fatalf("reserve after release: %v", err)
		}
	})

	t.Run("used slots count against the limit", func(t *testing.T) {
		t.Parallel()

		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(1, 0)

		tok, err := c.TryReserve(false)
		if err != nil {
			t.Fatal(err)
		}
		c.Commit(tok, swarm.RandAddress(t))
		if _, err := c.TryReserve(false); !errors.Is(err, lightnode.ErrLightNodeLimit) {
			t.Fatalf("got %v, want %v", err, lightnode.ErrLightNodeLimit)
		}
	})

	t.Run("disconnect of an unknown overlay frees nothing", func(t *testing.T) {
		t.Parallel()

		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(2, 1)

		tok, err := c.TryReserve(true)
		if err != nil {
			t.Fatal(err)
		}
		peer := swarm.RandAddress(t)
		c.Commit(tok, peer)
		expectSlots(t, c, 1, 0, 1, 0)

		// Neither a light nor a full peer the container does not hold
		// changes any count.
		c.Disconnected(p2p.Peer{Address: swarm.RandAddress(t)})
		c.Disconnected(p2p.Peer{Address: swarm.RandAddress(t), FullNode: true})
		expectSlots(t, c, 1, 0, 1, 0)

		// The held peer frees its slot even when it arrives marked as a
		// full node.
		c.Disconnected(p2p.Peer{Address: peer, FullNode: true})
		expectSlots(t, c, 0, 0, 0, 0)

		// A second disconnect of the same overlay frees nothing more.
		c.Disconnected(p2p.Peer{Address: peer})
		expectSlots(t, c, 0, 0, 0, 0)
	})

	t.Run("ultra-light limit", func(t *testing.T) {
		t.Parallel()

		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(3, 1)

		tok, err := c.TryReserve(true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.TryReserve(true); !errors.Is(err, lightnode.ErrUltraLightNodeLimit) {
			t.Fatalf("reserved ultra-light slot: got %v, want %v", err, lightnode.ErrUltraLightNodeLimit)
		}
		ultra := swarm.RandAddress(t)
		c.Commit(tok, ultra)
		if _, err := c.TryReserve(true); !errors.Is(err, lightnode.ErrUltraLightNodeLimit) {
			t.Fatalf("used ultra-light slot: got %v, want %v", err, lightnode.ErrUltraLightNodeLimit)
		}

		// A light peer with a chequebook still gets a slot.
		if _, err := c.TryReserve(false); err != nil {
			t.Fatalf("light peer refused: %v", err)
		}
		expectSlots(t, c, 1, 1, 1, 0)

		// The ultra-light peer leaves, and its slot is free again.
		c.Disconnected(p2p.Peer{Address: ultra})
		if _, err := c.TryReserve(true); err != nil {
			t.Fatalf("ultra-light peer refused after a disconnect: %v", err)
		}
	})

	t.Run("released ultra-light reservation frees its slot", func(t *testing.T) {
		t.Parallel()

		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(3, 1)

		tok, err := c.TryReserve(true)
		if err != nil {
			t.Fatal(err)
		}
		c.Release(tok)
		expectSlots(t, c, 0, 0, 0, 0)
		if _, err := c.TryReserve(true); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("recommit of a held overlay takes no second slot", func(t *testing.T) {
		t.Parallel()

		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(3, 2)

		peer := swarm.RandAddress(t)
		tok, _ := c.TryReserve(true)
		c.Commit(tok, peer)
		tok, _ = c.TryReserve(false)
		c.Commit(tok, peer)
		expectSlots(t, c, 1, 0, 0, 0)

		c.Disconnected(p2p.Peer{Address: peer})
		expectSlots(t, c, 0, 0, 0, 0)
	})

	t.Run("concurrent reservations never exceed the limit", func(t *testing.T) {
		t.Parallel()

		const limit = 10
		c := lightnode.NewContainer(swarm.RandAddress(t))
		c.SetLimits(limit, 0)

		var (
			wg     sync.WaitGroup
			mu     sync.Mutex
			tokens []lightnode.Token
		)
		for range 200 {
			wg.Go(func() {
				tok, err := c.TryReserve(false)
				if err != nil {
					return
				}
				mu.Lock()
				tokens = append(tokens, tok)
				mu.Unlock()
			})
		}
		wg.Wait()

		if len(tokens) != limit {
			t.Fatalf("got %d reservations, want %d", len(tokens), limit)
		}
		for _, tok := range tokens {
			c.Commit(tok, swarm.RandAddress(t))
		}
		expectSlots(t, c, limit, 0, 0, 0)
	})
}
