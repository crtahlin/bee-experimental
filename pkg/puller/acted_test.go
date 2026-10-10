// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
)

func acted(c *cursorPuller) (uint8, time.Time, bool) { return c.p.ActedOnRadius() }

func waitActed(t *testing.T, c *cursorPuller, radius uint8) time.Time {
	t.Helper()
	var since time.Time
	waitFor(t, 5*time.Second, func() bool {
		r, s, ok := acted(c)
		since = s
		return ok && r == radius
	}, "puller never acted on the radius")
	return since
}

func TestActedOnRadius(t *testing.T) {
	t.Parallel()
	b := swarm.RandAddress(t)
	c := newCursorPuller(t, time.Second, "", kadMock.AddrTuple{Addr: b, PO: 2})
	c.rs.SetStorageRadius(2)
	before := time.Now()
	c.start(t)

	since := waitActed(t, c, 2)
	if since.Before(before) {
		t.Fatalf("since %v before the start %v", since, before)
	}

	// Later recalculations at the same radius do not move since.
	runs := c.p.OnChangeDurationCount()
	c.kad.Trigger()
	waitFor(t, 5*time.Second, func() bool { return c.p.OnChangeDurationCount() > runs }, "no further run")
	if _, s, _ := acted(c); !s.Equal(since) {
		t.Fatalf("since moved from %v to %v on a run at the same radius", since, s)
	}

	// A decrease sets a new since once B syncs every bin from the new radius.
	c.rs.SetStorageRadius(1)
	c.kad.Trigger()
	since1 := waitActed(t, c, 1)
	if !since1.After(since) {
		t.Fatalf("since %v after the decrease, want after %v", since1, since)
	}
	if !c.ps.binSynced(b, 1) {
		t.Fatal("acted on radius 1 without bin 1 syncing")
	}

	// An increase also sets a new since; B's bins above it were already syncing.
	c.rs.SetStorageRadius(2)
	c.kad.Trigger()
	since2 := waitActed(t, c, 2)
	if !since2.After(since1) {
		t.Fatalf("since %v after the increase, want after %v", since2, since1)
	}
}

func TestNotActedWithoutNeighbour(t *testing.T) {
	t.Parallel()
	a := swarm.RandAddress(t)
	far := swarm.RandAddress(t)
	c := newCursorPuller(t, 50*time.Millisecond, "", kadMock.AddrTuple{Addr: a, PO: 2}, kadMock.AddrTuple{Addr: far, PO: 0})
	c.rs.SetStorageRadius(2)
	c.ps.set(a, "error") // the only neighbour never gives its cursors
	c.start(t)

	waitFor(t, 5*time.Second, func() bool { return c.p.OnChangeDurationCount() >= 1 }, "no run completed")
	c.kad.Trigger()
	waitFor(t, 5*time.Second, func() bool { return c.p.OnChangeDurationCount() >= 2 }, "no second run")
	if _, _, ok := acted(c); ok {
		t.Fatal("acted with no neighbour syncing")
	}
}

func TestActedNotRecordedWhileRunBlocked(t *testing.T) {
	t.Parallel()
	b := swarm.RandAddress(t)
	c := newCursorPuller(t, time.Hour, "", kadMock.AddrTuple{Addr: b, PO: 2})
	c.rs.SetStorageRadius(2)
	c.start(t)
	waitActed(t, c, 2)

	// The decrease re-creates B; its cursors request now blocks the run.
	c.ps.set(b, "block")
	c.rs.SetStorageRadius(1)
	c.kad.Trigger()
	waitFor(t, 5*time.Second, func() bool { return c.ps.blocking.Load() == 1 }, "run never blocked")
	if r, _, _ := acted(c); r != 2 {
		t.Fatalf("acted radius %d while the run at radius 1 is blocked, want 2", r)
	}
	close(c.ps.release)
	waitActed(t, c, 1)
}
