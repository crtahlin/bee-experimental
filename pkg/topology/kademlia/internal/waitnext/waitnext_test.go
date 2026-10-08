// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package waitnext_test

import (
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/kademlia/internal/waitnext"
)

const (
	stable  = time.Minute
	maxWait = 15 * time.Minute
	ttl     = time.Hour
	base    = 20 * time.Second
)

// t0 is the current time, because Waiting and Attempts read the real clock.
var t0 = time.Now().Truncate(time.Second)

func newWaitNext() *waitnext.WaitNext {
	return waitnext.New(stable, maxWait, ttl)
}

func tryAfter(t *testing.T, w *waitnext.WaitNext, addr swarm.Address) time.Time {
	t.Helper()
	ta, ok := w.TryAfter(addr)
	if !ok {
		t.Fatal("no entry")
	}
	return ta
}

func wantTryAfter(t *testing.T, w *waitnext.WaitNext, addr swarm.Address, want time.Time) {
	t.Helper()
	if got := tryAfter(t, w, addr); !got.Equal(want) {
		t.Fatalf("tryAfter: got %v, want %v", got.Sub(t0), want.Sub(t0))
	}
}

// shortConnection records one connection to addr that starts at at and ends
// after one second.
func shortConnection(t *testing.T, w *waitnext.WaitNext, addr swarm.Address, at time.Time) {
	t.Helper()
	w.Connected(addr, at)
	if short, _ := w.Disconnected(addr, at.Add(time.Second), base); !short {
		t.Fatal("one-second connection not counted as short-lived")
	}
}

func TestWaitingAndAttempts(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	addr := swarm.RandAddress(t)

	now := time.Now()
	w.Failed(addr, now, now.Add(time.Hour), 2, base)
	if !w.Waiting(addr) {
		t.Fatal("should be waiting")
	}
	if got := w.Attempts(addr); got != 2 {
		t.Fatalf("attempts: got %d, want 2", got)
	}

	w.Remove(addr)
	if w.Waiting(addr) || w.Attempts(addr) != 0 {
		t.Fatal("removed entry still waiting or counting")
	}
}

func TestShortLivedConnection(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	addr := swarm.RandAddress(t)

	w.Failed(addr, t0, t0.Add(base), 2, base)
	w.Connected(addr, t0)
	short, lasted := w.Disconnected(addr, t0.Add(time.Second), base)
	if !short || lasted != time.Second {
		t.Fatalf("got short %v lasted %v, want true and 1s", short, lasted)
	}
	if got := w.ShortLived(addr); got != 1 {
		t.Fatalf("short-lived: got %d, want 1", got)
	}
	if got := w.Attempts(addr); got != 2 {
		t.Fatalf("a short connection reset the failure count: got %d, want 2", got)
	}
	wantTryAfter(t, w, addr, t0.Add(time.Second+base))
}

func TestStableConnectionResets(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	addr := swarm.RandAddress(t)

	shortConnection(t, w, addr, t0)
	shortConnection(t, w, addr, t0.Add(time.Hour))
	w.Failed(addr, t0.Add(2*time.Hour), t0, 3, base)

	at := t0.Add(3 * time.Hour)
	w.Connected(addr, at)
	short, lasted := w.Disconnected(addr, at.Add(stable), base)
	if short || lasted != stable {
		t.Fatalf("got short %v lasted %v, want false and %v", short, lasted, stable)
	}
	if w.ShortLived(addr) != 0 || w.Attempts(addr) != 0 {
		t.Fatalf("stable connection did not reset: short-lived %d attempts %d", w.ShortLived(addr), w.Attempts(addr))
	}
	wantTryAfter(t, w, addr, at.Add(stable+base))
}

func TestBackoffDoublesToCap(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	addr := swarm.RandAddress(t)

	want := []time.Duration{20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 320 * time.Second, 640 * time.Second, 900 * time.Second, 900 * time.Second}
	at := t0
	for i, wantWait := range want {
		shortConnection(t, w, addr, at)
		end := at.Add(time.Second)
		if got := tryAfter(t, w, addr).Sub(end); got != wantWait {
			t.Fatalf("short connection %d: wait %v, want %v", i+1, got, wantWait)
		}
		at = at.Add(time.Hour)
	}
}

func TestSecondDisconnectCountsNothing(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	addr := swarm.RandAddress(t)

	shortConnection(t, w, addr, t0)
	before := tryAfter(t, w, addr)
	if short, _ := w.Disconnected(addr, t0.Add(2*time.Second), base); short {
		t.Fatal("second disconnect of the same connection counted as short-lived")
	}
	if got := w.ShortLived(addr); got != 1 {
		t.Fatalf("short-lived: got %d, want 1", got)
	}
	wantTryAfter(t, w, addr, later(before, t0.Add(2*time.Second+base)))
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func TestConnectedOverwritesStaleStart(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	addr := swarm.RandAddress(t)

	// A stale start an hour old, left by a disconnect that was not seen.
	w.Connected(addr, t0)
	at := t0.Add(time.Hour)
	w.Connected(addr, at)
	if short, _ := w.Disconnected(addr, at.Add(time.Second), base); !short {
		t.Fatal("a stale connection start made a one-second connection look long")
	}
}

// TestTryAfterPerMethod checks, for each method, whether it overwrites the
// next dial time or keeps the later one, against a 15-minute wait that is
// still running.
func TestTryAfterPerMethod(t *testing.T) {
	t.Parallel()

	running := func(t *testing.T) (*waitnext.WaitNext, swarm.Address, time.Time) {
		t.Helper()
		w := newWaitNext()
		addr := swarm.RandAddress(t)
		w.Failed(addr, t0, t0.Add(maxWait), 1, base)
		return w, addr, t0.Add(maxWait)
	}
	now := t0.Add(time.Minute)

	t.Run("connected does not change it", func(t *testing.T) {
		t.Parallel()
		w, addr, wait := running(t)
		w.Connected(addr, now)
		wantTryAfter(t, w, addr, wait)
	})
	t.Run("disconnect without a connection keeps the later", func(t *testing.T) {
		t.Parallel()
		w, addr, wait := running(t)
		w.Disconnected(addr, now, base)
		wantTryAfter(t, w, addr, wait)
	})
	t.Run("short disconnect keeps the later", func(t *testing.T) {
		t.Parallel()
		w, addr, wait := running(t)
		w.Connected(addr, now)
		w.Disconnected(addr, now.Add(time.Second), base)
		wantTryAfter(t, w, addr, wait)
	})
	t.Run("stable disconnect overwrites", func(t *testing.T) {
		t.Parallel()
		w, addr, _ := running(t)
		w.Connected(addr, now)
		end := now.Add(2 * time.Minute)
		w.Disconnected(addr, end, base)
		wantTryAfter(t, w, addr, end.Add(base))
	})
	t.Run("failed keeps the later", func(t *testing.T) {
		t.Parallel()
		w, addr, wait := running(t)
		w.Failed(addr, now, now.Add(base), 2, base)
		wantTryAfter(t, w, addr, wait)
	})
	t.Run("failed waits for retryAt when later", func(t *testing.T) {
		t.Parallel()
		w, addr, _ := running(t)
		w.Failed(addr, now, now.Add(time.Hour), 2, base)
		wantTryAfter(t, w, addr, now.Add(time.Hour))
	})
	t.Run("pruned keeps the later", func(t *testing.T) {
		t.Parallel()
		w, addr, wait := running(t)
		shortConnection(t, w, addr, now)
		w.Pruned(addr, now.Add(2*time.Second), base)
		wantTryAfter(t, w, addr, wait)
	})
}

// TestFailedAfterShortLived checks that a failed dial waits the backoff for
// the short-lived connections so far, not only the plain retry.
func TestFailedAfterShortLived(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	addr := swarm.RandAddress(t)

	shortConnection(t, w, addr, t0)
	shortConnection(t, w, addr, t0.Add(time.Hour))
	now := t0.Add(2 * time.Hour)
	w.Failed(addr, now, now.Add(base), 1, base)
	wantTryAfter(t, w, addr, now.Add(2*base))
}

func TestPruned(t *testing.T) {
	t.Parallel()

	t.Run("no short-lived history deletes the entry", func(t *testing.T) {
		t.Parallel()
		w := newWaitNext()
		addr := swarm.RandAddress(t)
		w.Failed(addr, t0, t0.Add(base), 4, base)
		w.Pruned(addr, t0.Add(base), base)
		if _, ok := w.TryAfter(addr); ok {
			t.Fatal("entry kept for a peer with no short-lived connections")
		}
	})

	t.Run("keeps a reduced entry with a fresh wait", func(t *testing.T) {
		t.Parallel()
		w := newWaitNext()
		addr := swarm.RandAddress(t)
		shortConnection(t, w, addr, t0)
		shortConnection(t, w, addr, t0.Add(time.Hour))
		shortConnection(t, w, addr, t0.Add(2*time.Hour))
		// The pruning dial ran because the wait had passed.
		now := t0.Add(3 * time.Hour)
		w.Failed(addr, now, now, 6, base)
		w.Pruned(addr, now, base)

		if got := w.ShortLived(addr); got != 3 {
			t.Fatalf("short-lived: got %d, want 3", got)
		}
		if got := w.Attempts(addr); got != 0 {
			t.Fatalf("attempts: got %d, want 0", got)
		}
		wantTryAfter(t, w, addr, now.Add(4*base))

		w.Sweep(now.Add(4*base + ttl - time.Second))
		if w.Len() != 1 {
			t.Fatal("swept before the expiry")
		}
		w.Sweep(now.Add(4*base + ttl))
		if w.Len() != 0 {
			t.Fatal("not swept at the expiry")
		}
	})

	t.Run("a later event makes it a normal entry", func(t *testing.T) {
		t.Parallel()
		w := newWaitNext()
		addr := swarm.RandAddress(t)
		shortConnection(t, w, addr, t0)
		w.Pruned(addr, t0.Add(time.Minute), base)
		w.Failed(addr, t0.Add(2*time.Minute), t0.Add(3*time.Minute), 1, base)
		w.Sweep(t0.Add(10 * ttl))
		if w.Len() != 1 {
			t.Fatal("an entry with a later event was swept")
		}
	})
}

func TestSweepKeepsNormalEntries(t *testing.T) {
	t.Parallel()

	w := newWaitNext()
	w.Failed(swarm.RandAddress(t), t0, t0.Add(base), 1, base)
	w.Connected(swarm.RandAddress(t), t0)
	w.Sweep(t0.Add(100 * ttl))
	if got := w.Len(); got != 2 {
		t.Fatalf("got %d entries, want 2", got)
	}
}

// TestExpiredEntryDeletedOnLookup checks that Waiting and Attempts delete a
// reduced entry already past its expiry, and that a later event starts a
// fresh entry rather than reviving the old short-lived count.
func TestExpiredEntryDeletedOnLookup(t *testing.T) {
	t.Parallel()

	// A short TTL, and events an hour in the past, so the real clock in
	// Waiting and Attempts is past the expiry.
	w := waitnext.New(time.Minute, 15*time.Minute, time.Millisecond)
	start := time.Now().Add(-time.Hour)

	expired := func(t *testing.T) swarm.Address {
		t.Helper()
		addr := swarm.RandAddress(t)
		w.Connected(addr, start)
		w.Disconnected(addr, start.Add(time.Second), time.Millisecond)
		w.Pruned(addr, start.Add(2*time.Second), time.Millisecond)
		return addr
	}

	a := expired(t)
	if w.Waiting(a) {
		t.Fatal("expired entry still waiting")
	}
	if _, ok := w.TryAfter(a); ok {
		t.Fatal("Waiting did not delete the expired entry")
	}

	b := expired(t)
	if got := w.Attempts(b); got != 0 {
		t.Fatalf("attempts on an expired entry: got %d, want 0", got)
	}
	if _, ok := w.TryAfter(b); ok {
		t.Fatal("Attempts did not delete the expired entry")
	}

	c := expired(t)
	w.Failed(c, time.Now(), time.Now(), 1, time.Second)
	if got := w.ShortLived(c); got != 0 {
		t.Fatalf("an event on an expired entry kept its short-lived count %d", got)
	}
}
