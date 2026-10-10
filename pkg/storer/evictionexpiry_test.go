// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"errors"
	"testing"
	"time"
)

// TestExpiryRunNeverOpensEpisode checks that an expiry run alone never
// makes the node evicting, however long it runs (#663).
func TestExpiryRunNeverOpensEpisode(t *testing.T) {
	t.Parallel()

	var e evictionEpisode
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	e.expiryStarted(t0)
	for _, d := range []time.Duration{time.Second, EvictingMinAge, 5 * time.Minute} {
		if got := e.activeFor(at(d)); got != 0 {
			t.Fatalf("during an expiry run, at %v: %v, want 0", d, got)
		}
	}
	if got := e.expiryActiveFor(at(5 * time.Minute)); got != 5*time.Minute {
		t.Fatalf("expiry run active time: %v, want 5m", got)
	}
	if got := e.expiryEnded(at(6*time.Minute), false); got != 6*time.Minute {
		t.Fatalf("expiry run returned %v, want 6m", got)
	}
	if got := e.activeFor(at(6 * time.Minute)); got != 0 {
		t.Fatalf("after the expiry run: %v, want 0", got)
	}
	if got := e.expiryActiveFor(at(6 * time.Minute)); got != 0 {
		t.Fatalf("expiry gauge after the run: %v, want 0", got)
	}
}

// TestExpiryRunHoldsOpenEpisode checks that an expiry run inside an open
// episode keeps it open past the idle grace without adding active time,
// and that the episode continues afterwards (#663).
func TestExpiryRunHoldsOpenEpisode(t *testing.T) {
	t.Parallel()

	var e evictionEpisode
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	e.runStarted(t0)
	e.runEnded(at(30*time.Second), false) // interrupted, work left
	e.expiryStarted(at(30 * time.Second))
	// Both reads are past the idle grace after the unreserve run ended.
	for _, d := range []time.Duration{45 * time.Second, 110 * time.Second} {
		if got := e.activeFor(at(d)); got != 30*time.Second {
			t.Fatalf("during the hold, at %v: %v, want 30s (open, frozen)", d, got)
		}
	}
	e.expiryEnded(at(120*time.Second), false)
	if got := e.activeFor(at(125 * time.Second)); got != 30*time.Second {
		t.Fatalf("after the hold, within the grace: %v, want 30s", got)
	}
	e.runStarted(at(125 * time.Second))
	e.runEnded(at(165*time.Second), false)
	if got := e.activeFor(at(165 * time.Second)); got != 70*time.Second {
		t.Fatalf("after the last run: %v, want 70s", got)
	}
	if got := e.activeFor(at(165*time.Second + episodeIdleGrace + time.Second)); got != 0 {
		t.Fatalf("after the idle grace: %v, want 0", got)
	}
}

// TestExpiryRunDoesNotReviveStaleEpisode checks that a hold starting after
// the idle grace closes the stale episode instead of reviving it (#663).
func TestExpiryRunDoesNotReviveStaleEpisode(t *testing.T) {
	t.Parallel()

	var e evictionEpisode
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	e.runStarted(t0)
	e.runEnded(at(30*time.Second), false) // work left, no read follows
	e.expiryStarted(at(30*time.Second + episodeIdleGrace + 5*time.Second))
	if got := e.activeFor(at(2 * time.Minute)); got != 0 {
		t.Fatalf("during a hold that started past the grace: %v, want 0", got)
	}
	e.expiryEnded(at(3*time.Minute), false)
	if got := e.activeFor(at(3 * time.Minute)); got != 0 {
		t.Fatalf("after it: %v, want 0", got)
	}
}

// TestExpiryRunPausesDoNotTouchHeldEpisode checks that a pause during an
// expiry run is counted against the expiry run, and that a shutdown during
// a hold closes the episode (#663).
func TestExpiryRunPausesDoNotTouchHeldEpisode(t *testing.T) {
	t.Parallel()

	var e evictionEpisode
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	e.runStarted(t0)
	e.runEnded(at(30*time.Second), false)
	e.expiryStarted(at(30 * time.Second))
	e.pauseStarted(at(40 * time.Second))
	if got := e.expiryActiveFor(at(50 * time.Second)); got != 10*time.Second {
		t.Fatalf("expiry run during a pause: %v, want 10s", got)
	}
	e.pauseEnded(20 * time.Second)
	if got := e.expiryEnded(at(70*time.Second), false); got != 20*time.Second {
		t.Fatalf("expiry run active time: %v, want 20s (40s minus 20s paused)", got)
	}
	e.runStarted(at(70 * time.Second))
	if got := e.activeFor(at(80 * time.Second)); got != 40*time.Second {
		t.Fatalf("episode after the paused hold: %v, want 40s (pause not counted against it)", got)
	}
	e.runEnded(at(80*time.Second), false)

	e.expiryStarted(at(85 * time.Second))
	e.expiryEnded(at(90*time.Second), true) // shutdown
	if got := e.activeFor(at(90 * time.Second)); got != 0 {
		t.Fatalf("after a shutdown during a hold: %v, want 0", got)
	}
}

// TestExpiryRunEndsOnError checks that an expiry run that returns an error
// still ends: the hold is released and the expiry gauge returns to 0 (#663).
func TestExpiryRunEndsOnError(t *testing.T) {
	t.Parallel()

	db := &DB{metrics: newMetrics()}
	db.episode.runStarted(time.Now())
	db.episode.runEnded(time.Now(), false) // an open episode with work left
	err := db.evictionRun(evictionKindExpiry, func() error { return errors.New("expiry failed") })
	if err == nil {
		t.Fatal("want the run's error")
	}
	if got := db.episode.expiryActiveFor(time.Now()); got != 0 {
		t.Fatalf("expiry gauge after a failed run: %v, want 0", got)
	}
	db.episode.mu.Lock()
	held := db.episode.held
	db.episode.mu.Unlock()
	if held {
		t.Fatal("episode still held after a failed expiry run")
	}
}
