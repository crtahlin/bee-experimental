// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"testing"
	"time"
)

// TestEvictionEpisodeAcrossRuns checks that an eviction interrupted by a
// batch expiry and restarted stays one episode, that its active time keeps
// growing across the runs, and that the episode closes after the idle grace
// when no run follows, or at once when no work is left (#649).
func TestEvictionEpisodeAcrossRuns(t *testing.T) {
	t.Parallel()

	var e evictionEpisode
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	if got := e.activeFor(t0); got != 0 {
		t.Fatalf("before any run: %v, want 0", got)
	}
	e.runStarted(t0)
	if got := e.activeFor(at(30 * time.Second)); got != 30*time.Second {
		t.Fatalf("during the first run: %v, want 30s", got)
	}
	// Interrupted by an expiry with work left.
	e.runEnded(at(40*time.Second), false)
	if got := e.activeFor(at(45 * time.Second)); got != 40*time.Second {
		t.Fatalf("between runs, within the grace: %v, want 40s", got)
	}
	e.runStarted(at(45 * time.Second))
	if got := e.activeFor(at(75 * time.Second)); got != 70*time.Second {
		t.Fatalf("second run of the same episode: %v, want 70s", got)
	}
	if e.activeFor(at(75*time.Second)) < EvictingMinAge {
		t.Fatal("an interrupted eviction never counts as evicting")
	}
	e.runEnded(at(80*time.Second), false)
	if got := e.activeFor(at(80*time.Second + episodeIdleGrace + time.Second)); got != 0 {
		t.Fatalf("after the idle grace: %v, want 0", got)
	}
	// A new run after the grace starts a new episode.
	e.runStarted(at(100 * time.Second))
	if got := e.activeFor(at(110 * time.Second)); got != 10*time.Second {
		t.Fatalf("new episode: %v, want 10s", got)
	}
	// No work left closes the episode at once.
	e.runEnded(at(111*time.Second), true)
	if got := e.activeFor(at(111 * time.Second)); got != 0 {
		t.Fatalf("after a run that left no work: %v, want 0", got)
	}
}

// TestEvictionEpisodeExcludesPauses checks that time paused for a sample
// does not count: a short top-up paused for minutes stays short (#649).
func TestEvictionEpisodeExcludesPauses(t *testing.T) {
	t.Parallel()

	var e evictionEpisode
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	e.runStarted(t0)
	e.pauseStarted(at(time.Second))
	if got := e.activeFor(at(5 * time.Minute)); got != time.Second {
		t.Fatalf("during the pause: %v, want 1s", got)
	}
	e.pauseEnded(5*time.Minute - time.Second)
	if got := e.activeFor(at(5*time.Minute + time.Second)); got != 2*time.Second {
		t.Fatalf("after the pause: %v, want 2s", got)
	}
	e.runEnded(at(5*time.Minute+time.Second), false)
	if got := e.activeFor(at(5*time.Minute + 2*time.Second)); got != 2*time.Second {
		t.Fatalf("after the run: %v, want 2s", got)
	}
}
