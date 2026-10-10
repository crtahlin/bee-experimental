// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"time"
)

// Two kinds of reserve sample pause eviction (wasp #659,
// docs/experiments/eviction-progress-pause-bound/spec.md).
//
// The storage lottery's sample pauses eviction for its whole duration, as
// since #649, capped per continuous stretch as a guard against a stuck
// sample. Any other sample (/rchash, tests) shares a budget: eviction waits
// for such samples for at most maxEvictionPause of waited time in any
// window of twice that, counting the wait in progress. Back-to-back
// /rchash calls therefore cannot hold eviction off indefinitely, and
// eviction gets at least half of every window.

type lotterySampleKey struct{}

// WithLotterySample marks ctx as the storage lottery's sample. The storage
// incentives agent sets it in makeSample; /rchash goes through the same
// agent path without it.
func WithLotterySample(ctx context.Context) context.Context {
	return context.WithValue(ctx, lotterySampleKey{}, true)
}

// IsLotterySample reports whether ctx carries WithLotterySample.
func IsLotterySample(ctx context.Context) bool {
	return isLotterySample(ctx)
}

func isLotterySample(ctx context.Context) bool {
	v, _ := ctx.Value(lotterySampleKey{}).(bool)
	return v
}

// clock and samplingPausePollVar are variables so a test can drive the
// pause deterministically.
var (
	clock                = time.Now
	samplingPausePollVar = samplingPausePoll
)

type waitInterval struct{ start, end time.Time }

// pauseBudget is the waited time for non-lottery samples. Guarded by
// db.samplingMu.
type pauseBudget struct {
	intervals []waitInterval // closed waits, pruned to the window on insert
	open      time.Time      // start of the wait in progress, zero if none
	atBound   bool           // the budget was used up at the last check
	lastWarn  time.Time
}

func budgetWindow() time.Duration { return 2 * maxEvictionPause }

// waited returns the waited time inside the window ending at now, counting
// the wait in progress; an interval that crosses the window's start counts
// only its part inside.
func (b *pauseBudget) waited(now time.Time) time.Duration {
	from := now.Add(-budgetWindow())
	var d time.Duration
	add := func(s, e time.Time) {
		if s.Before(from) {
			s = from
		}
		if e.After(s) {
			d += e.Sub(s)
		}
	}
	for _, iv := range b.intervals {
		add(iv.start, iv.end)
	}
	if !b.open.IsZero() {
		add(b.open, now)
	}
	return d
}

// setOpen starts or ends the wait in progress. A closed wait is added to
// the list, and intervals that left the window are dropped.
func (b *pauseBudget) setOpen(open bool, now time.Time) {
	switch {
	case open && b.open.IsZero():
		b.open = now
	case !open && !b.open.IsZero():
		b.intervals = append(b.intervals, waitInterval{b.open, now})
		b.open = time.Time{}
		from := now.Add(-budgetWindow())
		kept := b.intervals[:0]
		for _, iv := range b.intervals {
			if iv.end.After(from) {
				kept = append(kept, iv)
			}
		}
		b.intervals = kept
	}
}

// samplePause is why eviction waits.
type samplePause int

const (
	pauseNone     samplePause = iota
	pauseLottery              // a lottery sample runs, within its stretch cap
	pauseForOther             // only other samples run, within the budget
)

// evictionPauseReason reports whether eviction must wait, and why. Called
// with db.samplingMu held.
func (db *DB) evictionPauseReasonLocked(now time.Time) samplePause {
	if db.samplingCount > 0 {
		if now.Sub(db.samplingSince) < maxEvictionPause {
			return pauseLottery
		}
		if !db.samplingCapWarned {
			db.samplingCapWarned = true
			db.metrics.EvictionPauseCapped.Inc()
			db.logger.Warning("reserve eviction resumed after a sample ran for too long", "sampling_for", now.Sub(db.samplingSince).Round(time.Second), "cap", maxEvictionPause)
		}
	}
	if db.otherSamplingCount == 0 {
		return pauseNone
	}
	b := &db.pauseBudget
	waited := b.waited(now)
	if waited < maxEvictionPause {
		b.atBound = false
		return pauseForOther
	}
	if !b.atBound {
		b.atBound = true
		db.metrics.EvictionPauseCapped.Inc()
		if b.lastWarn.IsZero() || now.Sub(b.lastWarn) >= budgetWindow() {
			b.lastWarn = now
			db.logger.Warning("reserve eviction resumed: samples other than the lottery used up their share of the window", "waited", waited.Round(time.Second), "window", budgetWindow())
		}
	}
	return pauseNone
}
