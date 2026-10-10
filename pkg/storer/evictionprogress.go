// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"sync"
	"time"
)

// Progress of a long eviction (wasp #626,
// docs/experiments/eviction-progress-pause-bound/spec.md). The evicted
// counter and the reserve size already move per round (#623); this adds
// what is left, and a line at most once a minute while a run lasts, so an
// operator can read a long eviction from metrics and the journal.

// progressLineInterval is the shortest gap between two progress lines. A
// variable so a test can shorten it.
var progressLineInterval = time.Minute

const (
	progressKindUnreserve = "unreserve"
	progressKindExpired   = "expired"
)

// evictionRunProgress is the state of the eviction run in progress. The run
// sets it on the DB, so the wait for a sample, which also runs for a radius
// step, writes the line with the run's own kind and counts.
type evictionRunProgress struct {
	mu          sync.Mutex
	active      bool
	kind        string
	evicted     int64
	batchesLeft int // expired runs only
	pausedSince time.Time
	paused      time.Duration
	lastLine    time.Time
	lastEvicted int64
}

func (db *DB) progressStart(kind string, batches int) {
	p := &db.progress
	p.mu.Lock()
	defer p.mu.Unlock()
	now := clock()
	p.active, p.kind, p.batchesLeft = true, kind, batches
	p.evicted, p.lastEvicted, p.lastLine = 0, 0, now
	p.paused, p.pausedSince = 0, time.Time{}
	db.setRemaining(kind, batches)
}

func (db *DB) progressEnd() {
	p := &db.progress
	p.mu.Lock()
	p.active = false
	p.mu.Unlock()
	db.metrics.EvictionRemaining.Set(0)
	db.metrics.EvictionExpiredBatchesRemaining.Set(0)
	if db.reserve != nil {
		db.metrics.EvictionRemaining.Set(float64(max(0, db.reserve.EvictionTarget())))
	}
}

// progressRound records a round of the run, refreshes the gauges and writes
// the line when it is due.
func (db *DB) progressRound(n int) {
	p := &db.progress
	p.mu.Lock()
	if p.active {
		p.evicted += int64(n)
	}
	p.mu.Unlock()
	db.progressLine(false)
}

// progressBatchDone records an expired batch finished.
func (db *DB) progressBatchDone() {
	p := &db.progress
	p.mu.Lock()
	if p.active && p.batchesLeft > 0 {
		p.batchesLeft--
	}
	left := p.batchesLeft
	p.mu.Unlock()
	db.metrics.EvictionExpiredBatchesRemaining.Set(float64(left))
}

func (db *DB) setRemaining(kind string, batches int) {
	if db.reserve != nil {
		db.metrics.EvictionRemaining.Set(float64(max(0, db.reserve.EvictionTarget())))
	}
	if kind == progressKindExpired {
		db.metrics.EvictionExpiredBatchesRemaining.Set(float64(batches))
	}
}

// progressLine refreshes the remaining gauge and, at most once per
// progressLineInterval while a run lasts, writes the progress line. paused
// is true when it is written from the wait for a sample.
func (db *DB) progressLine(paused bool) {
	if db.reserve != nil {
		db.metrics.EvictionRemaining.Set(float64(max(0, db.reserve.EvictionTarget())))
	}
	p := &db.progress
	p.mu.Lock()
	now := clock()
	switch {
	case paused && p.pausedSince.IsZero():
		p.pausedSince = now
	case !paused && !p.pausedSince.IsZero():
		p.paused += now.Sub(p.pausedSince)
		p.pausedSince = time.Time{}
	}
	if !p.active || now.Sub(p.lastLine) < progressLineInterval {
		p.mu.Unlock()
		return
	}
	elapsed := now.Sub(p.lastLine)
	rate := float64(p.evicted-p.lastEvicted) / elapsed.Seconds()
	pausedFor := p.paused
	if !p.pausedSince.IsZero() {
		pausedFor += now.Sub(p.pausedSince)
	}
	kind, evicted, batchesLeft := p.kind, p.evicted, p.batchesLeft
	p.lastLine, p.lastEvicted = now, p.evicted
	p.mu.Unlock()

	configured, workers := db.evictionRateAndWorkers()
	kv := []any{
		"kind", kind,
		"evicted", evicted,
		"rate_last_interval", int64(rate),
		"rate", configured,
		"workers", workers,
		"paused_for", pausedFor.Round(time.Second),
		"paused", paused,
	}
	if kind == progressKindExpired {
		kv = append(kv, "batches_left", batchesLeft)
	} else if db.reserve != nil {
		remaining := max(0, db.reserve.EvictionTarget())
		kv = append(kv, "remaining", remaining)
		if rate > 0 {
			kv = append(kv, "eta", (time.Duration(float64(remaining)/rate) * time.Second).Round(time.Second))
		}
	}
	db.logger.Info("reserve eviction progress", kv...)
}
