// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Eviction and reserve samples (wasp #649,
// docs/experiments/lottery-skip-while-evicting/spec.md).
//
// No eviction round and no radius step runs while a reserve sample runs, so
// a sample sees the reserve it started on. Eviction holds evictionMu across
// one round and across a radius step, and checks the sampling counter under
// it; a sample increments the counter and then takes and releases
// evictionMu once, so the round or step in progress, if any, finishes
// before the sample reads the radius, and every later one waits. The wait
// is capped per continuous stretch of sampling.
//
// Separately, an eviction episode measures how long the node has been
// evicting, across reserve-worker runs, so the storage incentives agent can
// sit out a round while a large eviction runs.

const (
	// EvictingMinAge is how long an eviction episode must have been active
	// before the node counts as evicting and sits out a lottery round.
	// Routine top-ups of about ReserveMinEvictCount chunks take seconds.
	EvictingMinAge = time.Minute
	// episodeIdleGrace is how long after a run that left work an episode
	// stays open without a new run.
	episodeIdleGrace = 10 * time.Second
	// samplingPausePoll is how often a paused eviction checks whether the
	// sample has ended, as the puller does.
	samplingPausePoll = 250 * time.Millisecond
)

// maxEvictionPause caps how long eviction waits for one continuous stretch
// of sampling. A variable so tests can shorten it.
var maxEvictionPause = 15 * time.Minute

// sampleStarted registers a running sample and waits for the eviction round
// or radius step in progress, if any. It must be called before the sample
// reads the radius; sampleDone undoes it.
func (db *DB) sampleStarted() {
	db.samplingMu.Lock()
	if db.samplingCount == 0 {
		db.samplingSince = time.Now()
		db.samplingCapWarned = false
	}
	db.samplingCount++
	db.samplingMu.Unlock()
	db.samplingActive.Add(1)

	db.evictionMu.Lock()
	db.evictionMu.Unlock() //nolint:staticcheck // a barrier: wait for the round in progress
}

func (db *DB) sampleDone() {
	db.samplingMu.Lock()
	db.samplingCount--
	db.samplingMu.Unlock()
	db.samplingActive.Add(-1)
}

// evictionShouldWait reports whether eviction must wait for a sample: one
// runs and the current stretch of sampling has not reached the cap. The
// first check past the cap in a stretch logs a Warning.
func (db *DB) evictionShouldWait() bool {
	db.samplingMu.Lock()
	defer db.samplingMu.Unlock()
	if db.samplingCount == 0 {
		return false
	}
	if time.Since(db.samplingSince) < maxEvictionPause {
		return true
	}
	if !db.samplingCapWarned {
		db.samplingCapWarned = true
		db.metrics.EvictionPauseCapped.Inc()
		db.logger.Warning("reserve eviction resumed after a sample ran for too long", "sampling_for", time.Since(db.samplingSince).Round(time.Second), "cap", maxEvictionPause)
	}
	return false
}

// waitWhileSampling waits until eviction may proceed. A shutdown, a batch
// expiry and the context end the wait, checked in that order. A nil expiry
// never fires. The paused time is counted and left out of the eviction
// episode's active time and of the pacer's headroom.
func (db *DB) waitWhileSampling(ctx context.Context, expiry <-chan struct{}) error {
	if !db.evictionShouldWait() {
		return nil
	}
	start := time.Now()
	db.episode.pauseStarted(start)
	db.logger.Debug("reserve eviction paused while a sample runs")
	defer func() {
		d := time.Since(start)
		db.episode.pauseEnded(d)
		db.metrics.EvictionPausedSeconds.Add(d.Seconds())
		if p := db.evictionPacer; p != nil {
			p.shiftHeadroom(d)
		}
		db.logger.Debug("reserve eviction resumed after a sample", "paused", d)
	}()
	t := time.NewTicker(samplingPausePoll)
	defer t.Stop()
	for db.evictionShouldWait() {
		select {
		case <-db.quit:
			return ErrDBQuit
		default:
		}
		select {
		case <-db.quit:
			return ErrDBQuit
		case <-expiry:
			return errEvictionExpiry
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

// evictionBarrier takes evictionMu once no sample needs the reserve to stay
// still, waiting outside the lock while one does. The caller runs one round
// or one radius step and then calls the returned release.
func (db *DB) evictionBarrier(ctx context.Context, expiry <-chan struct{}) (func(), error) {
	for {
		db.evictionMu.Lock()
		if !db.evictionShouldWait() {
			return db.evictionMu.Unlock, nil
		}
		db.evictionMu.Unlock()
		if err := db.waitWhileSampling(ctx, expiry); err != nil {
			return nil, err
		}
	}
}

// evictionEpisode tracks how long the node has been evicting, across
// reserve-worker runs. Runs are started and ended by the reserve worker;
// EvictingFor may be read from any goroutine.
type evictionEpisode struct {
	mu sync.Mutex

	open       bool
	active     time.Duration // active time of the episode's finished runs
	runStart   time.Time     // zero when no run is in progress
	runPaused  time.Duration // paused time of the run in progress
	pauseStart time.Time     // zero when no pause is in progress
	lastRunEnd time.Time
}

// expiredLocked closes an episode whose last run ended more than the idle
// grace ago with no run since.
func (e *evictionEpisode) expiredLocked(now time.Time) bool {
	if e.open && e.runStart.IsZero() && now.Sub(e.lastRunEnd) > episodeIdleGrace {
		e.open = false
		e.active = 0
	}
	return !e.open
}

func (e *evictionEpisode) runStarted(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.expiredLocked(now) {
		e.open = true
		e.active = 0
	}
	e.runStart = now
	e.runPaused = 0
	e.pauseStart = time.Time{}
}

// runEnded ends the run in progress; done closes the episode, because no
// eviction work is left.
func (e *evictionEpisode) runEnded(now time.Time, done bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.runStart.IsZero() {
		e.active += max(0, now.Sub(e.runStart)-e.runPaused)
	}
	e.runStart = time.Time{}
	e.runPaused = 0
	e.pauseStart = time.Time{}
	e.lastRunEnd = now
	if done {
		e.open = false
		e.active = 0
	}
}

func (e *evictionEpisode) pauseStarted(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pauseStart = now
}

func (e *evictionEpisode) pauseEnded(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.runStart.IsZero() {
		e.runPaused += d
	}
	e.pauseStart = time.Time{}
}

// activeFor returns the active time of the open episode, or 0.
func (e *evictionEpisode) activeFor(now time.Time) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.expiredLocked(now) {
		return 0
	}
	d := e.active
	if !e.runStart.IsZero() {
		run := now.Sub(e.runStart) - e.runPaused
		if !e.pauseStart.IsZero() {
			run -= now.Sub(e.pauseStart)
		}
		d += max(0, run)
	}
	return d
}

// EvictingFor returns how long the current eviction episode has been
// actively evicting, not counting time paused for a sample, or 0 when no
// episode is open. The storage incentives agent sits out a round when it is
// at least EvictingMinAge (#649).
func (db *DB) EvictingFor() time.Duration {
	return db.episode.activeFor(time.Now())
}

// evictionRun runs one eviction run of the reserve worker inside the
// current episode, and closes the episode when no work is left or the node
// is stopping.
func (db *DB) evictionRun(run func() error) error {
	db.episode.runStarted(time.Now())
	err := run()
	done := errors.Is(err, ErrDBQuit) || !db.evictionWorkLeft()
	db.episode.runEnded(time.Now(), done)
	return err
}

// evictionWorkLeft reports whether the reserve is over capacity or an
// expired batch is still recorded for eviction.
func (db *DB) evictionWorkLeft() bool {
	if db.reserve != nil && db.reserve.EvictionTarget() > 0 {
		return true
	}
	batches, err := db.getExpiredBatches()
	return err != nil || len(batches) > 0
}
