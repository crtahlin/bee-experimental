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

// This file holds the decision to lower the storage radius. The radius is
// lowered when the puller reports that the historical sync for the current
// radius is finished and the count of chunks within the radius is below the
// threshold. Today's rule, a historical sync rate of 0, remains as a
// fallback. See docs/experiments/radius-on-sync-done/spec.md and issue #588.

// radiusTrigger says what started a radius check. It is the trigger label of
// the radius_check_total metric.
type radiusTrigger string

const (
	triggerTicker   radiusTrigger = "ticker"
	triggerSyncDone radiusTrigger = "sync_done"
	triggerExpiry   radiusTrigger = "expiry"
)

// Result labels of the radius_check_total metric.
const (
	radiusLowered           = "lowered"
	radiusAboveThreshold    = "above_threshold"
	radiusSyncPending       = "sync_pending"
	radiusHeld              = "held"
	radiusPostponedSampling = "postponed_sampling"
	radiusMinimum           = "minimum"
	radiusSkippedScan       = "skipped_scan"
	radiusFallback          = "fallback"
)

const (
	// defaultHoldAfterDecrease is how long no decrease follows a decrease,
	// and how long none follows the start of the reserve worker.
	defaultHoldAfterDecrease = 2 * time.Minute
	// defaultHoldAfterRaise is how long no decrease follows a raise made by
	// unreserve. Together with the hold above it limits how often a cycle of
	// lowering, going over capacity and raising can repeat.
	defaultHoldAfterRaise = 15 * time.Minute
	// defaultScanInterval is the shortest time between two scans started by
	// a sync signal or a batch expiry.
	defaultScanInterval = 2 * time.Minute
	// defaultSamplingPoll is how often a check postponed by a running
	// sample looks again.
	defaultSamplingPoll = 30 * time.Second
	// fallbackChecks is how many ticker checks in a row today's rule must
	// hold, while the new rule does not lower the radius, before today's
	// rule lowers it.
	fallbackChecks = 4
)

// radiusTimings are the hold times and intervals of the radius decision.
// They are constants in production; they are fields so that a test can
// shorten them for one store.
type radiusTimings struct {
	holdAfterDecrease time.Duration
	holdAfterRaise    time.Duration
	scanInterval      time.Duration
	samplingPoll      time.Duration
}

func defaultRadiusTimings() radiusTimings {
	return radiusTimings{
		holdAfterDecrease: defaultHoldAfterDecrease,
		holdAfterRaise:    defaultHoldAfterRaise,
		scanInterval:      defaultScanInterval,
		samplingPoll:      defaultSamplingPoll,
	}
}

// withinRadiusCount is the last count of reserve chunks within the storage
// radius, and the radius it was counted at. It feeds /status, the
// reserve_size_within_radius metric and the radius decision.
type withinRadiusCount struct {
	mtx    sync.Mutex
	count  int
	radius uint8
	set    bool
}

// setWithinRadius stores a count of chunks within the radius and the radius
// it was counted at.
func (db *DB) setWithinRadius(count int, radius uint8) {
	db.withinRadius.mtx.Lock()
	db.withinRadius.count = count
	db.withinRadius.radius = radius
	db.withinRadius.set = true
	db.withinRadius.mtx.Unlock()
	db.metrics.ReserveSizeWithinRadius.Set(float64(count))
}

// storedWithinRadius returns the last stored count and its radius. ok is
// false before the first count.
func (db *DB) storedWithinRadius() (count int, radius uint8, ok bool) {
	db.withinRadius.mtx.Lock()
	defer db.withinRadius.mtx.Unlock()
	return db.withinRadius.count, db.withinRadius.radius, db.withinRadius.set
}

// radiusChecker is the state of the radius decision. It is owned by the
// reserve worker goroutine and needs no lock.
type radiusChecker struct {
	db *DB

	// holdUntil is the earliest time the radius may be lowered.
	holdUntil time.Time
	// lastScan is when the last scan started by a signal or an expiry ran.
	lastScan time.Time
	// lastSweep is when the ticker last ran the combined pass.
	lastSweep time.Time

	// retry is the one timer that runs a refused signal or expiry check
	// again. retryC is nil while the timer is not armed, so a select on it
	// never fires.
	retry        *time.Timer
	retryC       <-chan time.Time
	retryAt      time.Time
	retryTrigger radiusTrigger

	// fallbackRuns counts the ticker checks in a row in which today's rule
	// held but the new rule did not lower the radius. fallbackRadius is the
	// radius those checks saw; a change resets the count.
	fallbackRuns   int
	fallbackRadius uint8
}

func newRadiusChecker(db *DB, now time.Time) *radiusChecker {
	return &radiusChecker{
		db:        db,
		holdUntil: now.Add(db.radiusTimings.holdAfterDecrease),
		lastSweep: now,
	}
}

func (c *radiusChecker) stop() {
	if c.retry != nil {
		c.retry.Stop()
	}
}

// arm sets the retry timer to the earliest of its current time and at. An
// expiry among the refused checks makes the retry an expiry check, so that
// it still scans.
func (c *radiusChecker) arm(at time.Time, trigger radiusTrigger) {
	if c.retryC == nil || trigger == triggerExpiry {
		c.retryTrigger = trigger
	}
	if c.retryC != nil && !at.Before(c.retryAt) {
		return
	}
	d := time.Until(at)
	if c.retry == nil {
		c.retry = time.NewTimer(d)
	} else {
		c.retry.Stop()
		c.retry.Reset(d)
	}
	c.retryC = c.retry.C
	c.retryAt = at
}

// fired disarms the retry timer after it fired and returns the trigger of
// the check to run.
func (c *radiusChecker) fired() radiusTrigger {
	c.retryC = nil
	return c.retryTrigger
}

// raised records a radius increase made by unreserve.
func (c *radiusChecker) raised(now time.Time) {
	if until := now.Add(c.db.radiusTimings.holdAfterRaise); until.After(c.holdUntil) {
		c.holdUntil = until
	}
}

func (c *radiusChecker) record(trigger radiusTrigger, result string) {
	c.db.metrics.RadiusCheck.WithLabelValues(string(trigger), result).Inc()
}

func (c *radiusChecker) lower(radius uint8) {
	db := c.db
	radius--
	if err := db.reserve.SetRadius(radius); err != nil {
		db.logger.Error(err, "reserve set radius")
	}
	db.metrics.StorageRadius.Set(float64(radius))
	db.logger.Info("reserve radius decrease", "radius", radius)
	c.holdUntil = time.Now().Add(db.radiusTimings.holdAfterDecrease)
	c.fallbackRuns = 0
	c.fallbackRadius = radius
}

// check runs one radius check. It returns ErrDBQuit when the store shuts
// down; any other failure is logged.
func (c *radiusChecker) check(ctx context.Context, trigger radiusTrigger) error {
	if trigger != triggerTicker {
		return c.checkOnSignal(trigger)
	}

	radius := c.db.reserve.Radius()
	// The ticker scans unconditionally, as before, so batch reconciliation,
	// the within-radius metric and /status stay current.
	count, err := c.db.reserveWakeupScan(ctx, &c.lastSweep)
	if err != nil {
		if errors.Is(err, ErrDBQuit) {
			return err
		}
		c.db.logger.Warning("reserve worker count within radius", "error", err)
		return nil
	}
	c.checkOnTicker(radius, count)
	return nil
}

// checkOnTicker evaluates the rule with the count the ticker scan just
// made, and today's rule for the fallback.
func (c *radiusChecker) checkOnTicker(radius uint8, count int) {
	db := c.db
	if radius != c.fallbackRadius {
		c.fallbackRuns = 0
		c.fallbackRadius = radius
	}
	if radius <= db.reserveOptions.minimumRadius {
		c.fallbackRuns = 0
		c.record(triggerTicker, radiusMinimum)
		return
	}

	thr := threshold(db.reserve.Capacity())
	// Today's rule, with the peer guard it lost in 2023: a rate of 0 with no
	// neighbour means nothing is synced, not that sync is finished.
	oldRule := count < thr && db.syncer.SyncRate() == 0 && db.syncer.HasNeighbour(radius)
	if !oldRule {
		c.fallbackRuns = 0
	}

	var result string
	switch {
	case time.Now().Before(c.holdUntil):
		result = radiusHeld
	case db.IsSampling():
		result = radiusPostponedSampling
	case !db.syncer.HistoricalSyncDone(radius):
		result = radiusSyncPending
	case count >= thr:
		result = radiusAboveThreshold
	default:
		c.lower(radius)
		c.record(triggerTicker, radiusLowered)
		return
	}

	if oldRule {
		c.fallbackRuns++
		if c.fallbackRuns >= fallbackChecks && result != radiusHeld && result != radiusPostponedSampling {
			db.logger.Warning("reserve radius lowered by the sync rate rule: historical sync was not reported finished on consecutive checks while the sync rate was 0", "checks", c.fallbackRuns, "radius", radius-1)
			c.lower(radius)
			c.record(triggerTicker, radiusFallback)
			return
		}
	}
	c.record(triggerTicker, result)
}

// checkOnSignal evaluates the rule after a sync signal or a batch expiry.
// The cheap conditions come first; the reserve is scanned only when the
// count is not already known.
func (c *radiusChecker) checkOnSignal(trigger radiusTrigger) error {
	db := c.db
	now := time.Now()
	radius := db.reserve.Radius()

	if radius <= db.reserveOptions.minimumRadius {
		c.record(trigger, radiusMinimum)
		return nil
	}
	if now.Before(c.holdUntil) {
		c.arm(c.holdUntil, trigger)
		c.record(trigger, radiusHeld)
		return nil
	}
	if db.IsSampling() {
		c.arm(now.Add(db.radiusTimings.samplingPoll), trigger)
		c.record(trigger, radiusPostponedSampling)
		return nil
	}
	if !db.syncer.HistoricalSyncDone(radius) {
		c.record(trigger, radiusSyncPending)
		return nil
	}

	thr := threshold(db.reserve.Capacity())
	// The reserve size is current and at least the count within radius, so
	// a size below the threshold settles it without a scan.
	if db.reserve.Size() < thr {
		c.lower(radius)
		c.record(trigger, radiusLowered)
		return nil
	}

	stored, storedRadius, ok := db.storedWithinRadius()
	if ok && storedRadius == radius && stored >= thr && trigger != triggerExpiry {
		c.record(trigger, radiusSkippedScan)
		return nil
	}

	if !c.lastScan.IsZero() && now.Before(c.lastScan.Add(db.radiusTimings.scanInterval)) {
		c.arm(c.lastScan.Add(db.radiusTimings.scanInterval), trigger)
		c.record(trigger, radiusHeld)
		return nil
	}
	c.lastScan = now

	count, err := db.countChunksWithinRadius()
	if err != nil {
		if errors.Is(err, ErrDBQuit) {
			return err
		}
		db.logger.Warning("reserve worker count within radius", "error", err)
		return nil
	}
	db.setWithinRadius(count, radius)

	if count >= thr {
		c.record(trigger, radiusAboveThreshold)
		return nil
	}
	c.lower(radius)
	c.record(trigger, radiusLowered)
	return nil
}
