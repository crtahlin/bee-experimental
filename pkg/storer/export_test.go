// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"io"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/events"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func (db *DB) Reserve() *reserve.Reserve {
	return db.reserve
}

func (db *DB) Events() *events.Subscriber {
	return db.events
}

func ReplaceSharkyShardLimit(val int) {
	sharkyNoOfShards = val
}

func (db *DB) WaitForBgCacheWorkers() (unblock func()) {
	for range defaultBgCacheWorkers {
		db.cacheLimiter.sem <- struct{}{}
	}
	return func() {
		for range defaultBgCacheWorkers {
			<-db.cacheLimiter.sem
		}
	}
}

func DefaultOptions() *Options {
	return defaultOptions()
}

// CountWithinRadius exposes the reserve scan so a test can assert how many
// batch-existence lookups it makes.
func (db *DB) CountWithinRadius(ctx context.Context) (int, error) {
	return db.countWithinRadius(ctx)
}

// CountChunksWithinRadius exposes the cheap within-radius count the frequent
// wake-up uses, so a test can assert it agrees with the combined scan. See #28.
func (db *DB) CountChunksWithinRadius() (int, error) {
	return db.countChunksWithinRadius()
}

// MaxSamplerSortWindow exposes the read-ordering window cap so a test can
// assert an oversized setting is clamped to it rather than honoured.
const MaxSamplerSortWindow = maxSamplerSortWindow

// AcquireSlot exposes the ReserveHas gate so its bounding and its shutdown
// escape can be tested without standing up a store.
func AcquireSlot(limiter chan struct{}, quit chan struct{}) (func(), error) {
	return acquireSlot(limiter, quit)
}

// ReserveHasSlots exposes the semaphore constructor so a test can assert that
// an unbounded setting really produces no semaphore rather than one of size
// zero, which would block every caller for ever.
func ReserveHasSlots(n int) chan struct{} { return reserveHasSlots(n) }

// ReserveHasLimit reports the bound the store was built with, 0 when the
// lookups are unbounded. Used to assert the option reaches the store rather
// than being read into a field nothing consults.
func (db *DB) ReserveHasLimit() int { return cap(db.reserveHasLimiter) }

// ReserveScanDuration exposes the scan-duration histogram so a test can assert
// a pass was actually timed, rather than that the code compiles.
func (db *DB) ReserveScanDuration() *prometheus.HistogramVec {
	return db.metrics.ReserveScanDuration
}

// Write-pause edge classification, exposed so its once-per-transition
// behaviour can be tested without driving a real store to its pause trigger.
type WritePauseChange = writePauseChange

const (
	WritePauseUnchanged = writePauseUnchanged
	WritePauseEntered   = writePauseEntered
	WritePauseLeft      = writePauseLeft
)

func WritePauseEdge(prev, cur bool) WritePauseChange { return writePauseEdge(prev, cur) }

// Windowed-proof test hooks (#273).

var (
	WindowDepthIncrement = windowDepthIncrement
	DeriveWindowAnchor   = deriveWindowAnchor
)

const (
	MinWindowCount          = minWindowCount
	MaxWindowDepthIncrement = maxWindowDepthIncrement
)

// ReserveSampleWithWindow drives the shared sampler with an explicit window
// predicate so tests can exercise the filtering path directly, independent of
// the reserve size that WindowedSample uses to size the window.
func (db *DB) ReserveSampleWithWindow(
	ctx context.Context,
	anchor []byte,
	committedDepth uint8,
	consensusTime uint64,
	minBatchBalance *big.Int,
	inWindow func([]byte) bool,
) (Sample, error) {
	return db.reserveSample(ctx, anchor, committedDepth, consensusTime, minBatchBalance, inWindow)
}

// LocalIngestPublished is the last usage figure handed to the local ingest
// gauge. A test uses it to check that the number an operator sees tracks the
// total on every path that moves it, not only on the ingest that happened to
// set it last. See issue #326.
func (db *DB) LocalIngestPublished() uint64 {
	db.localIngest.mu.Lock()
	defer db.localIngest.mu.Unlock()

	return db.localIngest.published
}

// TriggerQuit closes the shutdown signal without tearing the store down, so a
// test can assert the reserve scans stop on shutdown. It shares Close's guard,
// so a later Close does not double-close the channel. See issue #399.
func (db *DB) TriggerQuit() { db.quitOnce.Do(func() { close(db.quit) }) }

// EvictExpiredBatches and Unreserve expose the two reserve-worker operations
// that #407 gave a shutdown exit, so a test can run them directly rather than
// through the worker's triggers.
func (db *DB) EvictExpiredBatches(ctx context.Context) error { return db.evictExpiredBatches(ctx) }

func (db *DB) Unreserve(ctx context.Context) error { return db.unreserve(ctx) }

// NewForCloseTest builds the smallest DB whose Close is meaningful: the store's
// own closer, the shutdown budget, and the two drains Close waits on. A full
// store is not needed and would make the timings depend on real work.
//
// The DRAIN WINDOW is passed rather than taken from the default so a test can
// use a short one. It is the window #399 relies on and the one #428 must not
// shorten, so it is the number these tests turn on.
func NewForCloseTest(t *testing.T, dbCloser io.Closer, drainWindow time.Duration) *DB {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return &DB{
		logger:      log.Noop,
		quit:        make(chan struct{}),
		dbCloser:    dbCloser,
		drainWindow: drainWindow,
		cacheLimiter: cacheLimiter{
			sem:    make(chan struct{}, 1),
			ctx:    ctx,
			cancel: cancel,
		},
	}
}

// HoldInFlight adds one unit of background work and returns the release, so a
// test can make a drain outlast the shutdown budget. Calling the release is
// what lets the test's goroutine finish; Close does not wait for it.
func HoldInFlight(db *DB) func() {
	db.inFlight.Add(1)
	var once sync.Once
	return func() { once.Do(db.inFlight.Done) }
}

// HoldCacheWork adds one unit of cache work and returns the release, so a test
// can make the cache drain time out. See #428.
func HoldCacheWork(db *DB) func() {
	db.cacheLimiter.wg.Add(1)
	var once sync.Once
	return func() { once.Do(db.cacheLimiter.wg.Done) }
}

// CacheLimiterCancelled reports whether Close force-closed the cache limiter.
func CacheLimiterCancelled(db *DB) bool {
	select {
	case <-db.cacheLimiter.ctx.Done():
		return true
	default:
		return false
	}
}

// SetRadiusDecreaseHold replaces how long after the stale state the radius
// decrease stays blocked, for a test, and returns a function that restores it.
func SetRadiusDecreaseHold(d time.Duration) func() {
	old := radiusDecreaseHold
	radiusDecreaseHold = d
	return func() { radiusDecreaseHold = old }
}

// SetHeldLimits replaces the held-area bound and the validation pacing for
// a test, and returns a function that restores them.
func SetHeldLimits(maxAddrs uint64, check time.Duration, round int, pause time.Duration) func() {
	oldM, oldC, oldR, oldP := heldChunksMax, heldCheckInterval, heldRound, heldRoundPause
	heldChunksMax, heldCheckInterval, heldRound, heldRoundPause = maxAddrs, check, round, pause
	return func() { heldChunksMax, heldCheckInterval, heldRound, heldRoundPause = oldM, oldC, oldR, oldP }
}

// ValidateHeldPass runs one validation pass over the held index, as one tick
// of the validation worker does.
func (db *DB) ValidateHeldPass(ctx context.Context) error { return db.validateHeld(ctx) }

// HeldCount returns the number of distinct held addresses.
func (db *DB) HeldCount() uint64 { return db.held.count() }

// DeleteChunkForTest removes one chunk-store reference, as a crash or a
// recovery prune might leave a held entry without its chunk.
func (db *DB) DeleteChunkForTest(ctx context.Context, addr swarm.Address) error {
	return db.storage.Run(ctx, func(s transaction.Store) error { return s.ChunkStore().Delete(ctx, addr) })
}

// RadiusDecreaseDeferred returns the deferred radius decreases counted for
// reason (#696).
func (db *DB) RadiusDecreaseDeferred(reason string) float64 {
	var m dto.Metric
	if err := db.metrics.RadiusDecreaseDeferred.WithLabelValues(reason).Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

const (
	DeferNotStarted  = deferNotStarted
	DeferNotAtRadius = deferNotAtRadius
	DeferSettling    = deferSettling
)

// SetDrainWindow shortens the shutdown drain of a real DB for a test.
func SetDrainWindow(db *DB, d time.Duration) { db.drainWindow = d }
