// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethersphere/bee/v2/pkg/postage"
	batchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// These tests cover the decision to lower the storage radius when the
// historical sync is done. See docs/experiments/radius-on-sync-done/spec.md
// and issue #588. They run in a synctest bubble, so the hold times, the
// scan rate limit and the ticker pass in fake time.

const day = 24 * time.Hour

type radiusEnv struct {
	db     *storer.DB
	syncer *pullerMock.Syncer
	bs     postage.Storer
	base   swarm.Address
	batch  *postage.Batch
}

type radiusOpts struct {
	capacity int
	radius   uint8
	wakeup   time.Duration
	rate     float64
	done     bool
	// timings, when set, replaces the hold times and intervals:
	// hold after decrease, hold after raise, scan interval, sampling poll.
	timings []time.Duration
}

// newRadiusEnv builds an in-memory store inside the current synctest
// bubble, starts its reserve worker and waits until the radius is set.
func newRadiusEnv(t *testing.T, o radiusOpts) *radiusEnv {
	t.Helper()

	base := swarm.RandAddress(t)
	bs := batchstore.New()
	db, err := storer.New(context.Background(), "", dbTestOps(base, o.capacity, bs, nil, o.wakeup))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
		// The in-memory index store leaves a goroutine that ends on its
		// next tick; let it end inside the bubble.
		time.Sleep(time.Minute)
	})
	if o.timings != nil {
		db.SetRadiusTimings(o.timings[0], o.timings[1], o.timings[2], o.timings[3])
	}
	syncer := pullerMock.NewMockSyncer(o.rate, o.done)
	ready := make(chan struct{})
	db.StartReserveWorker(context.Background(), syncer, networkRadiusFunc(o.radius), ready)
	<-ready
	synctest.Wait()
	if got := db.StorageRadius(); got != o.radius {
		t.Fatalf("start radius %d, want %d", got, o.radius)
	}
	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	return &radiusEnv{db: db, syncer: syncer, bs: bs, base: base, batch: batch}
}

// put stores n chunks at proximity order po under the store's saved batch.
func (e *radiusEnv) put(t *testing.T, po, n int) {
	t.Helper()
	e.putBatch(t, e.batch, po, n)
}

// putNewBatch stores n chunks at proximity order po under a new batch and
// returns it. The batchstore mock holds one batch, so the new batch is
// unknown to it: use this only where no combined scan, which evicts
// chunks of unknown batches, runs.
func (e *radiusEnv) putNewBatch(t *testing.T, po, n int) *postage.Batch {
	t.Helper()
	batch := postagetesting.MustNewBatch()
	e.putBatch(t, batch, po, n)
	return batch
}

func (e *radiusEnv) putBatch(t *testing.T, batch *postage.Batch, po, n int) {
	t.Helper()
	putter := e.db.ReservePutter()
	for range n {
		ch := chunk.GenerateTestRandomChunkAt(t, e.base, po).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		if err := putter.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}
	synctest.Wait()
}

func (e *radiusEnv) signal() {
	e.syncer.Signal()
	synctest.Wait()
}

func wantRadius(t *testing.T, db *storer.DB, want uint8, why string) {
	t.Helper()
	synctest.Wait()
	if got := db.StorageRadius(); got != want {
		t.Fatalf("radius %d, want %d: %s", got, want, why)
	}
}

func wantChecks(t *testing.T, db *storer.DB, trigger, result string, want float64) {
	t.Helper()
	if got := db.RadiusChecks(trigger, result); got != want {
		t.Fatalf("radius_check_total{trigger=%q,result=%q} = %v, want %v", trigger, result, got, want)
	}
}

func wantScans(t *testing.T, db *storer.DB, kind string, want uint64, why string) {
	t.Helper()
	if got := db.ReserveScans(kind); got != want {
		t.Fatalf("%d %s scans, want %d: %s", got, kind, want, why)
	}
}

// Reserve 1: historical chunks still arriving, a sync rate above 0, do not
// stop lowering once the historical sync is done. Today's rule needs a
// rate of 0.
func TestRadiusLowersWhileRateAboveZero(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 3, wakeup: 15 * time.Minute, rate: 5, done: true})

		time.Sleep(3 * time.Minute) // past the hold from start
		e.signal()
		wantRadius(t, e.db, 2, "the sync is done; a rate above 0 must not stop the decrease")

		time.Sleep(15 * time.Minute)
		wantRadius(t, e.db, 1, "the ticker lowers it too while the rate is above 0")
		wantChecks(t, e.db, "ticker", "lowered", 1)
	})
}

// Reserve 2: with no peers the puller reports not done, and the radius is
// not lowered, although the rate is 0. Today's rule would lower it at the
// first tick. The fallback would lower it at the fourth; this test stops
// before that.
func TestRadiusNotLoweredWithoutPeers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 3, wakeup: 15 * time.Minute, rate: 0, done: false})

		time.Sleep(time.Duration(storer.FallbackChecks-1)*15*time.Minute + time.Minute)
		e.signal()
		wantRadius(t, e.db, 3, "no peers means the sync is not done")
		wantChecks(t, e.db, "ticker", "sync_pending", float64(storer.FallbackChecks-1))
		wantChecks(t, e.db, "sync_done", "sync_pending", 1)
	})
}

// Reserve 3: a sync_done signal lowers the radius without waiting for the
// ticker.
func TestRadiusLoweredOnSyncSignal(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 3, wakeup: day, done: true})

		time.Sleep(3 * time.Minute)
		wantRadius(t, e.db, 3, "no check has run yet")
		e.signal()
		wantRadius(t, e.db, 2, "the signal runs the check at once")
		wantChecks(t, e.db, "sync_done", "lowered", 1)
		wantScans(t, e.db, "count", 0, "an empty reserve is below the threshold without a scan")

		// A second signal right after the decrease is held for 2 minutes,
		// and the timer runs it then.
		e.signal()
		wantRadius(t, e.db, 2, "held for 2 minutes after a decrease")
		wantChecks(t, e.db, "sync_done", "held", 1)
		time.Sleep(2*time.Minute + time.Second)
		wantRadius(t, e.db, 1, "the timer ran the check when the hold ended")
	})
}

// Reserve 4: a second signal within 2 minutes does not run a second scan;
// the timer runs the check when the 2 minutes have passed.
func TestRadiusScanRateLimit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// No hold after a decrease, so only the rate limit refuses.
		e := newRadiusEnv(t, radiusOpts{capacity: 100, radius: 3, wakeup: day, done: true,
			timings: []time.Duration{0, 15 * time.Minute, 2 * time.Minute, 30 * time.Second}})
		// 60 chunks below the radius: the reserve is above the threshold
		// of 50, the count within radius is 0, so only a scan can tell.
		e.put(t, 0, 60)

		e.signal()
		wantRadius(t, e.db, 2, "the first signal scans and lowers")
		wantScans(t, e.db, "count", 1, "the first signal")

		e.signal()
		wantRadius(t, e.db, 2, "the second signal is within the rate limit")
		wantScans(t, e.db, "count", 1, "the second signal must not scan within 2 minutes")
		wantChecks(t, e.db, "sync_done", "held", 1)

		time.Sleep(2*time.Minute + time.Second)
		wantScans(t, e.db, "count", 2, "the timer runs the refused check")
		wantRadius(t, e.db, 1, "the retried check lowers")
	})
}

// Reserve 5: a signal during a hold or a sample is retried by the timer,
// not dropped.
func TestRadiusSignalRetried(t *testing.T) {
	t.Parallel()
	t.Run("hold", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 3, wakeup: day, done: true})
			e.signal() // within the 2 minute hold from start
			wantRadius(t, e.db, 3, "held from start")
			wantChecks(t, e.db, "sync_done", "held", 1)
			time.Sleep(time.Minute)
			wantRadius(t, e.db, 3, "still held")
			time.Sleep(time.Minute + time.Second)
			wantRadius(t, e.db, 2, "the timer ran the check when the hold ended")
		})
	})
	t.Run("sampling", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 3, wakeup: day, done: true,
				timings: []time.Duration{0, 15 * time.Minute, 2 * time.Minute, 30 * time.Second}})
			e.db.SetSampling(true)
			e.signal()
			time.Sleep(5 * time.Minute)
			wantRadius(t, e.db, 3, "a sample runs, the check is postponed")
			if got := e.db.RadiusChecks("sync_done", "postponed_sampling"); got < 5 {
				t.Fatalf("%v postponed checks in 5 minutes, want one every 30 seconds", got)
			}
			e.db.SetSampling(false)
			time.Sleep(31 * time.Second)
			wantRadius(t, e.db, 2, "the sample ended and the timer ran the check")
		})
	})
}

// Reserve 6: a signal does not scan when the last count, at the same
// radius, was above the threshold.
func TestRadiusSignalSkipsScan(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newRadiusEnv(t, radiusOpts{capacity: 100, radius: 3, wakeup: day, done: true,
			timings: []time.Duration{0, 15 * time.Minute, 2 * time.Minute, 30 * time.Second}})
		e.put(t, 5, 60) // within radius, above the threshold of 50

		e.signal() // the stored count is from before the radius was set
		wantScans(t, e.db, "count", 1, "no count at this radius yet")
		wantChecks(t, e.db, "sync_done", "above_threshold", 1)

		time.Sleep(3 * time.Minute) // past the rate limit
		e.signal()
		wantScans(t, e.db, "count", 1, "the count at this radius was above the threshold")
		wantChecks(t, e.db, "sync_done", "skipped_scan", 1)
		wantRadius(t, e.db, 3, "above the threshold")
	})
}

// Reserve 7: the hold after a raise by unreserve prevents an immediate
// decrease.
func TestRadiusHoldAfterRaise(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 0, wakeup: day, done: true})
		time.Sleep(3 * time.Minute) // past the hold from start

		e.put(t, 0, 11) // over capacity: unreserve raises the radius to 1
		wantRadius(t, e.db, 1, "unreserve raised the radius")

		e.signal()
		wantRadius(t, e.db, 1, "held after the raise")
		wantChecks(t, e.db, "sync_done", "held", 1)
		time.Sleep(14 * time.Minute)
		wantRadius(t, e.db, 1, "still held 14 minutes after the raise")
		time.Sleep(2 * time.Minute)
		wantRadius(t, e.db, 0, "the timer ran the check when the 15 minute hold ended")
	})
}

// Reserve 8: an expiry scans even when the last count was above the
// threshold, because the reserve just got smaller.
func TestRadiusExpiryScans(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newRadiusEnv(t, radiusOpts{capacity: 100, radius: 3, wakeup: day, done: true,
			timings: []time.Duration{0, 15 * time.Minute, 2 * time.Minute, 30 * time.Second}})
		e.put(t, 0, 30)
		e.put(t, 5, 30)
		expiring := e.putNewBatch(t, 5, 30)

		e.signal()
		wantChecks(t, e.db, "sync_done", "above_threshold", 1) // 60 within radius
		time.Sleep(3 * time.Minute)
		e.signal()
		wantChecks(t, e.db, "sync_done", "skipped_scan", 1)

		// After the expiry the reserve holds 60 chunks, above the
		// threshold, and 30 within radius, below it.
		if err := e.db.EvictBatch(context.Background(), expiring.ID); err != nil {
			t.Fatal(err)
		}
		wantRadius(t, e.db, 2, "the expiry scanned and found the count below the threshold")
		wantChecks(t, e.db, "expiry", "lowered", 1)
		wantScans(t, e.db, "count", 2, "one scan for the first signal, one for the expiry")
	})
}

// Reserve 9: the fallback lowers the radius after 4 ticker checks in a row
// in which today's rule held and the new rule refused, as with a worker
// blocked on a peer. Its count resets on a radius change and when today's
// rule does not hold.
func TestRadiusFallback(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const tick = 15 * time.Minute
		e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 3, wakeup: tick, rate: 0, done: false})
		time.Sleep(time.Second) // keep the checks between ticks

		time.Sleep(3 * tick)
		wantRadius(t, e.db, 3, "three refused checks")
		time.Sleep(tick)
		wantRadius(t, e.db, 2, "the fourth refused check lowers by the fallback")
		wantChecks(t, e.db, "ticker", "fallback", 1)

		// The decrease reset the count: three more checks do not lower.
		time.Sleep(3 * tick)
		wantRadius(t, e.db, 2, "the count restarted after the radius changed")

		// One check where today's rule does not hold resets the count.
		e.syncer.SetSyncRate(1)
		time.Sleep(tick)
		e.syncer.SetSyncRate(0)
		time.Sleep(3 * tick)
		wantRadius(t, e.db, 2, "the count restarted after today's rule did not hold")
		time.Sleep(tick)
		wantRadius(t, e.db, 1, "four refused checks in a row again")
		wantChecks(t, e.db, "ticker", "fallback", 2)
	})
}

// Reserve 9, continued: the fallback is postponed while a sample runs.
func TestRadiusFallbackPostponedBySampling(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const tick = 15 * time.Minute
		e := newRadiusEnv(t, radiusOpts{capacity: 10, radius: 3, wakeup: tick, rate: 0, done: false})
		time.Sleep(time.Second)

		time.Sleep(3 * tick)
		e.db.SetSampling(true)
		time.Sleep(tick)
		wantRadius(t, e.db, 3, "the fourth check falls in a sample")
		wantChecks(t, e.db, "ticker", "postponed_sampling", 1)
		e.db.SetSampling(false)
		time.Sleep(tick)
		wantRadius(t, e.db, 2, "the first check after the sample lowers by the fallback")
	})
}

// Reserve 10: the ticker scans even when a cheap condition fails, so the
// count behind /status stays current.
func TestRadiusTickerAlwaysScans(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const tick = 15 * time.Minute
		e := newRadiusEnv(t, radiusOpts{capacity: 100, radius: 3, wakeup: tick, rate: 1, done: false})
		before := e.db.ReserveScans("combined")

		e.put(t, 5, 20)
		time.Sleep(tick + time.Second)
		wantScans(t, e.db, "combined", before+1, "the ticker scans although the sync is not done")
		if got := e.db.ReserveSizeWithinRadius(); got != 20 {
			t.Fatalf("ReserveSizeWithinRadius = %d, want 20", got)
		}

		e.db.SetSampling(true)
		e.put(t, 5, 5)
		time.Sleep(tick)
		wantScans(t, e.db, "combined", before+2, "the ticker scans during a sample")
		if got := e.db.ReserveSizeWithinRadius(); got != 25 {
			t.Fatalf("ReserveSizeWithinRadius = %d, want 25", got)
		}
		e.db.SetSampling(false)
	})
}
