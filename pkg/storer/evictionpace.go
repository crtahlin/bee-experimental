// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"golang.org/x/time/rate"
)

// Paced reserve eviction (wasp #623, docs/experiments/paced-eviction/spec.md).
//
// Eviction always works in rounds of at most reserve.EvictionRound chunks,
// which bounds its memory. With reserve-eviction-rate set, each round also
// pays for the chunks it deleted with tokens of a rate limiter, after the
// batch lock is released, and deletes on few goroutines. The rate is raised
// to the rate at which chunks arrive, with more goroutines if needed, so
// the excess over capacity does not grow while the eviction is paced.

const (
	// evictionWorkers is how many goroutines a paced round deletes with
	// before the arrival rate asks for more. Compiled in, not a setting.
	evictionWorkers = 2
	// arrivalWindow is how far back the arrival rate looks.
	arrivalWindow = time.Minute
	// workersStepUpAfter is how many consecutive rounds must miss the
	// effective rate before another goroutine is added.
	workersStepUpAfter = 3
	// workersStepDownAfter is how long the deletion rate must stay at least
	// workersHeadroom above the effective rate before a goroutine is removed.
	workersStepDownAfter = time.Minute
	workersHeadroom      = 1.25
)

// errEvictionExpiry ends a paced unreserve when a batch expires during a
// wait, as the expiry check between batches does.
var errEvictionExpiry = errors.New("eviction stopped by a batch expiry")

type arrivalSample struct {
	at time.Time
	n  uint64
}

// evictionPacer paces reserve eviction. Eviction runs only on the reserve
// worker goroutine, so the pacer is used from one goroutine at a time; the
// mutex only keeps the metrics and tests safe.
type evictionPacer struct {
	mu sync.Mutex

	limit      rate.Limit // configured chunks per second
	lim        *rate.Limiter
	arrivals   func() uint64
	now        func() time.Time
	maxWorkers int

	workers       int
	perChunk      time.Duration // smoothed deletion time per chunk
	missed        int           // consecutive rounds below the effective rate
	headroomSince time.Time     // start of the current stretch of headroom
	samples       []arrivalSample

	setWorkers     func(int)
	setArrivalRate func(float64)
}

// newEvictionPacer returns nil for a rate of 0 or below: eviction is then
// not paced, which is the previous behaviour apart from the rounds.
func newEvictionPacer(perSecond int, arrivals func() uint64) *evictionPacer {
	if perSecond <= 0 {
		return nil
	}
	return &evictionPacer{
		limit:          rate.Limit(perSecond),
		lim:            rate.NewLimiter(rate.Limit(perSecond), reserve.EvictionRound),
		arrivals:       arrivals,
		now:            time.Now,
		maxWorkers:     max(evictionWorkers, runtime.NumCPU()),
		workers:        evictionWorkers,
		setWorkers:     func(int) {},
		setArrivalRate: func(float64) {},
	}
}

// Workers returns how many goroutines the next round deletes with.
func (p *evictionPacer) Workers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workers
}

// arrivalRate records the current arrival count and returns the chunks per
// second added since the baseline sample, the newest one at least
// arrivalWindow old, or the oldest one when none is that old. It reads 0
// until the samples span at least a second.
func (p *evictionPacer) arrivalRate(now time.Time) float64 {
	n := p.arrivals()
	p.samples = append(p.samples, arrivalSample{at: now, n: n})
	// Keep the newest sample at or beyond the window edge as the baseline,
	// so a wait longer than the window does not leave nothing to compare
	// with: rounds are sampled once each, and at a low rate a round's wait
	// can exceed the window.
	cut := 0
	for cut < len(p.samples)-1 && now.Sub(p.samples[cut+1].at) >= arrivalWindow {
		cut++
	}
	p.samples = p.samples[cut:]
	oldest := p.samples[0]
	span := now.Sub(oldest.at)
	if span < time.Second || n < oldest.n {
		return 0
	}
	r := float64(n-oldest.n) / span.Seconds()
	p.setArrivalRate(r)
	return r
}

// effective returns max(configured rate, arrival rate).
func (p *evictionPacer) effective(now time.Time) rate.Limit {
	if a := rate.Limit(p.arrivalRate(now)); a > p.limit {
		return a
	}
	return p.limit
}

// observe records a round that deleted n chunks in took, and adjusts the
// number of goroutines: one more after workersStepUpAfter consecutive
// rounds below the effective rate, one fewer after workersStepDownAfter of
// at least workersHeadroom above it, never below evictionWorkers.
func (p *evictionPacer) observe(n int, took time.Duration) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	per := took / time.Duration(n)
	if per <= 0 {
		per = time.Nanosecond
	}
	if p.perChunk == 0 {
		p.perChunk = per
	} else {
		// Smoothed over about the last four rounds.
		p.perChunk = (3*p.perChunk + per) / 4
	}
	achieved := float64(time.Second) / float64(p.perChunk)
	now := p.now()
	eff := float64(p.effective(now))

	switch {
	case achieved < eff:
		p.headroomSince = time.Time{}
		p.missed++
		if p.missed >= workersStepUpAfter && p.workers < p.maxWorkers {
			p.workers++
			p.missed = 0
		}
	case achieved >= workersHeadroom*eff:
		p.missed = 0
		if p.headroomSince.IsZero() {
			p.headroomSince = now
		} else if now.Sub(p.headroomSince) >= workersStepDownAfter && p.workers > evictionWorkers {
			p.workers--
			p.headroomSince = now
		}
	default:
		p.missed = 0
		p.headroomSince = time.Time{}
	}
	p.setWorkers(p.workers)
}

// wait pays for n deleted chunks: it waits until the limiter, set to the
// effective rate, allows them. It is called with the batch lock released.
// quit, expiry and the context end the wait and cancel the reservation; a
// nil channel never fires.
func (p *evictionPacer) wait(ctx context.Context, n int, quit, expiry <-chan struct{}) error {
	p.mu.Lock()
	now := p.now()
	p.lim.SetLimitAt(now, p.effective(now))
	res := p.lim.ReserveN(now, n)
	p.mu.Unlock()
	if !res.OK() {
		// n is at most one round, the burst, so this does not happen.
		return nil
	}
	d := res.DelayFrom(now)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-quit:
		res.Cancel()
		return ErrDBQuit
	case <-expiry:
		res.Cancel()
		return errEvictionExpiry
	case <-ctx.Done():
		res.Cancel()
		return ctx.Err()
	}
}

// evictionHooks returns the per-round hooks for one eviction. expired tells
// whether it evicts expired batches, which selects the counter the evicted
// chunks are added to. expiry ends a paced wait when a batch expires; nil
// for an eviction that expiries do not interrupt.
func (db *DB) evictionHooks(ctx context.Context, expired bool, expiry <-chan struct{}) reserve.EvictionHooks {
	counter := db.metrics.EvictedChunkCount
	if expired {
		counter = db.metrics.ExpiredChunkCount
	}
	h := reserve.EvictionHooks{
		After: func(n int, took time.Duration) {
			counter.Add(float64(n))
			db.metrics.ReserveSize.Set(float64(db.reserve.Size()))
			if db.evictionPacer != nil {
				db.evictionPacer.observe(n, took)
			}
		},
	}
	// Between rounds, at every rate, a shutdown or a batch expiry stops the
	// eviction, so a long eviction of one batch is not the only work that
	// cannot be stopped. A nil expiry channel never fires.
	stop := func() error {
		// A shutdown first, on its own: select picks at random among ready
		// cases, and a shutdown that also ends the context must stop the
		// eviction as a shutdown, not as a context error the worker logs.
		select {
		case <-db.quit:
			return ErrDBQuit
		default:
		}
		select {
		case <-expiry:
			return errEvictionExpiry
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	h.Pay = func(int) error { return stop() }
	if p := db.evictionPacer; p != nil {
		h.Workers = p.Workers
		h.Pay = func(n int) error {
			if err := stop(); err != nil {
				return err
			}
			return p.wait(ctx, n, db.quit, expiry)
		}
	}
	return h
}

// evictionRateAndWorkers returns the configured rate and the current number
// of goroutines, for the log line at the start of an eviction. 0 workers
// means runtime.NumCPU(), the previous behaviour.
func (db *DB) evictionRateAndWorkers() (int, int) {
	p := db.evictionPacer
	if p == nil {
		return 0, runtime.NumCPU()
	}
	return int(p.limit), p.Workers()
}
