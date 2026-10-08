// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
)

func TestEvictionPacerOffAtZero(t *testing.T) {
	t.Parallel()
	if p := newEvictionPacer(0, func() uint64 { return 0 }); p != nil {
		t.Fatal("a rate of 0 must leave eviction unpaced")
	}
	if p := newEvictionPacer(-1, func() uint64 { return 0 }); p != nil {
		t.Fatal("a negative rate must leave eviction unpaced")
	}
}

// fakeClock lets the pacer's own time be moved by the test. The limiter's
// waits still use real timers, so the tests keep their waits short.
type fakeClock struct{ t atomic.Int64 }

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.t.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.t.Add(int64(d)) }

func newTestPacer(perSecond int, arrivals func() uint64) (*evictionPacer, *fakeClock) {
	p := newEvictionPacer(perSecond, arrivals)
	c := &fakeClock{}
	c.t.Store(time.Unix(1_000_000, 0).UnixNano())
	p.now = c.now
	return p, c
}

func TestEvictionPacerWaitsForTokens(t *testing.T) {
	t.Parallel()
	p := newEvictionPacer(100, func() uint64 { return 0 })

	// The burst is one round, so the first round is free.
	start := time.Now()
	if err := p.wait(context.Background(), reserve.EvictionRound, nil, nil); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("first round waited %v, want no wait", d)
	}
	// The next 20 chunks at 100 per second cost about 200 ms.
	start = time.Now()
	if err := p.wait(context.Background(), 20, nil, nil); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Fatalf("waited %v, want about 200 ms", d)
	}
}

func TestEvictionPacerWaitEndsEarly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		fire func(quit, expiry chan struct{}, cancel context.CancelFunc)
		want error
	}{
		{"quit", func(q, _ chan struct{}, _ context.CancelFunc) { close(q) }, ErrDBQuit},
		{"expiry", func(_, e chan struct{}, _ context.CancelFunc) { close(e) }, errEvictionExpiry},
		{"context", func(_, _ chan struct{}, c context.CancelFunc) { c() }, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := newEvictionPacer(1, func() uint64 { return 0 })
			// Spend the burst, so the next wait is long.
			if err := p.wait(context.Background(), reserve.EvictionRound, nil, nil); err != nil {
				t.Fatal(err)
			}
			quit, expiry := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				time.Sleep(50 * time.Millisecond)
				tc.fire(quit, expiry, cancel)
			}()
			done := make(chan error, 1)
			go func() { done <- p.wait(ctx, 100, quit, expiry) }()
			select {
			case err := <-done:
				if !errors.Is(err, tc.want) {
					t.Fatalf("got %v, want %v", err, tc.want)
				}
			case <-time.After(5 * time.Second):
				// The wait would last 100 s at 1 per second.
				t.Fatal("the wait did not end on the signal")
			}
		})
	}
}

func TestEvictionPacerArrivalFloor(t *testing.T) {
	t.Parallel()
	var arrived atomic.Uint64
	p, clock := newTestPacer(100, arrived.Load)

	if got := p.effective(clock.now()); got != 100 {
		t.Fatalf("effective %v with no arrivals, want the configured 100", got)
	}
	// 30,000 chunks in 10 s is 3,000 per second, above the configured rate.
	clock.advance(10 * time.Second)
	arrived.Store(30_000)
	if got := float64(p.effective(clock.now())); got < 2_900 || got > 3_100 {
		t.Fatalf("effective %v, want about 3,000 (the arrival rate)", got)
	}
	// After the window, old arrivals no longer count.
	clock.advance(2 * arrivalWindow)
	if got := p.effective(clock.now()); got != 100 {
		t.Fatalf("effective %v after a quiet window, want 100", got)
	}
}

func TestEvictionPacerWorkerScaling(t *testing.T) {
	t.Parallel()
	p, clock := newTestPacer(1_000, func() uint64 { return 0 })
	if p.Workers() != evictionWorkers {
		t.Fatalf("workers %d, want %d to start", p.Workers(), evictionWorkers)
	}
	if p.maxWorkers < runtime.NumCPU() {
		t.Fatalf("max workers %d, want at least the CPU count", p.maxWorkers)
	}

	// 1,000 chunks in 2 s is 500 per second, below the rate of 1,000.
	// Two missed rounds are not enough; the third adds a worker.
	p.observe(1_000, 2*time.Second)
	p.observe(1_000, 2*time.Second)
	if p.Workers() != evictionWorkers {
		t.Fatalf("workers %d after two missed rounds, want %d", p.Workers(), evictionWorkers)
	}
	p.observe(1_000, 2*time.Second)
	if p.maxWorkers > evictionWorkers && p.Workers() != evictionWorkers+1 {
		t.Fatalf("workers %d after three missed rounds, want %d", p.Workers(), evictionWorkers+1)
	}
	up := p.Workers()

	// Fast rounds: the smoothed rate needs a few rounds to catch up, and
	// the headroom must then last a minute before a worker is removed.
	// 20 rounds 5 s apart give one step down, not two.
	for range 6 {
		p.observe(1_000, 100*time.Millisecond)
		clock.advance(5 * time.Second)
	}
	if p.Workers() != up {
		t.Fatalf("workers %d before a minute of headroom, want %d", p.Workers(), up)
	}
	for range 14 {
		p.observe(1_000, 100*time.Millisecond)
		clock.advance(5 * time.Second)
	}
	if up > evictionWorkers && p.Workers() != up-1 {
		t.Fatalf("workers %d after a minute of headroom, want %d", p.Workers(), up-1)
	}
	// Never below evictionWorkers.
	for range 100 {
		p.observe(1_000, 100*time.Millisecond)
		clock.advance(time.Minute)
	}
	if p.Workers() != evictionWorkers {
		t.Fatalf("workers %d, want the floor %d", p.Workers(), evictionWorkers)
	}
}
