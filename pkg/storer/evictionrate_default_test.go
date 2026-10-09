// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
)

// drainSim plays paced eviction against arrivals on the pacer's fake clock,
// with the same reservation the real wait uses: each round deletes up to
// one round of the excess, then waits for its tokens while chunks keep
// arriving. It returns the excess when the clock reaches until.
type drainSim struct {
	p       *evictionPacer
	clock   *fakeClock
	arrived *atomic.Uint64
	excess  float64
}

func (s *drainSim) run(t *testing.T, arrivalsPerSec float64, until time.Time) {
	t.Helper()
	for s.clock.now().Before(until) {
		n := min(reserve.EvictionRound, int(s.excess))
		step := 100 * time.Millisecond
		if n > 0 {
			s.excess -= float64(n)
			_, d := s.p.reserve(n)
			if d > 0 {
				step = d
			}
		}
		if rest := until.Sub(s.clock.now()); step > rest {
			step = rest
		}
		in := arrivalsPerSec * step.Seconds()
		s.clock.advance(step)
		s.arrived.Add(uint64(in))
		s.excess += in
	}
}

// TestDefaultRateKeepsUpWithArrivals checks the bounds in the spec (#651):
// at the default of 500 chunks per second, a burst of arrivals at 2,000 per
// second for 5 s grows the excess by less than about 3,750 chunks when the
// arrival samples are fresh, and by at most (2,000 - 500) x 5 s = 7,500 when
// they are stale; after arrivals stop the reserve reaches capacity within
// 25 s from a starting excess of 2,000.
func TestDefaultRateKeepsUpWithArrivals(t *testing.T) {
	t.Parallel()
	const (
		rateDefault = 500
		arrivals    = 2_000.0
		burst       = 5 * time.Second
		start       = 2_000.0
	)

	for _, tc := range []struct {
		name      string
		fresh     bool
		maxGrowth float64
	}{
		{name: "fresh samples", fresh: true, maxGrowth: 3_750},
		{name: "stale samples", fresh: false, maxGrowth: 7_500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var arrived atomic.Uint64
			p, clock := newTestPacer(rateDefault, arrived.Load)
			s := &drainSim{p: p, clock: clock, arrived: &arrived}

			if tc.fresh {
				// A minute of arrivals at the burst rate, all evicted as
				// they come, so the samples already show about 2,000/s.
				_ = p.effective(clock.now())
				s.run(t, arrivals, clock.now().Add(arrivalWindow))
				s.excess = 0
			} else {
				// One sample, then two idle minutes: stale when the burst
				// starts.
				_ = p.effective(clock.now())
				clock.advance(2 * arrivalWindow)
			}

			s.excess = start
			s.run(t, arrivals, clock.now().Add(burst))
			growth := s.excess - start
			if growth >= tc.maxGrowth {
				t.Fatalf("excess grew by %.0f over the burst, want under %.0f", growth, tc.maxGrowth)
			}

			s.run(t, 0, clock.now().Add(25*time.Second))
			if s.excess >= 1 {
				t.Fatalf("excess %.0f 25 s after arrivals stopped, want 0", s.excess)
			}
		})
	}
}

// TestDefaultRateCreatesPacer checks that the default rate gives a pacer and
// an explicit 0 gives none (#651; TestEvictionPacerOffAtZero covers 0 alone).
func TestDefaultRateCreatesPacer(t *testing.T) {
	t.Parallel()
	if p := newEvictionPacer(500, func() uint64 { return 0 }); p == nil {
		t.Fatal("no pacer at 500")
	}
	if p := newEvictionPacer(0, func() uint64 { return 0 }); p != nil {
		t.Fatal("a pacer at 0, want none (no limit)")
	}
}
