// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node

import (
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/transaction"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
)

type timedBackend struct {
	transaction.Backend
	observed time.Duration
}

func (b timedBackend) AverageBlockTime() time.Duration { return b.observed }

// TestBlockTimeFuncPrecedence: an explicitly set block-time is used as is;
// otherwise the observed block time is used once there is one (#540).
func TestBlockTimeFuncPrecedence(t *testing.T) {
	t.Parallel()

	backend := timedBackend{Backend: backendmock.New(), observed: 2 * time.Second}

	if got := blockTimeFunc(backend, 5*time.Second, true)(); got != 5*time.Second {
		t.Fatalf("explicit setting: got %v, want 5s", got)
	}
	if got := blockTimeFunc(backend, 5*time.Second, false)(); got != 2*time.Second {
		t.Fatalf("default: got %v, want the observed 2s", got)
	}
	unmeasured := timedBackend{Backend: backendmock.New()}
	if got := blockTimeFunc(unmeasured, 5*time.Second, false)(); got != 5*time.Second {
		t.Fatalf("nothing observed yet: got %v, want the 5s default", got)
	}
}

// TestBlockTimeDrift: the warning comes once, only after the observed block
// time has differed from the configured one by more than the tolerance for
// the whole period; a brief difference or nothing observed does not count.
func TestBlockTimeDrift(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_000_000, 0)
	at := func(minutes int) time.Time { return start.Add(time.Duration(minutes) * time.Minute) }

	t.Run("lasting difference warns once", func(t *testing.T) {
		t.Parallel()
		d := &blockTimeDrift{configured: 5 * time.Second}
		for m := 0; m < 10; m++ {
			if d.check(at(m), 2*time.Second) {
				t.Fatalf("warned after %d minutes, before the period", m)
			}
		}
		if !d.check(at(10), 2*time.Second) {
			t.Fatal("no warning after the full period")
		}
		if d.check(at(11), 2*time.Second) {
			t.Fatal("warned a second time")
		}
	})

	t.Run("brief difference does not warn", func(t *testing.T) {
		t.Parallel()
		d := &blockTimeDrift{configured: 5 * time.Second}
		for m := 0; m < 9; m++ {
			d.check(at(m), 2*time.Second)
		}
		d.check(at(9), 5*time.Second) // back within tolerance: the clock restarts
		for m := 10; m < 19; m++ {
			if d.check(at(m), 2*time.Second) {
				t.Fatalf("warned at minute %d, less than a full period after the difference returned", m)
			}
		}
	})

	t.Run("within tolerance or unmeasured never warns", func(t *testing.T) {
		t.Parallel()
		d := &blockTimeDrift{configured: 5 * time.Second}
		for m := 0; m < 30; m++ {
			if d.check(at(m), 5500*time.Millisecond) || d.check(at(m), 0) {
				t.Fatal("warned for a 10% difference or for no measurement")
			}
		}
	})
}
