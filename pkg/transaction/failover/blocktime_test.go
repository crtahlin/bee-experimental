// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package failover_test

import (
	"context"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/transaction"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	"github.com/ethersphere/bee/v2/pkg/transaction/failover"
)

// timedBackend is a backend that reports an observed block time.
type timedBackend struct {
	transaction.Backend
	observed time.Duration
}

func (b timedBackend) AverageBlockTime() time.Duration { return b.observed }

// TestAverageBlockTimeFromActiveEndpoint: the failover backend reports the
// block time its active endpoint observed, and 0 when that endpoint observes
// none, so transaction.ObservedBlockTime falls back to the configured value
// (#540).
func TestAverageBlockTimeFromActiveEndpoint(t *testing.T) {
	t.Parallel()

	answers := backendmock.WithBlockNumberFunc(func(context.Context) (uint64, error) { return 100, nil })

	timed := newBackend(t,
		failover.Endpoint{Name: "a", Backend: timedBackend{Backend: backendmock.New(answers), observed: 2 * time.Second}},
		endpoint("b", answers),
	)
	if got := timed.AverageBlockTime(); got != 2*time.Second {
		t.Fatalf("got %v, want the active endpoint's 2s", got)
	}

	untimed := newBackend(t, endpoint("a", answers), endpoint("b", answers))
	if got := untimed.AverageBlockTime(); got != 0 {
		t.Fatalf("got %v from an endpoint that observes nothing, want 0", got)
	}
	if got := transaction.ObservedBlockTime(untimed, 5*time.Second)(); got != 5*time.Second {
		t.Fatalf("got %v, want the 5s fallback", got)
	}
}
