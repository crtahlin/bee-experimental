// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package reserve_test

import (
	"sync"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/storer/internal"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kademlia "github.com/ethersphere/bee/v2/pkg/topology/mock"
)

func newRadiusReserve(t *testing.T) *reserve.Reserve {
	t.Helper()
	r, err := reserve.New(swarm.RandAddress(t), internal.NewInmemStorage(), 0, kademlia.NewTopologyDriver(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestRadiusStateCountsIncreases checks that SetRadius counts only a higher
// radius and keeps radius and count in step (#658).
func TestRadiusStateCountsIncreases(t *testing.T) {
	t.Parallel()

	r := newRadiusReserve(t)
	for _, step := range []struct {
		radius    uint8
		wantCount uint64
	}{
		{5, 1}, // from 0
		{5, 1}, // same
		{4, 1}, // lower
		{6, 2}, // higher
		{3, 2}, // lower
		{4, 3}, // higher again
	} {
		if err := r.SetRadius(step.radius); err != nil {
			t.Fatal(err)
		}
		radius, count := r.RadiusState()
		if radius != step.radius || count != step.wantCount {
			t.Fatalf("after SetRadius(%d): radius %d count %d, want %d and %d", step.radius, radius, count, step.radius, step.wantCount)
		}
		if got := r.Radius(); got != step.radius {
			t.Fatalf("Radius() %d, want %d", got, step.radius)
		}
	}
}

// TestRadiusStateConsistent checks under -race that a reader never sees a
// radius and an increase count from different writes: the writer only
// raises the radius by one per call, so radius and count stay equal.
func TestRadiusStateConsistent(t *testing.T) {
	t.Parallel()

	r := newRadiusReserve(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for rad := uint8(1); rad < 200; rad++ {
			if err := r.SetRadius(rad); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for range 20_000 {
		radius, count := r.RadiusState()
		if uint64(radius) != count {
			t.Fatalf("radius %d with count %d: from different writes", radius, count)
		}
	}
	wg.Wait()
}
