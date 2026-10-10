// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"testing"
	"time"

	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

func startActed(t *testing.T, settle time.Duration, syncer storer.Syncer, start uint8, health *healthMock) *storer.DB {
	t.Helper()
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 10, nil, nil, 50*time.Millisecond)
	opts.RadiusDecreaseSettle = settle
	st, err := memStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}
	if health != nil {
		st.SetPostageSyncHealth(health)
	}
	readyC := make(chan struct{})
	st.StartReserveWorker(context.Background(), syncer, networkRadiusFunc(start), readyC)
	<-readyC
	return st
}

func TestRadiusDropsAfterSettle(t *testing.T) {
	t.Parallel()
	s := &actedSyncer{}
	s.set(true, 4, time.Now().Add(-time.Hour))
	st := startActed(t, time.Minute, s, 4, nil)
	if err := spinlock.Wait(5*time.Second, func() bool { return st.Reserve().Radius() == 3 }); err != nil {
		t.Fatalf("radius %d, want one step down to 3 after the settle window", st.Reserve().Radius())
	}
	// The puller has not acted on 3: no further step.
	time.Sleep(500 * time.Millisecond)
	if r := st.Reserve().Radius(); r != 3 {
		t.Fatalf("radius %d, want it held at 3 until the puller acts on 3", r)
	}
	if st.RadiusDecreaseDeferred(storer.DeferNotAtRadius) == 0 {
		t.Fatal("deferral at the stale acted radius not counted")
	}
}

func TestRadiusDeferredWhileSettling(t *testing.T) {
	t.Parallel()
	s := &actedSyncer{}
	s.set(true, 4, time.Now())
	st := startActed(t, time.Hour, s, 4, nil)
	if err := spinlock.Wait(5*time.Second, func() bool { return st.RadiusDecreaseDeferred(storer.DeferSettling) >= 2 }); err != nil {
		t.Fatal("settling deferral not counted")
	}
	if r := st.Reserve().Radius(); r != 4 {
		t.Fatalf("radius %d while the puller settles, want 4", r)
	}
}

// TestRadiusDeferredWithinSettle pins the settle boundary: an age between
// half and the full window still defers.
func TestRadiusDeferredWithinSettle(t *testing.T) {
	t.Parallel()
	s := &actedSyncer{}
	s.set(true, 4, time.Now().Add(-40*time.Minute))
	st := startActed(t, time.Hour, s, 4, nil)
	if err := spinlock.Wait(5*time.Second, func() bool { return st.RadiusDecreaseDeferred(storer.DeferSettling) >= 2 }); err != nil {
		t.Fatal("deferral at 40 of 60 minutes not counted")
	}
	if r := st.Reserve().Radius(); r != 4 {
		t.Fatalf("radius %d at 40 of 60 minutes settled, want 4", r)
	}
}

func TestRadiusDeferredNotAtRadius(t *testing.T) {
	t.Parallel()
	s := &actedSyncer{}
	s.set(true, 5, time.Now().Add(-time.Hour))
	st := startActed(t, 0, s, 4, nil)
	if err := spinlock.Wait(5*time.Second, func() bool { return st.RadiusDecreaseDeferred(storer.DeferNotAtRadius) >= 2 }); err != nil {
		t.Fatal("not-at-radius deferral not counted")
	}
	if r := st.Reserve().Radius(); r != 4 {
		t.Fatalf("radius %d with the puller still at 5, want 4", r)
	}
}

func TestRadiusDeferredNotStarted(t *testing.T) {
	t.Parallel()
	s := &actedSyncer{}
	st := startActed(t, 0, s, 4, nil)
	if err := spinlock.Wait(5*time.Second, func() bool { return st.RadiusDecreaseDeferred(storer.DeferNotStarted) >= 2 }); err != nil {
		t.Fatal("not-started deferral not counted")
	}
	if r := st.Reserve().Radius(); r != 4 {
		t.Fatalf("radius %d before the puller acted on any radius, want 4", r)
	}
}

func TestRadiusNoReporterKeepsOldRule(t *testing.T) {
	t.Parallel()
	st := startActed(t, time.Hour, pullerMock.NewMockRateReporter(0), 4, nil)
	if err := spinlock.Wait(5*time.Second, func() bool { return st.Reserve().Radius() <= 2 }); err != nil {
		t.Fatalf("radius %d with a syncer that does not report, want it lowered at each check as before", st.Reserve().Radius())
	}
}

func TestRadiusStallGateBeforeDeferral(t *testing.T) {
	t.Parallel()
	s := &actedSyncer{}
	health := &healthMock{stale: true}
	st := startActed(t, 0, s, 4, health)
	time.Sleep(500 * time.Millisecond)
	if r := st.Reserve().Radius(); r != 4 {
		t.Fatalf("radius %d while stale, want 4", r)
	}
	for _, reason := range []string{storer.DeferNotStarted, storer.DeferNotAtRadius, storer.DeferSettling} {
		if n := st.RadiusDecreaseDeferred(reason); n != 0 {
			t.Fatalf("deferral %s counted %v times while the stall gate holds, want 0", reason, n)
		}
	}
}
