// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// healthMock is a postage.SyncHealth a test can switch.
type healthMock struct {
	mu       sync.Mutex
	stale    bool
	endedAt  time.Time
	progress time.Duration
}

func (h *healthMock) Stale() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stale
}

func (h *healthMock) SinceProgress() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.progress
}

func (h *healthMock) StaleEndedAt() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.endedAt
}

func (h *healthMock) set(stale bool, endedAt time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stale, h.endedAt = stale, endedAt
}

// TestRadiusNotLoweredWhileStale checks that the reserve worker does not
// lower the storage radius while the batch store is stale, nor within the
// hold after it, and lowers it once the hold has passed (#583).
func TestRadiusNotLoweredWhileStale(t *testing.T) {
	defer storer.SetRadiusDecreaseHold(time.Second)()

	baseAddr := swarm.RandAddress(t)
	bs := mock.New()
	st, err := memStorer(t, dbTestOps(baseAddr, 10, bs, nil, 100*time.Millisecond))()
	if err != nil {
		t.Fatal(err)
	}

	health := &healthMock{stale: true}
	st.SetPostageSyncHealth(health)

	readyC := make(chan struct{})
	st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(3), readyC)
	<-readyC

	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	putter := st.ReservePutter()
	for range 4 {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 4).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		if err := putter.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}

	// Under the threshold, sync rate 0: today the radius would drop.
	// While stale it must not.
	time.Sleep(600 * time.Millisecond)
	if r := st.Reserve().Radius(); r != 3 {
		t.Fatalf("radius %d while stale, want 3", r)
	}

	// Just recovered: still held for the hold window.
	health.set(false, time.Now())
	time.Sleep(400 * time.Millisecond)
	if r := st.Reserve().Radius(); r != 3 {
		t.Fatalf("radius %d within the hold after the stale state, want 3", r)
	}

	// After the hold the radius drops as today.
	if err := spinlock.Wait(10*time.Second, func() bool { return st.Reserve().Radius() < 3 }); err != nil {
		t.Fatalf("radius %d after the hold, want it lowered", st.Reserve().Radius())
	}
}
