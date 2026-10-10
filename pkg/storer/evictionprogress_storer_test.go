// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"testing"
	"time"

	batchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// The remaining gauge follows the reserve's eviction target through an
// unreserve run and ends at the true remainder (#626).
func TestEvictionRemainingGauge(t *testing.T) {
	t.Parallel()

	baseAddr := swarm.RandAddress(t)
	bs := batchstore.New()
	opts := dbTestOps(baseAddr, 20, bs, nil, time.Minute)
	opts.ReserveMinEvictCount = 1
	st, err := memStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}

	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	putter := st.ReservePutter()
	for range 60 {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		if err := putter.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}
	target := st.EvictionTargetForTest()
	if target <= 0 {
		t.Fatal("reserve not over capacity")
	}
	st.RefreshEvictionProgressForTest()
	if got := st.EvictionRemainingForTest(); got != float64(target) {
		t.Fatalf("remaining gauge %v over capacity, want the eviction target %d", got, target)
	}

	if err := st.Unreserve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := st.EvictionRemainingForTest(), float64(max(0, st.EvictionTargetForTest())); got != want {
		t.Fatalf("remaining gauge %v after the run, want the eviction target %v", got, want)
	}
}

// The expired-batches gauge counts the batches left and is 0 after the run.
func TestEvictionExpiredBatchesGauge(t *testing.T) {
	t.Parallel()

	baseAddr := swarm.RandAddress(t)
	st, err := memStorer(t, dbTestOps(baseAddr, 1000, batchstore.New(), nil, time.Minute))()
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		b := postagetesting.MustNewBatch()
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewBatchStamp(b.ID))
		if err := st.ReservePutter().Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
		if err := st.EvictBatch(context.Background(), b.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.EvictExpiredBatches(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := st.ExpiredBatchesRemainingForTest(); got != 0 {
		t.Fatalf("expired batches remaining %v after the run, want 0", got)
	}
}
