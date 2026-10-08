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
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestUnreservePaced checks that with reserve-eviction-rate set, an
// over-capacity reserve is still brought back to capacity, and the chunks
// below the new radius are the ones evicted (#623).
func TestUnreservePaced(t *testing.T) {
	t.Parallel()

	const (
		storageRadius = 2
		capacity      = 30
		perPO         = 10
	)

	for _, name := range []string{"disk", "mem"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			bs := batchstore.New()
			baseAddr := swarm.RandAddress(t)
			opts := dbTestOps(baseAddr, capacity, bs, nil, time.Minute)
			opts.ReserveEvictionRate = 1_000
			newStorer := memStorer(t, opts)
			if name == "disk" {
				newStorer = diskStorer(t, opts)
			}
			st, err := newStorer()
			if err != nil {
				t.Fatal(err)
			}
			readyC := make(chan struct{})
			st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), readyC)
			<-readyC

			batch := postagetesting.MustNewBatch()
			if err := bs.Save(batch); err != nil {
				t.Fatal(err)
			}
			unreserved, unsub := st.Events().Subscribe("reserveUnreserved")
			defer unsub()

			chunksPO := make([][]swarm.Chunk, 5)
			putter := st.ReservePutter()
			for b := range 5 {
				for range perPO {
					ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, b).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
					chunksPO[b] = append(chunksPO[b], ch)
					if err := putter.Put(context.Background(), ch); err != nil {
						t.Fatal(err)
					}
				}
			}

			deadline := time.After(30 * time.Second)
			for st.ReserveSize() != capacity {
				select {
				case <-unreserved:
				case <-deadline:
					t.Fatalf("reserve size %d, want %d", st.ReserveSize(), capacity)
				}
			}

			for po, chunks := range chunksPO {
				for _, ch := range chunks {
					stampHash, err := ch.Stamp().Hash()
					if err != nil {
						t.Fatal(err)
					}
					has, err := st.ReserveHas(ch.Address(), ch.Stamp().BatchID(), stampHash)
					if err != nil {
						t.Fatal(err)
					}
					if po < storageRadius && has {
						t.Fatalf("chunk at PO %d kept, want it evicted", po)
					}
					if po >= storageRadius && !has {
						t.Fatalf("chunk at PO %d evicted, want it kept", po)
					}
				}
			}
		})
	}
}

// TestNegativeEvictionRate checks that a negative reserve-eviction-rate is
// refused when the storer starts.
func TestNegativeEvictionRate(t *testing.T) {
	t.Parallel()
	opts := dbTestOps(swarm.RandAddress(t), 10, nil, nil, time.Minute)
	opts.ReserveEvictionRate = -1
	if _, err := storer.New(context.Background(), "", opts); err == nil {
		t.Fatal("want an error for a negative eviction rate")
	}
}
