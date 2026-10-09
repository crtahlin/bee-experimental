// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	batchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
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
// refused when the storer starts, with and without a reserve.
func TestNegativeEvictionRate(t *testing.T) {
	t.Parallel()
	for _, capacity := range []int{10, 0} {
		opts := dbTestOps(swarm.RandAddress(t), capacity, nil, nil, time.Minute)
		opts.ReserveEvictionRate = -1
		if _, err := storer.New(context.Background(), "", opts); err == nil {
			t.Fatalf("capacity %d: want an error for a negative eviction rate", capacity)
		}
	}
}

// putBin0 stores n chunks of a batch in bin 0 of the node's reserve,
// without triggering an eviction.
func putBin0(t *testing.T, st *storer.DB, baseAddr swarm.Address, batchID []byte, n, bin int) []swarm.Chunk {
	t.Helper()
	chunks := make([]swarm.Chunk, 0, n)
	for range n {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, bin).WithStamp(postagetesting.MustNewBatchStamp(batchID))
		if err := st.ReservePutForTest(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, ch)
	}
	return chunks
}

// TestEvictionCountersPerRound checks that the evicted or expired counter
// and the reserve size gauge are updated after each round of 1,000, not
// once per batch (#623).
func TestEvictionCountersPerRound(t *testing.T) {
	t.Parallel()
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "evicted", true: "expired"}[expired], func(t *testing.T) {
			t.Parallel()
			baseAddr := swarm.RandAddress(t)
			st, err := memStorer(t, dbTestOps(baseAddr, 10_000, nil, nil, time.Minute))()
			if err != nil {
				t.Fatal(err)
			}
			batch := postagetesting.MustNewBatch()
			putBin0(t, st, baseAddr, batch.ID, 2_500, 0)

			counter := st.EvictedChunkCountForTest
			if expired {
				counter = st.ExpiredChunkCountForTest
			}
			var counts, sizes []float64
			n, err := st.EvictBatchBinForTest(context.Background(), batch.ID, 1, expired, func(int) {
				counts = append(counts, counter())
				sizes = append(sizes, st.ReserveSizeGaugeForTest())
			})
			if err != nil {
				t.Fatal(err)
			}
			if n != 2_500 {
				t.Fatalf("evicted %d, want 2,500", n)
			}
			if want := []float64{1_000, 2_000, 2_500}; !slices.Equal(counts, want) {
				t.Fatalf("counter after each round %v, want %v", counts, want)
			}
			if want := []float64{1_500, 500, 0}; !slices.Equal(sizes, want) {
				t.Fatalf("reserve size after each round %v, want %v", sizes, want)
			}
		})
	}
}

// TestEvictionNodeKeepsServing checks, with the node's own eviction hooks,
// unpaced and paced, that while a batch is evicted new chunks are stored
// into it and into another batch, its chunks that stay are served, and no
// store or retrieval spans more than two rounds of the eviction: it gets
// the batch lock between rounds, instead of waiting for the whole eviction.
// Rounds are counted, not timed, so contention on a loaded runner does not
// decide the result.
func TestEvictionNodeKeepsServing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		rate int
	}{{"rate 0", 0}, {"paced", 2_000}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseAddr := swarm.RandAddress(t)
			opts := dbTestOps(baseAddr, 100_000, nil, nil, time.Minute)
			opts.ReserveEvictionRate = tc.rate
			st, err := memStorer(t, opts)()
			if err != nil {
				t.Fatal(err)
			}
			target, other := postagetesting.MustNewBatch(), postagetesting.MustNewBatch()
			doomed := putBin0(t, st, baseAddr, target.ID, 6_000, 0)
			kept := putBin0(t, st, baseAddr, target.ID, 20, 5)
			kept = append(kept, putBin0(t, st, baseAddr, other.ID, 20, 0)...)

			get := func(ch swarm.Chunk) error {
				stampHash, err := ch.Stamp().Hash()
				if err != nil {
					return err
				}
				_, err = st.ReserveGet(context.Background(), ch.Address(), ch.Stamp().BatchID(), stampHash)
				return err
			}

			var rounds atomic.Int64 // rounds completed, counted under the lock
			done := make(chan error, 1)
			go func() {
				_, err := st.EvictBatchBinForTest(context.Background(), target.ID, 1, false, func(int) { rounds.Add(1) })
				done <- err
			}()

			var (
				added      []swarm.Chunk
				maxSpanned int64
				ops        int
			)
		loop:
			for {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
					break loop
				default:
				}
				before := rounds.Load()
				added = append(added, putBin0(t, st, baseAddr, target.ID, 1, 6)...)
				added = append(added, putBin0(t, st, baseAddr, other.ID, 1, 0)...)
				if err := get(kept[ops%len(kept)]); err != nil {
					t.Fatalf("serving a kept chunk during the eviction: %v", err)
				}
				// Only operations that started before the last round count:
				// one that started after it cannot have been held by it.
				if before < 6 {
					maxSpanned = max(maxSpanned, rounds.Load()-before)
				}
				ops++
			}

			for _, ch := range append(kept, added...) {
				if err := get(ch); err != nil {
					t.Fatalf("chunk stored or kept during the eviction: %v, want it served", err)
				}
			}
			for _, ch := range doomed {
				if err := get(ch); !errors.Is(err, storage.ErrNotFound) {
					t.Fatalf("doomed chunk: %v, want not found", err)
				}
			}
			t.Logf("rounds %d, operations %d, most rounds one store and retrieval spanned %d", rounds.Load(), ops, maxSpanned)
			if got := rounds.Load(); got != 6 {
				t.Fatalf("%d rounds, want 6 (6,000 chunks in rounds of 1,000)", got)
			}
			if ops < 3 {
				t.Fatalf("only %d operations during the eviction", ops)
			}
			// A Put, Put and Get each wait at most for the round holding the
			// lock, so the three together span at most a few rounds. Without
			// the yield between rounds they wait for the whole eviction.
			if maxSpanned > 3 {
				t.Fatalf("a store and retrieval spanned %d of 6 rounds: the batch lock was held across rounds", maxSpanned)
			}
		})
	}
}

// TestPacedEvictionStops checks, at a rate of 1 per second, that a batch
// expiry during a wait ends unreserve, and that Close during a wait stops
// the reserve worker promptly (#623).
//
// Not parallel: its reserve worker sets the within-radius count, a package
// variable every DB in the process shares, to about 2,600, and the windowed
// sample tests read it. Sequential tests end before parallel ones resume,
// and the count is cleared when this test ends.
func TestPacedEvictionStops(t *testing.T) {
	t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)
	bs := batchstore.New()
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 100, bs, nil, time.Minute)
	opts.ReserveEvictionRate = 1
	// Not memStorer: the test closes the store itself, and the in-memory
	// store cannot be closed twice.
	st, err := storer.New(context.Background(), "", opts)
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	putBin0(t, st, baseAddr, batch.ID, 2_600, 0)

	unreserved, unsub := st.Events().Subscribe("reserveUnreserved")
	defer unsub()
	ready := make(chan struct{})
	st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), ready)
	<-ready

	// The burst is one round: the first 1,000 go at once, the second
	// round then waits about 1,000 s.
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor("the second round", func() bool { return st.EvictedChunkCountForTest() >= 2_000 })

	// An expiry of any batch ends the wait, and unreserve returns.
	if err := st.EvictBatch(context.Background(), postagetesting.MustNewID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-unreserved:
	case <-time.After(10 * time.Second):
		t.Fatal("unreserve did not stop on a batch expiry during its wait")
	}
	// The prompt event is the proof: without the expiry the wait would
	// last about 1,000 s. The counter is not compared here, because the
	// worker may already have started the next unreserve.

	// The reserve is still over capacity, so the worker starts another
	// unreserve, whose rounds wait for tokens. Close must not wait for it.
	waitFor("the next round", func() bool { return st.EvictedChunkCountForTest() > 2_000 })
	closed := make(chan error, 1)
	go func() { closed <- st.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close did not stop a paced eviction during its wait")
	}
}

// TestUnpacedEvictionStopsBetweenRounds checks that at rate 0, where no
// round waits, a shutdown or a batch expiry still stops an eviction between
// two rounds of the same batch (#623).
func TestUnpacedEvictionStopsBetweenRounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"quit", "expiry"} {
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			baseAddr := swarm.RandAddress(t)
			st, err := storer.New(context.Background(), "", dbTestOps(baseAddr, 10_000, nil, nil, time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			batch := postagetesting.MustNewBatch()
			putBin0(t, st, baseAddr, batch.ID, 2_500, 0)

			var expiry chan struct{}
			if tc == "quit" {
				st.TriggerQuit()
			} else {
				expiry = make(chan struct{})
				close(expiry)
			}
			n, err := st.EvictBatchBinWithExpiryForTest(context.Background(), batch.ID, 1, false, expiry, func(int) {})
			switch {
			case tc == "quit" && !errors.Is(err, storer.ErrDBQuit):
				t.Fatalf("got %v, want %v", err, storer.ErrDBQuit)
			case tc == "expiry" && !storer.IsEvictionExpiry(err):
				t.Fatalf("got %v, want the batch expiry error", err)
			}
			if n != 1_000 {
				t.Fatalf("evicted %d, want one round of 1,000", n)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
