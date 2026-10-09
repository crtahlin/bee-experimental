// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	batchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// Eviction and reserve samples (#649): no eviction round and no radius step
// runs while a sample runs, up to a cap per stretch of sampling.

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stays checks that cond holds for the whole of d.
func stays(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if !cond() {
			t.Fatalf("%s did not hold", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type evictResult struct {
	n   int
	err error
}

// startEviction evicts the batch below bin 1 in a goroutine, with the node's
// hooks, counting the rounds.
func startEviction(st *storer.DB, batchID []byte, expiry <-chan struct{}, rounds *atomic.Int32, observe func(round int32)) <-chan evictResult {
	done := make(chan evictResult, 1)
	go func() {
		n, err := st.EvictBatchBinWithExpiryForTest(context.Background(), batchID, 1, false, expiry, func(int) {
			r := rounds.Add(1)
			if observe != nil {
				observe(r)
			}
		})
		done <- evictResult{n, err}
	}()
	return done
}

func newEvictionStore(t *testing.T, rate int) (*storer.DB, swarm.Address) {
	t.Helper()
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 10_000, nil, nil, time.Minute)
	opts.ReserveEvictionRate = rate
	st, err := memStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}
	return st, baseAddr
}

// TestEvictionWaitsForSample checks, unpaced and paced, that no round runs
// while a sample runs, including the first, that paused time is counted,
// and that the eviction completes after the sample (spec tests 6 and 13).
func TestEvictionWaitsForSample(t *testing.T) {
	t.Parallel()
	for _, rate := range []int{0, 1_000_000} {
		t.Run(map[int]string{0: "unpaced", 1_000_000: "paced"}[rate], func(t *testing.T) {
			t.Parallel()
			st, baseAddr := newEvictionStore(t, rate)
			batch := postagetesting.MustNewBatch()
			putBin0(t, st, baseAddr, batch.ID, 2_500, 0)

			st.SampleStartedForTest()
			var rounds atomic.Int32
			done := startEviction(st, batch.ID, nil, &rounds, nil)
			stays(t, "no round during the sample", 500*time.Millisecond, func() bool { return rounds.Load() == 0 })
			st.SampleDoneForTest()

			r := <-done
			if r.err != nil || r.n != 2_500 {
				t.Fatalf("evicted %d, error %v, want 2,500 and no error", r.n, r.err)
			}
			if st.EvictionPausedSecondsForTest() < 0.4 {
				t.Fatalf("paused seconds %v, want about the sample duration", st.EvictionPausedSecondsForTest())
			}
		})
	}
}

// TestEvictionBarrierWaitsForRoundInProgress checks that a sample that
// starts while a round is in progress waits for that round only, and that
// the next round does not run until the sample ends (spec test 8).
func TestEvictionBarrierWaitsForRoundInProgress(t *testing.T) {
	t.Parallel()
	st, baseAddr := newEvictionStore(t, 0)
	batch := postagetesting.MustNewBatch()
	putBin0(t, st, baseAddr, batch.ID, 2_500, 0)

	inRound := make(chan struct{})
	finishRound := make(chan struct{})
	var rounds atomic.Int32
	done := startEviction(st, batch.ID, nil, &rounds, func(r int32) {
		if r == 1 {
			// After runs while the round still holds the barrier.
			close(inRound)
			<-finishRound
		}
	})
	<-inRound
	sampled := make(chan struct{})
	go func() {
		st.SampleStartedForTest()
		close(sampled)
	}()
	select {
	case <-sampled:
		t.Fatal("the sample started while an eviction round was in progress")
	case <-time.After(200 * time.Millisecond):
	}
	close(finishRound)
	select {
	case <-sampled:
	case <-time.After(10 * time.Second):
		t.Fatal("the sample still waits after the round ended")
	}
	stays(t, "no second round during the sample", 500*time.Millisecond, func() bool { return rounds.Load() == 1 })
	st.SampleDoneForTest()
	if r := <-done; r.err != nil || r.n != 2_500 {
		t.Fatalf("evicted %d, error %v, want 2,500 and no error", r.n, r.err)
	}
}

// TestEvictionWaitsForOverlappingSamples checks that when two samples
// overlap, eviction stays paused until the second ends (spec test 9).
func TestEvictionWaitsForOverlappingSamples(t *testing.T) {
	t.Parallel()
	st, baseAddr := newEvictionStore(t, 0)
	batch := postagetesting.MustNewBatch()
	putBin0(t, st, baseAddr, batch.ID, 1_500, 0)

	st.SampleStartedForTest()
	st.SampleStartedForTest()
	var rounds atomic.Int32
	done := startEviction(st, batch.ID, nil, &rounds, nil)
	st.SampleDoneForTest()
	stays(t, "no round while the second sample runs", 500*time.Millisecond, func() bool { return rounds.Load() == 0 })
	if !st.IsSampling() {
		t.Fatal("IsSampling is false while a sample still runs")
	}
	st.SampleDoneForTest()
	if r := <-done; r.err != nil || r.n != 1_500 {
		t.Fatalf("evicted %d, error %v, want 1,500 and no error", r.n, r.err)
	}
}

// TestEvictionPauseExits checks that a shutdown, a batch expiry and a
// cancelled context end a pause, with the same errors as between rounds
// (spec test 10).
func TestEvictionPauseExits(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"quit", "expiry"} {
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			baseAddr := swarm.RandAddress(t)
			// Not memStorer: the quit case closes the quit channel itself.
			st, err := storer.New(context.Background(), "", dbTestOps(baseAddr, 10_000, nil, nil, time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			batch := postagetesting.MustNewBatch()
			putBin0(t, st, baseAddr, batch.ID, 1_500, 0)

			st.SampleStartedForTest()
			expiry := make(chan struct{})
			var rounds atomic.Int32
			done := startEviction(st, batch.ID, expiry, &rounds, nil)
			time.Sleep(300 * time.Millisecond)
			if tc == "quit" {
				st.TriggerQuit()
			} else {
				close(expiry)
			}
			var r evictResult
			select {
			case r = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the pause did not end")
			}
			switch {
			case tc == "quit" && !errors.Is(r.err, storer.ErrDBQuit):
				t.Fatalf("got %v, want %v", r.err, storer.ErrDBQuit)
			case tc == "expiry" && !storer.IsEvictionExpiry(r.err):
				t.Fatalf("got %v, want the batch expiry error", r.err)
			}
			if r.n != 0 {
				t.Fatalf("evicted %d during the sample, want 0", r.n)
			}
			st.SampleDoneForTest()
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestEvictionPauseKeepsBatchUsable checks that while eviction is paused
// for a sample, a chunk of the same batch can still be stored (spec
// test 12): the pause holds no lock.
func TestEvictionPauseKeepsBatchUsable(t *testing.T) {
	t.Parallel()
	st, baseAddr := newEvictionStore(t, 0)
	batch := postagetesting.MustNewBatch()
	putBin0(t, st, baseAddr, batch.ID, 1_500, 0)

	st.SampleStartedForTest()
	var rounds atomic.Int32
	done := startEviction(st, batch.ID, nil, &rounds, nil)
	time.Sleep(200 * time.Millisecond)
	stored := make(chan error, 1)
	go func() {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 3).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		stored <- st.ReservePutForTest(context.Background(), ch)
	}()
	select {
	case err := <-stored:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a put of the batch waited for the paused eviction")
	}
	st.SampleDoneForTest()
	<-done
}

// unknownBatchReserve fills a reserve over capacity with chunks of a batch
// the batch store reports as existing but does not list, so unreserve finds
// nothing to evict and only raises the radius: the radius step on its own.
func unknownBatchReserve(t *testing.T) (*storer.DB, chan struct{}) {
	t.Helper()
	baseAddr := swarm.RandAddress(t)
	st, err := memStorer(t, dbTestOps(baseAddr, 10, batchstore.New(batchstore.WithAcceptAllExistsFunc()), nil, time.Minute))()
	if err != nil {
		t.Fatal(err)
	}
	putBin0(t, st, baseAddr, postagetesting.MustNewID(), 20, 5)
	ready := make(chan struct{})
	return st, ready
}

// TestNoRadiusStepDuringSample checks that unreserve does not raise the
// radius while a sample runs, and does once it ends (spec test 7).
func TestNoRadiusStepDuringSample(t *testing.T) {
	t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)
	st, ready := unknownBatchReserve(t)
	start := st.StorageRadius()

	st.SampleStartedForTest()
	st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), ready)
	<-ready
	stays(t, "radius during the sample", 500*time.Millisecond, func() bool { return st.StorageRadius() == start })
	st.SampleDoneForTest()
	waitUntil(t, "a radius step after the sample", func() bool { return st.StorageRadius() > start })
}

// TestEvictionPauseCap checks that once a stretch of sampling reaches the
// cap, rounds and radius steps run while the sample still runs, with one
// counted cap per stretch, and that the next stretch pauses again (spec
// test 11). Not parallel: it shortens the package-wide cap.
func TestEvictionPauseCap(t *testing.T) {
	defer storer.SetMaxEvictionPauseForTest(300 * time.Millisecond)()

	t.Run("rounds", func(t *testing.T) {
		st, baseAddr := newEvictionStore(t, 0)
		batch := postagetesting.MustNewBatch()
		putBin0(t, st, baseAddr, batch.ID, 3_500, 0)

		st.SampleStartedForTest()
		var rounds atomic.Int32
		done := startEviction(st, batch.ID, nil, &rounds, nil)
		r := <-done
		if r.err != nil || r.n != 3_500 {
			t.Fatalf("evicted %d, error %v, want all 3,500 while the sample still runs", r.n, r.err)
		}
		if got := rounds.Load(); got < 3 {
			t.Fatalf("%d rounds after the cap, want several", got)
		}
		if got := st.EvictionPauseCappedForTest(); got != 1 {
			t.Fatalf("capped counter %v, want 1 for one stretch", got)
		}
		st.SampleDoneForTest()

		// A new stretch pauses again.
		putBin0(t, st, baseAddr, batch.ID, 1_500, 0)
		st.SampleStartedForTest()
		var rounds2 atomic.Int32
		done = startEviction(st, batch.ID, nil, &rounds2, nil)
		stays(t, "no round at the start of a new stretch", 150*time.Millisecond, func() bool { return rounds2.Load() == 0 })
		st.SampleDoneForTest()
		<-done
	})

	t.Run("radius step", func(t *testing.T) {
		t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)
		st, ready := unknownBatchReserve(t)
		start := st.StorageRadius()
		st.SampleStartedForTest()
		defer st.SampleDoneForTest()
		st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), ready)
		<-ready
		waitUntil(t, "a radius step after the cap while the sample runs", func() bool { return st.StorageRadius() > start })
	})
}

// TestEvictingForFollowsTheWorker checks that EvictingFor is 0 before an
// eviction, positive while the reserve worker evicts, and 0 once the reserve
// is back within capacity (spec test 1).
func TestEvictingForFollowsTheWorker(t *testing.T) {
	t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)
	bs := batchstore.New()
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 100, bs, nil, time.Minute)
	opts.ReserveEvictionRate = 1_000
	st, err := memStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	putBin0(t, st, baseAddr, batch.ID, 2_100, 0)
	if got := st.EvictingFor(); got != 0 {
		t.Fatalf("before any eviction: %v, want 0", got)
	}

	ready := make(chan struct{})
	st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), ready)
	<-ready
	// About two seconds at 1,000 chunks/s after the first round.
	waitUntil(t, "an eviction in progress", func() bool { return st.EvictingFor() > 200*time.Millisecond })
	waitUntil(t, "the reserve within capacity", func() bool { return st.ReserveSize() <= 100 })
	waitUntil(t, "the episode closed", func() bool { return st.EvictingFor() == 0 })
}
