// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/postage"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	storage "github.com/ethersphere/bee/v2/pkg/storage"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// knownBatches is a stamp validator that knows only the batches a test adds,
// and answers postage.ErrNotFound for the others, like a stale batch store.
type knownBatches struct {
	mu    sync.Mutex
	known map[string]bool
}

func newKnownBatches() *knownBatches { return &knownBatches{known: map[string]bool{}} }

func (k *knownBatches) add(id []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.known[string(id)] = true
}

func (k *knownBatches) validate(ch swarm.Chunk) (swarm.Chunk, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.known[string(ch.Stamp().BatchID())] {
		return nil, fmt.Errorf("batchstore get: %w, %w", storage.ErrNotFound, postage.ErrNotFound)
	}
	return ch, nil
}

type heldFixture struct {
	db      *storer.DB
	health  *healthMock
	batches *knownBatches
	base    swarm.Address
}

func newHeldFixture(t *testing.T, path string) *heldFixture {
	t.Helper()
	f := &heldFixture{health: &healthMock{stale: true}, batches: newKnownBatches(), base: swarm.RandAddress(t)}
	f.open(t, path)
	return f
}

func (f *heldFixture) options() *storer.Options {
	opts := dbTestOps(f.base, 1000, nil, nil, time.Hour)
	opts.ValidStamp = f.batches.validate
	return opts
}

func (f *heldFixture) open(t *testing.T, path string) {
	t.Helper()
	db, err := storer.New(context.Background(), path, f.options())
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	db.SetPostageSyncHealth(f.health)
	ready := make(chan struct{})
	db.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), ready)
	<-ready
}

// unknownChunk returns a chunk inside the radius stamped with a fresh batch
// the validator does not know.
func (f *heldFixture) unknownChunk(t *testing.T) (swarm.Chunk, []byte) {
	t.Helper()
	batch := postagetesting.MustNewID()
	return chunk.GenerateTestRandomChunkAt(t, f.base, 0).WithStamp(postagetesting.MustNewBatchStamp(batch)), batch
}

var errUnknownBatch = fmt.Errorf("batchstore get: %w, %w", storage.ErrNotFound, postage.ErrNotFound)

func (f *heldFixture) hold(t *testing.T, ch swarm.Chunk) {
	t.Helper()
	held, err := f.db.HoldUnvalidated(context.Background(), ch, errUnknownBatch)
	if err != nil || !held {
		t.Fatalf("hold: held %v, error %v", held, err)
	}
}

// TestHoldOnlyWhileStale checks that a chunk is held only while the batch
// store is stale, and only for a cause a current batch store can resolve.
func TestHoldOnlyWhileStale(t *testing.T) {
	f := newHeldFixture(t, "")
	ch, _ := f.unknownChunk(t)

	if held, err := f.db.HoldUnvalidated(context.Background(), ch, postage.ErrOwnerMismatch); held || err != nil {
		t.Fatalf("held %v, error %v for an owner mismatch, want neither", held, err)
	}

	f.health.set(false, time.Time{})
	if held, err := f.db.HoldUnvalidated(context.Background(), ch, errUnknownBatch); held || err != nil {
		t.Fatalf("held %v, error %v while the batch store is current, want neither", held, err)
	}

	f.health.set(true, time.Time{})
	f.hold(t, ch)
	if held, err := f.db.HoldUnvalidated(context.Background(), ch, postage.ErrInvalidIndex); !held || err != nil {
		t.Fatalf("held %v, error %v for an index beyond the known depth, want held", held, err)
	}
}

// TestHeldChunkServed checks that a held chunk is served by retrieval's
// lookup and is pending validation, which keeps the lottery gate closed.
func TestHeldChunkServed(t *testing.T) {
	f := newHeldFixture(t, "")
	ch, _ := f.unknownChunk(t)
	f.hold(t, ch)

	got, err := f.db.Lookup().Get(context.Background(), ch.Address())
	if err != nil {
		t.Fatalf("lookup of a held chunk: %v", err)
	}
	if !got.Address().Equal(ch.Address()) {
		t.Fatal("lookup returned another chunk")
	}
	if !f.db.HeldPending() {
		t.Fatal("held chunk not reported pending")
	}
}

// TestHeldRoomPerAddress checks that the bound counts addresses: the same
// stamp twice, or a second stamp for the same address, takes no further
// room, also when handlers race.
func TestHeldRoomPerAddress(t *testing.T) {
	f := newHeldFixture(t, "")
	ch, _ := f.unknownChunk(t)
	f.hold(t, ch)
	f.hold(t, ch)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			other := swarm.NewChunk(ch.Address(), ch.Data()).WithStamp(postagetesting.MustNewBatchStamp(postagetesting.MustNewID()))
			if held, err := f.db.HoldUnvalidated(context.Background(), other, errUnknownBatch); !held || err != nil {
				t.Errorf("held %v, error %v", held, err)
			}
		}()
	}
	wg.Wait()
	if n := f.db.HeldCount(); n != 1 {
		t.Fatalf("held count %d for one address, want 1", n)
	}
}

// TestHeldAreaFull checks that a full held area refuses new addresses with
// ErrHeldAreaFull, keeps what it holds, and reports full so pulling pauses.
func TestHeldAreaFull(t *testing.T) {
	defer storer.SetHeldLimits(2, time.Hour, 1000, time.Millisecond)()

	f := newHeldFixture(t, "")
	first, _ := f.unknownChunk(t)
	second, _ := f.unknownChunk(t)
	f.hold(t, first)
	f.hold(t, second)

	third, _ := f.unknownChunk(t)
	if held, err := f.db.HoldUnvalidated(context.Background(), third, errUnknownBatch); held || !errors.Is(err, postage.ErrHeldAreaFull) {
		t.Fatalf("held %v, error %v on a full area, want ErrHeldAreaFull", held, err)
	}
	if !f.db.HeldFull() {
		t.Fatal("full held area not reported full")
	}
	for _, ch := range []swarm.Chunk{first, second} {
		if _, err := f.db.Lookup().Get(context.Background(), ch.Address()); err != nil {
			t.Fatalf("held chunk evicted by a full area: %v", err)
		}
	}
	// A second stamp for an address already held needs no room.
	again := swarm.NewChunk(first.Address(), first.Data()).WithStamp(postagetesting.MustNewBatchStamp(postagetesting.MustNewID()))
	f.hold(t, again)
}

// TestHeldValidatedAfterCatchUp checks that held chunks wait while the
// listener is not caught up, and then a valid one is promoted to the reserve
// and an invalid one dropped, each releasing its held entry (#583).
func TestHeldValidatedAfterCatchUp(t *testing.T) {
	defer storer.SetHeldLimits(100, 10*time.Millisecond, 1000, time.Millisecond)()

	f := newHeldFixture(t, "")
	valid, validBatch := f.unknownChunk(t)
	invalid, _ := f.unknownChunk(t)
	f.hold(t, valid)
	f.hold(t, invalid)

	// The batch appears on chain, but the listener is not caught up yet:
	// nothing is validated against a stale batch store.
	f.batches.add(validBatch)
	f.health.set(false, time.Now())
	time.Sleep(100 * time.Millisecond)
	if n := f.db.HeldCount(); n != 2 {
		t.Fatalf("held count %d before catch-up, want 2", n)
	}

	f.health.mu.Lock()
	f.health.caughtUp = true
	f.health.mu.Unlock()
	if err := spinlock.Wait(5*time.Second, func() bool { return f.db.HeldCount() == 0 }); err != nil {
		t.Fatalf("held count %d after catch-up, want 0", f.db.HeldCount())
	}

	stamp := valid.Stamp().(*postage.Stamp)
	hash, _ := stamp.Hash()
	if has, err := f.db.ReserveHas(valid.Address(), validBatch, hash); err != nil || !has {
		t.Fatalf("valid held chunk not promoted to the reserve: has %v, error %v", has, err)
	}
	if _, err := f.db.Lookup().Get(context.Background(), invalid.Address()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("invalid held chunk still stored: %v", err)
	}
	if f.db.HeldPending() {
		t.Fatal("held chunks reported pending after validation")
	}
}

// TestHeldMissingChunkDropped checks that a held entry whose chunk is gone
// from the chunk store is dropped by validation.
func TestHeldMissingChunkDropped(t *testing.T) {
	defer storer.SetHeldLimits(100, 10*time.Millisecond, 1000, time.Millisecond)()

	f := newHeldFixture(t, "")
	ch, batch := f.unknownChunk(t)
	f.hold(t, ch)
	if err := f.db.DeleteChunkForTest(context.Background(), ch.Address()); err != nil {
		t.Fatal(err)
	}
	f.batches.add(batch)
	f.health.mu.Lock()
	f.health.stale, f.health.caughtUp = false, true
	f.health.mu.Unlock()
	if err := spinlock.Wait(5*time.Second, func() bool { return f.db.HeldCount() == 0 }); err != nil {
		t.Fatalf("held entry with a missing chunk not dropped, count %d", f.db.HeldCount())
	}
}

// TestHeldSurvivesRestart checks that held entries survive a restart, the
// count is rebuilt, and a batch created after the stored chain state is
// promoted once the listener is caught up, not dropped at startup (#583).
func TestHeldSurvivesRestart(t *testing.T) {
	defer storer.SetHeldLimits(100, 10*time.Millisecond, 1000, time.Millisecond)()

	dir := t.TempDir()
	f := newHeldFixture(t, dir)
	ch, batch := f.unknownChunk(t)
	f.hold(t, ch)
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: current, but not caught up yet; the batch is not known yet
	// either, as after a restart from older chain state.
	f.health = &healthMock{}
	f.open(t, dir)
	t.Cleanup(func() { _ = f.db.Close() })
	if n := f.db.HeldCount(); n != 1 {
		t.Fatalf("held count %d after restart, want 1", n)
	}
	time.Sleep(100 * time.Millisecond)
	if n := f.db.HeldCount(); n != 1 {
		t.Fatalf("held count %d before catch-up after restart, want 1: validated against the stored chain state", n)
	}

	f.batches.add(batch)
	f.health.mu.Lock()
	f.health.caughtUp = true
	f.health.mu.Unlock()
	if err := spinlock.Wait(5*time.Second, func() bool { return f.db.HeldCount() == 0 }); err != nil {
		t.Fatalf("held count %d after catch-up, want 0", f.db.HeldCount())
	}
	stamp := ch.Stamp().(*postage.Stamp)
	hash, _ := stamp.Hash()
	if has, err := f.db.ReserveHas(ch.Address(), batch, hash); err != nil || !has {
		t.Fatalf("held chunk of a batch created after the stored state not promoted: has %v, error %v", has, err)
	}
}

// TestHeldNotEvictedByCache checks that cache eviction does not remove a
// held chunk: it is not in the cache order index (#583).
func TestHeldNotEvictedByCache(t *testing.T) {
	f := &heldFixture{health: &healthMock{stale: true}, batches: newKnownBatches(), base: swarm.RandAddress(t)}
	opts := f.options()
	opts.CacheCapacity = 10
	db, err := storer.New(context.Background(), "", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	f.db = db
	db.SetPostageSyncHealth(f.health)

	ch, _ := f.unknownChunk(t)
	f.hold(t, ch)

	for range 50 {
		if err := db.Cache().Put(context.Background(), chunk.GenerateTestRandomChunk()); err != nil {
			t.Fatal(err)
		}
	}
	if err := spinlock.WaitWithInterval(5*time.Second, 50*time.Millisecond, func() bool {
		info, err := db.DebugInfo(context.Background())
		return err == nil && info.Cache.Size == 10
	}); err != nil {
		t.Fatal("cache was not evicted down to its capacity")
	}
	if _, err := db.Lookup().Get(context.Background(), ch.Address()); err != nil {
		t.Fatalf("held chunk removed by cache eviction: %v", err)
	}
}

// catchUp makes the batch store current and caught up.
func (f *heldFixture) catchUp() {
	f.health.mu.Lock()
	f.health.stale, f.health.caughtUp = false, true
	f.health.endedAt = time.Now()
	f.health.mu.Unlock()
}

// TestHeldOutsideRadiusDropped checks that a valid held chunk outside the
// storage radius is dropped after catch-up, with its chunk-store reference,
// rather than promoted (#583).
func TestHeldOutsideRadiusDropped(t *testing.T) {
	defer storer.SetHeldLimits(100, 10*time.Millisecond, 1000, time.Millisecond)()

	f := newHeldFixture(t, "")
	if err := f.db.Reserve().SetRadius(2); err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewID()
	ch := chunk.GenerateTestRandomChunkAt(t, f.base, 0).WithStamp(postagetesting.MustNewBatchStamp(batch))
	f.hold(t, ch)

	f.batches.add(batch)
	f.catchUp()
	if err := spinlock.Wait(5*time.Second, func() bool { return f.db.HeldCount() == 0 }); err != nil {
		t.Fatalf("held count %d after catch-up, want 0", f.db.HeldCount())
	}
	hash, _ := ch.Stamp().Hash()
	if has, _ := f.db.ReserveHas(ch.Address(), batch, hash); has {
		t.Fatal("held chunk outside the radius was promoted")
	}
	if _, err := f.db.Lookup().Get(context.Background(), ch.Address()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("dropped held chunk still stored: %v", err)
	}
}

// TestHeldPromotionRepeated checks that a promotion interrupted after the
// reserve put, and so run again, leaves one reserve entry, no held entry,
// and the chunk still readable: dropping the held entry releases only its
// own chunk-store reference (#583).
func TestHeldPromotionRepeated(t *testing.T) {
	defer storer.SetHeldLimits(100, 10*time.Millisecond, 1000, time.Millisecond)()

	f := newHeldFixture(t, "")
	ch, batch := f.unknownChunk(t)
	f.hold(t, ch)

	// The first step of a promotion, as if the node stopped right after it.
	if err := f.db.ReservePutter().Put(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	sizeBefore := f.db.ReserveSize()

	f.batches.add(batch)
	f.catchUp()
	if err := spinlock.Wait(5*time.Second, func() bool { return f.db.HeldCount() == 0 }); err != nil {
		t.Fatalf("held count %d after catch-up, want 0", f.db.HeldCount())
	}
	if got := f.db.ReserveSize(); got != sizeBefore {
		t.Fatalf("reserve size %d after the repeated promotion, want %d", got, sizeBefore)
	}
	got, err := f.db.Lookup().Get(context.Background(), ch.Address())
	if err != nil {
		t.Fatalf("promoted chunk not readable: %v", err)
	}
	if !got.Address().Equal(ch.Address()) {
		t.Fatalf("got chunk %s, want %s", got.Address(), ch.Address())
	}
}

// TestHeldConcurrentClaimOnce checks that handlers holding the same address
// at once, with different stamps, claim room for it once (#583).
func TestHeldConcurrentClaimOnce(t *testing.T) {
	defer storer.SetHeldLimits(100, time.Hour, 1000, time.Millisecond)()

	f := newHeldFixture(t, "")
	base := chunk.GenerateTestRandomChunkAt(t, f.base, 0)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		ch := swarm.NewChunk(base.Address(), base.Data()).WithStamp(postagetesting.MustNewBatchStamp(postagetesting.MustNewID()))
		wg.Go(func() {
			held, err := f.db.HoldUnvalidated(context.Background(), ch, errUnknownBatch)
			if err == nil && !held {
				err = errors.New("not held")
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	if n := f.db.HeldCount(); n != 1 {
		t.Fatalf("held count %d for one address held 8 times, want 1", n)
	}
}
