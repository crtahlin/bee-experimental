// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package reserve_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemstore"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
	"github.com/ethersphere/bee/v2/pkg/storage/pebblestore"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kademlia "github.com/ethersphere/bee/v2/pkg/topology/mock"
)

// indexStorage is a transaction.Storage over any index store and an
// in-memory chunk store, so the rounds can be tested on every engine.
type indexStorage struct {
	idx storage.IndexStore
	cs  storage.ChunkStore
}

type indexTrx struct {
	idx storage.IndexStore
	cs  storage.ChunkStore
}

func (s *indexStorage) NewTransaction(context.Context) (transaction.Transaction, func()) {
	return &indexTrx{s.idx, s.cs}, func() {}
}
func (s *indexStorage) IndexStore() storage.Reader             { return s.idx }
func (s *indexStorage) ChunkStore() storage.ReadOnlyChunkStore { return s.cs }
func (s *indexStorage) Close() error                           { return nil }
func (s *indexStorage) Run(ctx context.Context, f func(transaction.Store) error) error {
	trx, done := s.NewTransaction(ctx)
	defer done()
	return f(trx)
}
func (t *indexTrx) IndexStore() storage.IndexStore { return t.idx }
func (t *indexTrx) ChunkStore() storage.ChunkStore { return t.cs }
func (t *indexTrx) Commit() error                  { return nil }

// engines returns a fresh storage per index engine: in memory, leveldb and
// pebble. leveldb and pebble position a PrefixAtStart query differently,
// and the rounds resume with it.
func engines(t *testing.T) map[string]func(t *testing.T) transaction.Storage {
	t.Helper()
	return map[string]func(t *testing.T) transaction.Storage{
		"inmem": func(t *testing.T) transaction.Storage {
			t.Helper()
			return &indexStorage{idx: inmemstore.New(), cs: inmemchunkstore.New()}
		},
		"leveldb": func(t *testing.T) transaction.Storage {
			t.Helper()
			st, _, err := leveldbstore.New("", nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return &indexStorage{idx: st, cs: inmemchunkstore.New()}
		},
		"pebble": func(t *testing.T) transaction.Storage {
			t.Helper()
			st, err := pebblestore.New(t.TempDir(), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return &indexStorage{idx: st, cs: inmemchunkstore.New()}
		},
	}
}

// batchIDs returns three batch IDs that sort next to each other, so a
// round that resumes near the end of the middle one would read into the
// next batch's entries if it did not stop at the batch boundary.
func batchIDs() (before, target, after []byte) {
	target = bytes.Repeat([]byte{0x40}, 32)
	before = bytes.Clone(target)
	before[31] = 0x3f
	after = bytes.Clone(target)
	after[31] = 0x41
	return before, target, after
}

func countRadiusItems(t *testing.T, st transaction.Storage) int {
	t.Helper()
	n, err := st.IndexStore().Count(&reserve.BatchRadiusItem{})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestEvictRoundsResume checks, on every index engine, that eviction in
// rounds deletes exactly the batch's chunks below the bin, never more than
// one round at a time, and never touches the neighbouring batches.
func TestEvictRoundsResume(t *testing.T) {
	defer reserve.SetEvictionRound(3)()

	for name, newStorage := range engines(t) {
		t.Run(name, func(t *testing.T) {
			baseAddr := swarm.RandAddress(t)
			st := newStorage(t)
			r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
			if err != nil {
				t.Fatal(err)
			}

			before, target, after := batchIDs()
			const perBin, bins, evictBelow = 4, 6, 3
			byBatch := map[string][]swarm.Chunk{}
			for _, id := range [][]byte{before, target, after} {
				for b := range bins {
					for range perBin {
						ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, b).WithStamp(postagetesting.MustNewBatchStamp(id))
						if err := r.Put(context.Background(), ch); err != nil {
							t.Fatal(err)
						}
						byBatch[string(id)] = append(byBatch[string(id)], ch)
					}
				}
			}

			var rounds []int
			evicted, err := r.EvictBatchBin(context.Background(), target, math.MaxInt, evictBelow, reserve.EvictionHooks{
				After: func(n int, _ time.Duration) { rounds = append(rounds, n) },
			})
			if err != nil {
				t.Fatal(err)
			}

			want := perBin * evictBelow
			if evicted != want {
				t.Fatalf("evicted %d, want %d", evicted, want)
			}
			if len(rounds) != want/3 {
				t.Fatalf("got %d rounds %v, want %d", len(rounds), rounds, want/3)
			}
			for _, n := range rounds {
				if n > 3 {
					t.Fatalf("a round deleted %d items, more than the round size 3: %v", n, rounds)
				}
			}

			for id, chunks := range byBatch {
				for _, ch := range chunks {
					stampHash, err := ch.Stamp().Hash()
					if err != nil {
						t.Fatal(err)
					}
					bin := swarm.Proximity(baseAddr.Bytes(), ch.Address().Bytes())
					_, err = r.Get(context.Background(), ch.Address(), ch.Stamp().BatchID(), stampHash)
					gone := id == string(target) && bin < evictBelow
					switch {
					case gone && !errors.Is(err, storage.ErrNotFound):
						t.Fatalf("batch %x bin %d: got %v, want not found", id[31], bin, err)
					case !gone && err != nil:
						t.Fatalf("batch %x bin %d: got %v, want the chunk kept", id[31], bin, err)
					}
				}
			}

			if got, want := r.Size(), countRadiusItems(t, st); got != want {
				t.Fatalf("reserve size %d, but %d items are stored", got, want)
			}
		})
	}
}

// TestEvictRoundsBatchBoundary checks, on every index engine, that a round
// stops at the end of the batch when nothing at or above the bin follows
// it: the next batch's entries, which sort right after, must not be read.
// This is the case of an expired batch (bin swarm.MaxBins), or of a batch
// with chunks only below the bin.
func TestEvictRoundsBatchBoundary(t *testing.T) {
	defer reserve.SetEvictionRound(3)()

	for name, newStorage := range engines(t) {
		t.Run(name, func(t *testing.T) {
			baseAddr := swarm.RandAddress(t)
			st := newStorage(t)
			r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
			if err != nil {
				t.Fatal(err)
			}
			_, target, after := batchIDs()
			for _, id := range [][]byte{target, after} {
				for b := range 3 {
					for range 4 {
						ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, b).WithStamp(postagetesting.MustNewBatchStamp(id))
						if err := r.Put(context.Background(), ch); err != nil {
							t.Fatal(err)
						}
					}
				}
			}

			evicted, err := r.EvictBatchBin(context.Background(), target, math.MaxInt, swarm.MaxBins, reserve.EvictionHooks{})
			if err != nil {
				t.Fatal(err)
			}
			if evicted != 12 {
				t.Fatalf("evicted %d, want the target batch's 12", evicted)
			}
			if got := countRadiusItems(t, st); got != 12 {
				t.Fatalf("%d items left, want the next batch's 12", got)
			}
		})
	}
}

// recordingIndexStore records the queries of BatchRadiusItem iterations
// and how many results each one handed to its callback.
type recordingIndexStore struct {
	storage.IndexStore
	queries   []storage.Query
	callbacks []int
}

func (s *recordingIndexStore) Iterate(q storage.Query, fn storage.IterateFn) error {
	if _, ok := q.Factory().(*reserve.BatchRadiusItem); !ok {
		return s.IndexStore.Iterate(q, fn)
	}
	s.queries = append(s.queries, q)
	s.callbacks = append(s.callbacks, 0)
	i := len(s.callbacks) - 1
	return s.IndexStore.Iterate(q, func(r storage.Result) (bool, error) {
		s.callbacks[i]++
		return fn(r)
	})
}

// TestEvictRoundsResumeQueries checks, on every index engine, that each
// round after the first resumes at the last item the previous round read
// (PrefixAtStart from that item's ID), so it does not walk again over the
// entries earlier rounds deleted, and that no round reads more than one
// round of items plus the one that ends it.
func TestEvictRoundsResumeQueries(t *testing.T) {
	defer reserve.SetEvictionRound(3)()
	defer reserve.SetEvictionYield(0)()

	for name, newStorage := range engines(t) {
		t.Run(name, func(t *testing.T) {
			baseAddr := swarm.RandAddress(t)
			plain := newStorage(t).(*indexStorage)
			rec := &recordingIndexStore{IndexStore: plain.idx}
			st := &indexStorage{idx: rec, cs: plain.cs}
			r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
			if err != nil {
				t.Fatal(err)
			}
			_, target, after := batchIDs()
			ids := make([]string, 0, 12)
			for _, id := range [][]byte{target, after} {
				for range 12 {
					ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewBatchStamp(id))
					if err := r.Put(context.Background(), ch); err != nil {
						t.Fatal(err)
					}
					if bytes.Equal(id, target) {
						stampHash, err := ch.Stamp().Hash()
						if err != nil {
							t.Fatal(err)
						}
						item := &reserve.BatchRadiusItem{Bin: 0, BatchID: id, Address: ch.Address(), StampHash: stampHash}
						ids = append(ids, item.ID())
					}
				}
			}
			slices.Sort(ids)
			rec.queries, rec.callbacks = nil, nil

			evicted, err := r.EvictBatchBin(context.Background(), target, math.MaxInt, 1, reserve.EvictionHooks{})
			if err != nil {
				t.Fatal(err)
			}
			if evicted != 12 {
				t.Fatalf("evicted %d, want 12", evicted)
			}
			// 4 full rounds of 3, then one that finds nothing left.
			if len(rec.queries) != 5 {
				t.Fatalf("got %d queries, want 5", len(rec.queries))
			}
			for k, q := range rec.queries {
				if k == 0 {
					if q.PrefixAtStart || q.Prefix != string(target) {
						t.Fatalf("query 1: %+v, want Prefix the batch ID without PrefixAtStart", q)
					}
				} else {
					want := ids[3*k-1]
					if !q.PrefixAtStart || q.Prefix != want || q.SkipFirst {
						t.Fatalf("query %d: PrefixAtStart %v SkipFirst %v, prefix the ID of item %d: %v", k+1, q.PrefixAtStart, q.SkipFirst, 3*k, q.Prefix == want)
					}
				}
				if rec.callbacks[k] > 3+1 {
					t.Fatalf("query %d handed %d results to the callback, more than a round plus one", k+1, rec.callbacks[k])
				}
			}
		})
	}
}

// TestEvictRoundsCount checks that a count smaller than the batch's
// entries is honoured across rounds.
func TestEvictRoundsCount(t *testing.T) {
	defer reserve.SetEvictionRound(3)()

	baseAddr := swarm.RandAddress(t)
	st := &indexStorage{idx: inmemstore.New(), cs: inmemchunkstore.New()}
	r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	for range 10 {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		if err := r.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}

	evicted, err := r.EvictBatchBin(context.Background(), batch.ID, 5, swarm.MaxBins, reserve.EvictionHooks{})
	if err != nil {
		t.Fatal(err)
	}
	if evicted != 5 {
		t.Fatalf("evicted %d, want 5", evicted)
	}
	if r.Size() != 5 || countRadiusItems(t, st) != 5 {
		t.Fatalf("size %d, items %d, want 5 and 5", r.Size(), countRadiusItems(t, st))
	}
}

// TestEvictRoundsPayWithoutLock checks that Pay is called with the batch
// lock released: a Put into the batch from inside Pay must not block.
func TestEvictRoundsPayWithoutLock(t *testing.T) {
	defer reserve.SetEvictionRound(2)()

	baseAddr := swarm.RandAddress(t)
	st := &indexStorage{idx: inmemstore.New(), cs: inmemchunkstore.New()}
	r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	for range 6 {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		if err := r.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}

	pays := 0
	_, err = r.EvictBatchBin(context.Background(), batch.ID, math.MaxInt, 1, reserve.EvictionHooks{
		Pay: func(n int) error {
			pays++
			done := make(chan error, 1)
			go func() {
				// Bin 5 is not evicted (the eviction is below bin 1).
				ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 5).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
				done <- r.Put(context.Background(), ch)
			}()
			select {
			case err := <-done:
				return err
			case <-time.After(5 * time.Second):
				t.Error("Put into the batch blocked inside Pay: the batch lock is held")
				return errors.New("lock held")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if pays != 3 {
		t.Fatalf("Pay called %d times, want 3 (6 chunks, rounds of 2)", pays)
	}
}

// TestEvictRoundsStampIndexReplacement checks that a Put between rounds
// that replaces a not yet evicted item at the same stamp index leaves the
// reserve consistent: the new chunk stays, the replaced one is gone, and
// the reserve size matches the stored items.
func TestEvictRoundsStampIndexReplacement(t *testing.T) {
	defer reserve.SetEvictionRound(2)()

	baseAddr := swarm.RandAddress(t)
	st := &indexStorage{idx: inmemstore.New(), cs: inmemchunkstore.New()}
	r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	old := make([]swarm.Chunk, 0, 6)
	for i := range 6 {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewFields(batch.ID, uint64(i), 1))
		if err := r.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
		old = append(old, ch)
	}

	var replaced, replacement swarm.Chunk
	evicted, err := r.EvictBatchBin(context.Background(), batch.ID, math.MaxInt, 1, reserve.EvictionHooks{
		Pay: func(int) error {
			if replaced != nil {
				return nil
			}
			for i, ch := range old {
				stampHash, err := ch.Stamp().Hash()
				if err != nil {
					return err
				}
				has, err := r.Has(ch.Address(), batch.ID, stampHash)
				if err != nil {
					return err
				}
				if !has {
					continue
				}
				// Same stamp index, newer timestamp, in a bin the eviction
				// does not reach.
				replaced = ch
				replacement = chunk.GenerateTestRandomChunkAt(t, baseAddr, 4).WithStamp(postagetesting.MustNewFields(batch.ID, uint64(i), 2))
				return r.Put(context.Background(), replacement)
			}
			return errors.New("no item left to replace")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replaced == nil {
		t.Fatal("no replacement happened")
	}
	if evicted != 5 {
		t.Fatalf("evicted %d, want 5 (6 minus the one replaced)", evicted)
	}

	stampHash, err := replacement.Stamp().Hash()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(context.Background(), replacement.Address(), batch.ID, stampHash); err != nil {
		t.Fatalf("replacement chunk: %v, want it kept", err)
	}
	if has, _ := st.ChunkStore().Has(context.Background(), replaced.Address()); has {
		t.Fatal("replaced chunk still in the chunk store")
	}
	if got, want := r.Size(), countRadiusItems(t, st); got != want || got != 1 {
		t.Fatalf("reserve size %d, stored items %d, want 1 and 1", got, want)
	}
}

// TestEvictRoundsPayAbort checks that an error from Pay ends the eviction
// before the next round and leaves the rest of the batch in place.
func TestEvictRoundsPayAbort(t *testing.T) {
	defer reserve.SetEvictionRound(2)()

	baseAddr := swarm.RandAddress(t)
	st := &indexStorage{idx: inmemstore.New(), cs: inmemchunkstore.New()}
	r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	for range 6 {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		if err := r.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}

	errStop := errors.New("stop")
	evicted, err := r.EvictBatchBin(context.Background(), batch.ID, math.MaxInt, 1, reserve.EvictionHooks{
		Pay: func(int) error { return errStop },
	})
	if !errors.Is(err, errStop) {
		t.Fatalf("got %v, want %v", err, errStop)
	}
	if evicted != 2 {
		t.Fatalf("evicted %d, want 2 (one round)", evicted)
	}
	if r.Size() != 4 || countRadiusItems(t, st) != 4 {
		t.Fatalf("size %d, items %d, want 4 and 4", r.Size(), countRadiusItems(t, st))
	}
}

// TestEvictionKeepsServing checks that a node keeps accepting and serving
// chunks while it evicts in rounds, unpaced and paced: new chunks are
// stored into the batch being evicted and into other batches, the batch's
// chunks that are not evicted are served while the eviction runs, and a
// retrieval waits at most about one round, not the whole eviction.
func TestEvictionKeepsServing(t *testing.T) {
	defer reserve.SetEvictionRound(20)()

	for _, tc := range []struct {
		name   string
		pause  time.Duration // what Pay waits per round; 0 is rate 0
		doomed int
	}{
		// At rate 0 the in-memory eviction is fast, so it gets more chunks
		// to run long enough to serve during it. Pay is nil, as in the
		// node at rate 0 apart from its stop check; the yield between
		// rounds is EvictBatchBin's own.
		{"rate 0", 0, 8000},
		{"paced", 20 * time.Millisecond, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseAddr := swarm.RandAddress(t)
			st := &indexStorage{idx: inmemstore.New(), cs: inmemchunkstore.New()}
			r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
			if err != nil {
				t.Fatal(err)
			}
			target, other := postagetesting.MustNewBatch(), postagetesting.MustNewBatch()
			put := func(id []byte, bin int) swarm.Chunk {
				t.Helper()
				ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, bin).WithStamp(postagetesting.MustNewBatchStamp(id))
				if err := r.Put(context.Background(), ch); err != nil {
					t.Fatal(err)
				}
				return ch
			}
			get := func(ch swarm.Chunk) error {
				stampHash, err := ch.Stamp().Hash()
				if err != nil {
					return err
				}
				_, err = r.Get(context.Background(), ch.Address(), ch.Stamp().BatchID(), stampHash)
				return err
			}

			const evictBelow = 1
			doomed := make([]swarm.Chunk, 0, tc.doomed)
			kept := make([]swarm.Chunk, 0, 40)
			for range tc.doomed {
				doomed = append(doomed, put(target.ID, 0))
			}
			for range 20 {
				kept = append(kept, put(target.ID, 5), put(other.ID, 0))
			}

			var maxHold time.Duration
			hooks := reserve.EvictionHooks{
				After: func(_ int, took time.Duration) { maxHold = max(maxHold, took) },
			}
			if tc.pause > 0 {
				hooks.Pay = func(int) error { time.Sleep(tc.pause); return nil }
			}
			done := make(chan error, 1)
			start := time.Now()
			go func() {
				_, err := r.EvictBatchBin(context.Background(), target.ID, math.MaxInt, evictBelow, hooks)
				done <- err
			}()

			var (
				added   []swarm.Chunk
				maxGet  time.Duration
				gets    int
				evicted bool
			)
			// maxGet is the longest wait of a Put plus a Get of the batch.
			for !evicted {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
					evicted = true
					continue
				default:
				}
				// New chunks into the batch being evicted, at and below the
				// bin it evicts, and into another batch.
				t0 := time.Now()
				added = append(added, put(target.ID, 6), put(other.ID, 0))
				ch := kept[gets%len(kept)]
				if err := get(ch); err != nil {
					t.Fatalf("serving a kept chunk during the eviction: %v", err)
				}
				maxGet = max(maxGet, time.Since(t0))
				gets++
			}
			total := time.Since(start)

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
			if got, want := r.Size(), countRadiusItems(t, st); got != want {
				t.Fatalf("reserve size %d, stored items %d", got, want)
			}

			// A retrieval and a store wait about one round, not the whole
			// eviction. Without the yield between rounds, a Get waited
			// nearly the whole eviction at rate 0 (155 to 231 ms of 233 to
			// 239 ms under -race); with it the longest wait is usually under
			// 2 ms, with rare outliers of tens of ms under -race. Half the
			// eviction separates the two without flaking.
			t.Logf("gets %d, max get %v, max round hold %v, eviction %v", gets, maxGet, maxHold, total)
			if gets < 20 {
				t.Fatalf("only %d retrievals during an eviction of %v", gets, total)
			}
			if maxGet > total/2 {
				t.Fatalf("a retrieval waited %v of an eviction of %v: the batch lock was held across rounds", maxGet, total)
			}
		})
	}
}

// TestArrivals checks that Arrivals counts only Puts that grew the reserve.
func TestArrivals(t *testing.T) {
	baseAddr := swarm.RandAddress(t)
	st := &indexStorage{idx: inmemstore.New(), cs: inmemchunkstore.New()}
	r, err := reserve.New(baseAddr, st, 0, kademlia.NewTopologyDriver(), log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewFields(batch.ID, 1, 1))
	if err := r.Put(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	// The same chunk again: not an arrival.
	if err := r.Put(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	// A replacement at the same stamp index: the size does not grow.
	repl := chunk.GenerateTestRandomChunkAt(t, baseAddr, 0).WithStamp(postagetesting.MustNewFields(batch.ID, 1, 2))
	if err := r.Put(context.Background(), repl); err != nil {
		t.Fatal(err)
	}
	if got := r.Arrivals(); got != 1 {
		t.Fatalf("arrivals %d, want 1", got)
	}
}
