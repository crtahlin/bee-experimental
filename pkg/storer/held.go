// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/soc"
	storage "github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/storageutil"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"resenje.org/multex"
)

// Held chunks (#583): while the batch store is stale, a chunk whose data is
// genuine but whose stamp names a batch the node has not seen yet, or an
// index beyond the depth it knows, is kept instead of dropped. It is written
// to the chunk store, so retrieval serves it, with its stamp in a separate
// held index that cache eviction does not touch. Once the listener has caught
// up, a validation worker puts each valid chunk inside the radius into the
// reserve and drops the rest.
//
// The held index is a new namespace that starts empty, like the local ingest
// records, so it needs no migration. A node downgraded while it holds chunks
// keeps their chunk-store references for good, since an older version ignores
// the namespace; operators downgrade only once the held gauge reads 0.

// Variables only so tests can shorten them.
var (
	// heldChunksMax bounds the held area by distinct chunk addresses: about
	// 410 MB at 100,000. A fake batch ID costs the sender nothing while the
	// node is stale, so the bound is what limits spam.
	heldChunksMax = uint64(100_000)
	// heldCheckInterval is how often the validation worker looks for held
	// chunks to validate.
	heldCheckInterval = 10 * time.Second
	// heldRound and heldRoundPause pace validation: promoting means a
	// reserve put per chunk, right after catch-up, when expired batches are
	// also evicted.
	heldRound      = 1000
	heldRoundPause = 100 * time.Millisecond
)

// heldState is the held area's accounting. The count of distinct addresses
// lives in memory and is rebuilt at startup from the index.
type heldState struct {
	// addrLock serialises the "already held?" check, the claim and the
	// release for one address, so two handlers holding the same address at
	// once claim room once.
	addrLock *multex.Multex

	// The limits, copied from the variables above when the store opens,
	// so a test that changes them does not race a running store.
	max        uint64
	check      time.Duration
	round      int
	roundPause time.Duration

	mu         sync.Mutex
	addrs      uint64
	warnedFull bool
}

func newHeldState() *heldState {
	return &heldState{
		addrLock:   multex.New(),
		max:        heldChunksMax,
		check:      heldCheckInterval,
		round:      heldRound,
		roundPause: heldRoundPause,
	}
}

// claim takes room for one more address, or reports the area full.
func (h *heldState) claim() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.addrs >= h.max {
		return false
	}
	h.addrs++
	return true
}

func (h *heldState) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.addrs > 0 {
		h.addrs--
	}
	h.warnedFull = false
}

func (h *heldState) count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.addrs
}

// warnFull reports whether the full-area Warning is due: once until room is
// released again.
func (h *heldState) warnFull() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.warnedFull {
		return false
	}
	h.warnedFull = true
	return true
}

var _ postage.ChunkHolder = (*DB)(nil)

// HoldUnvalidated implements postage.ChunkHolder.
func (db *DB) HoldUnvalidated(ctx context.Context, ch swarm.Chunk, cause error) (bool, error) {
	if !postage.HoldsUnvalidated(cause) || !db.postageStale() {
		return false, nil
	}
	// Only a chunk whose data matches its full content address is held:
	// the chunk store never overwrites, so whatever is held under an
	// address is what the node serves for it.
	if !cac.Valid(ch) && !soc.Valid(ch) {
		return false, nil
	}
	stamp, ok := ch.Stamp().(*postage.Stamp)
	if !ok || stamp == nil {
		return false, nil
	}
	stampHash, err := stamp.Hash()
	if err != nil {
		return false, nil
	}
	stampBytes, err := stamp.MarshalBinary()
	if err != nil {
		return false, nil
	}

	addr := ch.Address()
	item := &heldItem{
		Addr:      addr.Clone(),
		BatchID:   append([]byte(nil), stamp.BatchID()...),
		StampHash: stampHash,
		Stamp:     stampBytes,
		Arrived:   time.Now().UnixNano(),
	}

	key := addr.ByteString()
	db.held.addrLock.Lock(key)
	defer db.held.addrLock.Unlock(key)

	has, err := db.storage.IndexStore().Has(item)
	if err != nil {
		return false, fmt.Errorf("held index: %w", err)
	}
	if has {
		// The same chunk with the same stamp, offered again before
		// validation: already held, no second reference.
		return true, nil
	}

	addrHeld, err := db.addressHeld(addr)
	if err != nil {
		return false, fmt.Errorf("held index: %w", err)
	}
	claimed := false
	if !addrHeld {
		if !db.held.claim() {
			if db.held.warnFull() {
				db.logger.Warning("held area for chunks of batches not seen yet is full; new ones are refused and pulling pauses until the batch store catches up",
					"held_addresses", db.held.count(), "max", db.held.max)
			}
			return false, postage.ErrHeldAreaFull
		}
		claimed = true
	}

	// The held entry and the chunk-store reference commit together, so a
	// crash cannot leave one without the other.
	err = db.storage.Run(ctx, func(s transaction.Store) error {
		if err := s.ChunkStore().Put(ctx, ch); err != nil {
			return err
		}
		return s.IndexStore().Put(item)
	})
	if err != nil {
		if claimed {
			db.held.release()
		}
		return false, fmt.Errorf("hold chunk: %w", err)
	}
	db.metrics.HeldChunks.Set(float64(db.held.count()))
	return true, nil
}

// HeldFull implements postage.ChunkHolder.
func (db *DB) HeldFull() bool {
	return db.held.count() >= db.held.max
}

// HeldPending reports whether held chunks are waiting for validation. The
// storage lottery does not play until they are processed (#583).
func (db *DB) HeldPending() bool {
	return db.held.count() > 0
}

func (db *DB) postageStale() bool {
	p := db.postageSyncHealth.Load()
	return p != nil && (*p).Stale()
}

func (db *DB) postageCaughtUp() bool {
	p := db.postageSyncHealth.Load()
	return p != nil && (*p).CaughtUp()
}

// addressHeld reports whether any held entry exists for addr.
func (db *DB) addressHeld(addr swarm.Address) (bool, error) {
	found := false
	err := db.storage.IndexStore().Iterate(storage.Query{
		Factory:       func() storage.Item { return new(heldItem) },
		Prefix:        addr.ByteString(),
		ItemProperty:  storage.QueryItemID,
		PrefixAtStart: false,
	}, func(storage.Result) (bool, error) {
		found = true
		return true, nil
	})
	return found, err
}

// rebuildHeldCount counts the distinct held addresses at startup.
func (db *DB) rebuildHeldCount() {
	var (
		n    uint64
		last string
	)
	err := db.storage.IndexStore().Iterate(storage.Query{
		Factory:      func() storage.Item { return new(heldItem) },
		ItemProperty: storage.QueryItemID,
	}, func(r storage.Result) (bool, error) {
		if len(r.ID) < swarm.HashSize {
			return false, nil
		}
		if a := r.ID[:swarm.HashSize]; a != last {
			n++
			last = a
		}
		return false, nil
	})
	if err != nil {
		db.logger.Error(err, "held chunks: counting held addresses at startup, the figure may be short")
	}
	db.held.mu.Lock()
	db.held.addrs = n
	db.held.mu.Unlock()
	db.metrics.HeldChunks.Set(float64(n))
}

// heldValidator validates held chunks once the listener is caught up, and at
// startup when the index is not empty, under the same condition: it runs
// only while the batch store is current, so a chunk of a batch created after
// the stored chain state is not dropped as still unknown. It is started with
// the reserve worker, after storer recovery.
func (db *DB) heldValidator(ctx context.Context) {
	defer db.inFlight.Done()

	t := time.NewTicker(db.held.check)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-db.quit:
			return
		case <-t.C:
		}
		if db.held.count() == 0 || !db.postageCaughtUp() {
			continue
		}
		start := time.Now()
		if err := db.validateHeld(ctx); err != nil {
			if errors.Is(err, ErrDBQuit) || errors.Is(err, context.Canceled) {
				return
			}
			db.logger.Warning("held chunks: validation pass", "error", err)
			continue
		}
		if db.held.count() == 0 {
			d := time.Since(start)
			db.metrics.HeldValidationSeconds.Set(d.Seconds())
			db.logger.Info("held chunks validated", "duration", d.Round(time.Millisecond))
		}
	}
}

// validateHeld processes the held index in rounds, with a pause between them,
// for as long as the batch store stays caught up.
func (db *DB) validateHeld(ctx context.Context) error {
	for {
		if !db.postageCaughtUp() {
			return nil
		}
		batch := make([]*heldItem, 0, db.held.round)
		err := db.storage.IndexStore().Iterate(storage.Query{
			Factory: func() storage.Item { return new(heldItem) },
		}, func(r storage.Result) (bool, error) {
			batch = append(batch, r.Entry.(*heldItem).Clone().(*heldItem))
			return len(batch) >= db.held.round, nil
		})
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, item := range batch {
			if err := db.validateHeldItem(ctx, item); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-db.quit:
			return ErrDBQuit
		case <-time.After(db.held.roundPause):
		}
	}
}

// validateHeldItem promotes a held chunk into the reserve when its stamp is
// valid and it lies inside the storage radius, and drops it otherwise.
// Promotion is idempotent: the reserve put comes first, then the held entry
// and its chunk-store reference go in one transaction, so a crash in between
// only repeats the promotion.
func (db *DB) validateHeldItem(ctx context.Context, item *heldItem) error {
	key := item.Addr.ByteString()
	db.held.addrLock.Lock(key)
	defer db.held.addrLock.Unlock(key)

	ch, err := db.storage.ChunkStore().Get(ctx, item.Addr)
	if errors.Is(err, storage.ErrNotFound) {
		// The chunk is gone from the chunk store: nothing to promote and no
		// reference to release.
		return db.dropHeld(ctx, item, false)
	}
	if err != nil {
		return err
	}

	stamp := new(postage.Stamp)
	promote := false
	if stamp.UnmarshalBinary(item.Stamp) == nil {
		valid, verr := db.validStamp(swarm.NewChunk(ch.Address(), ch.Data()).WithStamp(stamp))
		if verr == nil && db.IsWithinStorageRadius(item.Addr) {
			if perr := db.ReservePutter().Put(ctx, valid); perr == nil {
				promote = true
			} else if !errors.Is(perr, storage.ErrOverwriteNewerChunk) {
				return fmt.Errorf("promote held chunk: %w", perr)
			}
		}
	}
	if err := db.dropHeld(ctx, item, true); err != nil {
		return err
	}
	if promote {
		db.metrics.HeldPromoted.Inc()
	} else {
		db.metrics.HeldDropped.Inc()
	}
	return nil
}

// dropHeld deletes a held entry, and its chunk-store reference when it still
// has one, in one transaction, and releases the address's room when this was
// its last entry. Called with the address lock held.
func (db *DB) dropHeld(ctx context.Context, item *heldItem, hasRef bool) error {
	err := db.storage.Run(ctx, func(s transaction.Store) error {
		if err := s.IndexStore().Delete(item); err != nil {
			return err
		}
		if hasRef {
			return s.ChunkStore().Delete(ctx, item.Addr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("drop held chunk: %w", err)
	}
	still, err := db.addressHeld(item.Addr)
	if err != nil {
		return err
	}
	if !still {
		db.held.release()
	}
	db.metrics.HeldChunks.Set(float64(db.held.count()))
	return nil
}

// heldItemSize is the fixed part of a held entry: address, batch ID, stamp
// hash and arrival time. The marshalled stamp follows.
const heldItemSize = 3*swarm.HashSize + 8

var errInvalidHeldItem = errors.New("storer: invalid held item")

var _ storage.Item = (*heldItem)(nil)

// heldItem is one held chunk: the chunk itself lives in the chunk store,
// this entry holds its stamp until validation. Keyed by address, batch ID and
// stamp hash, so the same chunk with the same stamp is held once.
type heldItem struct {
	Addr      swarm.Address
	BatchID   []byte
	StampHash []byte
	Stamp     []byte
	Arrived   int64
}

func (i *heldItem) ID() string {
	return i.Addr.ByteString() + string(i.BatchID) + string(i.StampHash)
}

func (heldItem) Namespace() string { return "heldChunk" }

func (i *heldItem) Marshal() ([]byte, error) {
	if i.Addr.IsZero() || len(i.BatchID) != swarm.HashSize || len(i.StampHash) != swarm.HashSize || len(i.Stamp) == 0 {
		return nil, errInvalidHeldItem
	}
	buf := make([]byte, heldItemSize+len(i.Stamp))
	copy(buf, i.Addr.Bytes())
	copy(buf[swarm.HashSize:], i.BatchID)
	copy(buf[2*swarm.HashSize:], i.StampHash)
	binary.LittleEndian.PutUint64(buf[3*swarm.HashSize:], uint64(i.Arrived))
	copy(buf[heldItemSize:], i.Stamp)
	return buf, nil
}

func (i *heldItem) Unmarshal(buf []byte) error {
	if len(buf) <= heldItemSize {
		return errInvalidHeldItem
	}
	*i = heldItem{
		Addr:      swarm.NewAddress(append([]byte(nil), buf[:swarm.HashSize]...)),
		BatchID:   append([]byte(nil), buf[swarm.HashSize:2*swarm.HashSize]...),
		StampHash: append([]byte(nil), buf[2*swarm.HashSize:3*swarm.HashSize]...),
		Arrived:   int64(binary.LittleEndian.Uint64(buf[3*swarm.HashSize:])),
		Stamp:     append([]byte(nil), buf[heldItemSize:]...),
	}
	return nil
}

func (i *heldItem) Clone() storage.Item {
	if i == nil {
		return nil
	}
	return &heldItem{
		Addr:      i.Addr.Clone(),
		BatchID:   append([]byte(nil), i.BatchID...),
		StampHash: append([]byte(nil), i.StampHash...),
		Stamp:     append([]byte(nil), i.Stamp...),
		Arrived:   i.Arrived,
	}
}

func (i heldItem) String() string {
	return storageutil.JoinFields(i.Namespace(), i.ID())
}
