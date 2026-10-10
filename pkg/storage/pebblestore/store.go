// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pebblestore implements storage.Store over cockroachdb/pebble.
//
// It replaces goleveldb, which has had no commit since July 2022 and carries an
// unfixed race in compTriggerWait. The real-reserve A/B in
// docs/experiments/storage-engine-eval/results.md found Pebble better on every
// axis a running node cares about once level 0 is kept shallow, so it is now the
// default index engine for a fresh data directory. goleveldb stays selectable
// with --storage-engine leveldb, and an existing goleveldb store keeps goleveldb.
// See issues #15 and #185.
package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethersphere/bee/v2/pkg/storage"
)

const separator = "/"

// key returns the identifier under which an item is stored.
//
// Identical to the leveldbstore rule, deliberately: the point of the comparison
// is that both engines see the same keys in the same order.
func key(item storage.Key) []byte {
	return []byte(item.Namespace() + separator + item.ID())
}

// filters decorates a slice of storage.Filter to help with evaluation.
type filters []storage.Filter

func (f filters) matchAny(k string, v []byte) bool {
	for _, filter := range f {
		if filter(k, v) {
			return true
		}
	}
	return false
}

var (
	_ storage.Store      = (*Store)(nil)
	_ storage.BatchStore = (*Store)(nil)
)

type Store struct {
	db   *pebble.DB
	path string

	// closeOnce makes Close idempotent.
	//
	// pebble.DB.Close panics with "pebble: closed" when called twice;
	// goleveldb returns an error. The shared conformance suite closes the store
	// itself, and any caller with a deferred Close alongside an explicit one
	// hits this, so the difference is not academic: it turns an ordinary
	// double-close into a crash. Guarded here rather than left for every caller
	// to remember. See docs/experiments/pebble-evaluation/results.md.
	closeOnce sync.Once
	closeErr  error
}

// DefaultOptions returns options with a bloom filter configured.
//
// Pebble sets filters per level and applies none unless asked, so a zero
// pebble.Options has no bloom filter anywhere. goleveldb takes one filter
// setting for the whole database.
//
// This is here on reasoning, not on a measurement, and the distinction is worth
// keeping straight because the first version of this comment claimed the
// opposite. Missing-key lookups measured 7.6x slower than goleveldb, the filter
// was added expecting to close the gap, and it changed nothing: 1,531 ns with
// it against 1,539 ns without, over the same benchmark selection.
//
// The reason it changes nothing is that the benchmark never produces a level
// for it to help with. One million entries leave 98 files in level 0 and none
// below it, and level-0 files overlap, so a point lookup consults many of them
// whatever their filters say. A node with a settled reserve has data below
// level 0, where the filter does pay — but that is an argument, and this
// harness cannot test it. See docs/experiments/pebble-evaluation/results.md.
func DefaultOptions() *pebble.Options {
	opts := &pebble.Options{}
	opts.EnsureDefaults()
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	// Keep level 0 shallow. Under a full reserve, Pebble left at a higher trigger
	// lets level 0 pile up and the reserve-sample read merges across it, which
	// measured 245s against goleveldb's 52s; at this trigger the same sample runs
	// in about 44s, faster than goleveldb. See
	// docs/experiments/storage-engine-eval/results.md.
	opts.L0CompactionThreshold = 4
	return opts
}

// New opens a store at path, or in memory when path is empty.
//
// Passing nil options gets DefaultOptions, not pebble's zero value, so that a
// caller who expresses no preference is not silently handed a store with no
// bloom filter.
func New(path string, opts *pebble.Options) (*Store, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	if path == "" {
		// Pebble has no in-memory backend as such; it has an in-memory
		// filesystem, which is the same thing from the caller's side and is
		// what the leveldbstore equivalent is being compared against.
		opts.FS = vfs.NewMem()
		path = "mem"
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

// DB exposes the underlying database.
func (s *Store) DB() *pebble.DB { return s.db }

func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.db.Close() })
	return s.closeErr
}

func (s *Store) Get(item storage.Item) error {
	val, closer, err := s.db.Get(key(item))
	if errors.Is(err, pebble.ErrNotFound) {
		return storage.ErrNotFound
	}
	if err != nil {
		return err
	}
	// The value is only valid until closer.Close, so unmarshal before closing
	// rather than retaining the slice. Pebble is explicit about this and
	// goleveldb is not, which is the kind of difference that turns into a use
	// after free rather than a compile error.
	uerr := item.Unmarshal(val)
	if cerr := closer.Close(); cerr != nil {
		return cerr
	}
	if uerr != nil {
		return fmt.Errorf("failed decoding value %w", uerr)
	}
	return nil
}

func (s *Store) Has(k storage.Key) (bool, error) {
	_, closer, err := s.db.Get(key(k))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

func (s *Store) GetSize(k storage.Key) (int, error) {
	val, closer, err := s.db.Get(key(k))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, storage.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	n := len(val)
	return n, closer.Close()
}

func (s *Store) Put(item storage.Item) error {
	value, err := item.Marshal()
	if err != nil {
		return fmt.Errorf("failed serializing: %w", err)
	}
	return s.db.Set(key(item), value, pebble.NoSync)
}

// PutSync implements the storage.SyncWriter interface: the write is synced
// to disk before it returns.
func (s *Store) PutSync(item storage.Item) error {
	value, err := item.Marshal()
	if err != nil {
		return fmt.Errorf("failed serializing: %w", err)
	}
	return s.db.Set(key(item), value, pebble.Sync)
}

func (s *Store) Delete(item storage.Item) error {
	// Entries written before namespaces existed have no namespace, and their
	// key is the bare ID with no separator. Carried over from leveldbstore
	// rather than tidied, because the two must address the same bytes for the
	// comparison to mean anything.
	var k []byte
	if item.Namespace() == "" {
		k = []byte(item.ID())
	} else {
		k = key(item)
	}
	return s.db.Delete(k, pebble.NoSync)
}

func (s *Store) Count(k storage.Key) (int, error) {
	prefix := []byte(k.Namespace() + separator)
	iter, err := s.db.NewIter(prefixBounds(prefix))
	if err != nil {
		return 0, err
	}

	var c int
	for iter.First(); iter.Valid(); iter.Next() {
		c++
	}
	return c, errors.Join(iter.Error(), iter.Close())
}

// countCheckEvery is how many keys a context-aware count steps over between
// checks of its context.
const countCheckEvery = 4096

// CountContext is Count that returns the context's error once the context
// ends, checked every countCheckEvery keys (wasp #635).
func (s *Store) CountContext(ctx context.Context, k storage.Key) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	prefix := []byte(k.Namespace() + separator)
	iter, err := s.db.NewIter(prefixBounds(prefix))
	if err != nil {
		return 0, err
	}

	var c int
	for iter.First(); iter.Valid(); iter.Next() {
		c++
		if c%countCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return 0, errors.Join(err, iter.Close())
			}
		}
	}
	return c, errors.Join(iter.Error(), iter.Close())
}

// prefixBounds returns iterator bounds covering every key with the prefix.
//
// Pebble has no prefix iterator, only bounds, so the upper bound is the prefix
// with its last byte incremented — the same construction goleveldb's
// util.BytesPrefix performs internally.
func prefixBounds(prefix []byte) *pebble.IterOptions {
	opts := &pebble.IterOptions{LowerBound: prefix}
	limit := make([]byte, len(prefix))
	copy(limit, prefix)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] < 0xff {
			limit[i]++
			opts.UpperBound = limit[:i+1]
			return opts
		}
	}
	// Every byte was 0xff, so there is no upper bound: the prefix is the last
	// key range in the space. Leaving UpperBound nil is correct, not a gap.
	return opts
}

func (s *Store) Iterate(q storage.Query, fn storage.IterateFn) error {
	if err := q.Validate(); err != nil {
		return fmt.Errorf("failed iteration: %w", err)
	}

	var prefix string
	if q.PrefixAtStart {
		prefix = q.Factory().Namespace()
	} else if q.Factory().Namespace() != "" {
		// Matches leveldbstore, including its allowance for the old statestore,
		// which iterates with no namespace at all.
		prefix = q.Factory().Namespace() + separator + q.Prefix
	}

	iter, err := s.db.NewIter(prefixBounds([]byte(prefix)))
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	var retErr error

	// Position the cursor. PrefixAtStart means "start at this key and include
	// what precedes it in the namespace", which is why it seeks and then steps
	// back one.
	var next func() bool
	switch {
	case q.Order == storage.KeyDescendingOrder:
		first := true
		next = func() bool {
			if first {
				first = false
				return iter.Last()
			}
			return iter.Prev()
		}
	case q.PrefixAtStart:
		// Start at the first key at or after the sought one, inclusive.
		//
		// leveldbstore seeks and then steps back with iter.Prev(). Porting that
		// literally yields the element *before* the target, because the Prev
		// there only compensates for its loop calling Next() before the first
		// read. The behaviour to reproduce is the net effect, not the
		// keystrokes.
		seek := []byte(prefix + separator + q.Prefix)
		first := true
		next = func() bool {
			if first {
				first = false
				return iter.SeekGE(seek)
			}
			return iter.Next()
		}
	default:
		first := true
		next = func() bool {
			if first {
				first = false
				return iter.First()
			}
			return iter.Next()
		}
	}

	firstSkipped := !q.SkipFirst

	for next() {
		nextKey := bytes.Clone(iter.Key())
		nextVal := bytes.Clone(iter.Value())

		k := strings.TrimPrefix(string(nextKey), prefix)

		if filters(q.Filters).matchAny(k, nextVal) {
			continue
		}

		if q.SkipFirst && !firstSkipped {
			firstSkipped = true
			continue
		}

		var (
			res  *storage.Result
			uerr error
		)

		switch q.ItemProperty {
		case storage.QueryItemID, storage.QueryItemSize:
			res = &storage.Result{ID: k, Size: len(nextVal)}
		case storage.QueryItem:
			newItem := q.Factory()
			uerr = newItem.Unmarshal(nextVal)
			res = &storage.Result{ID: k, Entry: newItem}
		}

		if uerr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed unmarshaling: %w", uerr))
			break
		}
		if res == nil {
			retErr = errors.Join(retErr, fmt.Errorf("unknown object attribute type: %v", q.ItemProperty))
			break
		}

		if stop, err := fn(*res); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("iterate callback function errored: %w", err))
			break
		} else if stop {
			break
		}
	}

	return errors.Join(retErr, iter.Error())
}
