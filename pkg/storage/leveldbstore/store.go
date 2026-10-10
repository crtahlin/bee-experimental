// Copyright 2022 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldbstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/syndtr/goleveldb/leveldb"
	ldbErrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	ldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	separator = "/"
	// legacyDirtyKey is the marker as it used to be written: a key inside the
	// database itself. Deleting it on close made shutdown depend on the store
	// being writable at exactly the moment it is least likely to be, so the
	// marker moved out of LevelDB. The key is still read on open, because
	// stores written by earlier builds carry it, and its presence there means
	// what it always meant. See issue #115.
	legacyDirtyKey = ".store-dirty-shutdown"
	// dirtyMarkerName is the marker file, inside the store directory.
	//
	// Inside rather than beside, so the marker is removed along with the store
	// when someone deletes it. A sibling would outlive the directory and make a
	// freshly created store report an unclean shutdown it never had.
	//
	// goleveldb leaves it alone: its file storage only removes files it
	// recognises — its own numbered files and pending CURRENT renames — and
	// ignores anything else in the directory.
	dirtyMarkerName = ".dirty-shutdown"
)

// dirtyMarkerPath returns the marker file for a store directory, or "" for an
// in-memory store, which has no directory and cannot outlive the process it
// runs in.
func dirtyMarkerPath(path string) string {
	if path == "" {
		return ""
	}
	return filepath.Join(path, dirtyMarkerName)
}

// key returns the Item identifier for the leveldb storage.
func key(item storage.Key) []byte {
	return []byte(item.Namespace() + separator + item.ID())
}

// filters is a decorator for a slice of storage.Filters
// that helps with its evaluation.
type filters []storage.Filter

// matchAny returns true if any of the filters match the item.
func (f filters) matchAny(k string, v []byte) bool {
	for _, filter := range f {
		if filter(k, v) {
			return true
		}
	}
	return false
}

// Storer returns the underlying db store.
type Storer interface {
	DB() *leveldb.DB
}

var (
	_ Storer        = (*Store)(nil)
	_ storage.Store = (*Store)(nil)
)

type Store struct {
	db   *leveldb.DB
	path string
	// readOnly suppresses the dirty-shutdown marker on both open and close.
	// Writing it fails outright in read-only mode, and a reader has no unclean
	// shutdown to record.
	readOnly bool
	// marker is the file whose presence means "a writer has this store open".
	// Empty for an in-memory store and for a read-only open.
	marker string
}

// New returns a new store the backed by leveldb.
// If path == "", the leveldb will run with in memory backend storage.
// The returned bool indicates whether the previous shutdown was unclean (dirty).
func New(path string, opts *opt.Options) (*Store, bool, error) {
	var (
		err error
		db  *leveldb.DB
	)

	if path == "" {
		db, err = leveldb.Open(ldbStorage.NewMemStorage(), opts)
	} else {
		db, err = leveldb.OpenFile(path, opts)
		if ldbErrors.IsCorrupted(err) && !opts.GetReadOnly() {
			db, err = leveldb.RecoverFile(path, opts)
		}
	}

	if err != nil {
		return nil, false, err
	}

	// Read both markers. A store written by an earlier build carries the key
	// inside the database; one written by this build carries the sibling file.
	// Either means the last writer did not close cleanly.
	legacyDirty, err := db.Has([]byte(legacyDirtyKey), nil)
	if err != nil {
		return nil, false, fmt.Errorf("has dirty record: %w", err)
	}

	marker := dirtyMarkerPath(path)
	fileDirty := false
	if marker != "" {
		if _, err := os.Stat(marker); err == nil {
			fileDirty = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, false, fmt.Errorf("stat dirty marker: %w", err)
		}
	}
	dirty := legacyDirty || fileDirty

	// A read-only open cannot claim the database, and must not try: the write
	// fails with "leveldb: read-only mode" and takes the whole open down, so
	// read-only mode was unusable. It is also meaningless — a reader neither
	// risks an unclean shutdown nor is entitled to mark the database as in use
	// by someone else.
	//
	// The dirty flag read above is still returned, so a read-only caller can
	// still see that a previous *writer* exited uncleanly.
	if opts.GetReadOnly() {
		return &Store{db: db, path: path, readOnly: true}, dirty, nil
	}

	// Clear the legacy key now rather than on close. Open is when writes are
	// known to work; close is exactly when they may not be, which is the whole
	// reason the marker moved. Best effort: failing to tidy up an old key is
	// not a reason to refuse to open the store, and the file marker written
	// below is what this build will read next time.
	if legacyDirty {
		_ = db.Delete([]byte(legacyDirtyKey), nil)
	}

	if marker != "" {
		if err := writeDirtyMarker(marker); err != nil {
			return nil, false, fmt.Errorf("write dirty marker: %w", err)
		}
	}

	return &Store{
		db:     db,
		path:   path,
		marker: marker,
	}, dirty, nil
}

// writeDirtyMarker creates the marker and makes it durable.
//
// The file itself is fsynced, and so is the directory holding it where the
// platform allows it, because a file that exists only in the page cache does
// not survive the power loss it exists to record. An unclean shutdown is
// precisely the case where an unsynced marker would be lost, which would report
// the store as clean when it is not — the failure direction that costs data
// rather than time.
func writeDirtyMarker(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Not portable, and deliberately not silently skipped: see syncDir, which
	// is a no-op only on Windows, where there is no equivalent operation.
	return syncDir(filepath.Dir(path))
}

// DB implements the Storer interface.
func (s *Store) DB() *leveldb.DB {
	return s.db
}

// Close implements the storage.Store interface.
//
// It performs no database write. Deleting a key here made shutdown depend on
// the store being writable, so a store whose writes were paused at the level-0
// trigger could not be closed at all: the node hung, was killed, and started
// again into recovery over an unclean marker it had never had the chance to
// clear. See issue #115.
//
// The marker is removed only after the database closes without error. A close
// that fails leaves the store marked dirty, which is the safe direction: a
// needless recovery pass costs time, and a skipped one costs data.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return err
	}
	if s.marker == "" {
		// Read-only or in-memory: nothing was ever claimed.
		return nil
	}
	if err := os.Remove(s.marker); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove dirty marker: %w", err)
	}
	return nil
}

// Get implements the storage.Store interface.
func (s *Store) Get(item storage.Item) error {
	val, err := s.db.Get(key(item), nil)

	if errors.Is(err, leveldb.ErrNotFound) {
		return storage.ErrNotFound
	}

	if err != nil {
		return err
	}

	if err = item.Unmarshal(val); err != nil {
		return fmt.Errorf("failed decoding value %w", err)
	}

	return nil
}

// Has implements the storage.Store interface.
func (s *Store) Has(k storage.Key) (bool, error) {
	return s.db.Has(key(k), nil)
}

// GetSize implements the storage.Store interface.
func (s *Store) GetSize(k storage.Key) (int, error) {
	val, err := s.db.Get(key(k), nil)

	if errors.Is(err, leveldb.ErrNotFound) {
		return 0, storage.ErrNotFound
	}

	if err != nil {
		return 0, err
	}

	return len(val), nil
}

// Iterate implements the storage.Store interface.
func (s *Store) Iterate(q storage.Query, fn storage.IterateFn) error {
	if err := q.Validate(); err != nil {
		return fmt.Errorf("failed iteration: %w", err)
	}

	var retErr error

	var iter iterator.Iterator
	var prefix string

	defer func() {
		if iter != nil {
			iter.Release()
		}
	}()

	iterOpts := &opt.ReadOptions{
		DontFillCache: true,
	}

	if q.PrefixAtStart {
		prefix = q.Factory().Namespace()
		iter = s.db.NewIterator(util.BytesPrefix([]byte(prefix)), iterOpts)
		exists := iter.Seek([]byte(prefix + separator + q.Prefix))
		if !exists {
			return nil
		}
		_ = iter.Prev()
	} else {
		// this is a small hack to make the iteration work with the
		// old implementation of statestore. this allows us to do a
		// full iteration without looking at the prefix.
		if q.Factory().Namespace() != "" {
			prefix = q.Factory().Namespace() + separator + q.Prefix
		}
		iter = s.db.NewIterator(util.BytesPrefix([]byte(prefix)), iterOpts)
	}

	nextF := iter.Next

	if q.Order == storage.KeyDescendingOrder {
		nextF = func() bool {
			nextF = iter.Prev
			return iter.Last()
		}
	}

	firstSkipped := !q.SkipFirst

	for nextF() {
		keyRaw := iter.Key()
		nextKey := make([]byte, len(keyRaw))
		copy(nextKey, keyRaw)

		valRaw := iter.Value()
		nextVal := make([]byte, len(valRaw))
		copy(nextVal, valRaw)

		key := strings.TrimPrefix(string(nextKey), prefix)

		if filters(q.Filters).matchAny(key, nextVal) {
			continue
		}

		if q.SkipFirst && !firstSkipped {
			firstSkipped = true
			continue
		}

		var (
			res *storage.Result
			err error
		)

		switch q.ItemProperty {
		case storage.QueryItemID, storage.QueryItemSize:
			res = &storage.Result{ID: key, Size: len(nextVal)}
		case storage.QueryItem:
			newItem := q.Factory()
			err = newItem.Unmarshal(nextVal)
			res = &storage.Result{ID: key, Entry: newItem}
		}

		if err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed unmarshaling: %w", err))
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

	if err := iter.Error(); err != nil {
		retErr = errors.Join(retErr, err)
	}

	return retErr
}

// Count implements the storage.Store interface.
func (s *Store) Count(key storage.Key) (int, error) {
	keys := util.BytesPrefix([]byte(key.Namespace() + separator))
	iter := s.db.NewIterator(keys, nil)

	var c int
	for iter.Next() {
		c++
	}

	iter.Release()

	return c, iter.Error()
}

// countCheckEvery is how many keys a context-aware count steps over between
// checks of its context.
const countCheckEvery = 4096

// CountContext is Count that returns the context's error once the context
// ends, checked every countCheckEvery keys (wasp #635).
func (s *Store) CountContext(ctx context.Context, key storage.Key) (int, error) {
	keys := util.BytesPrefix([]byte(key.Namespace() + separator))
	iter := s.db.NewIterator(keys, nil)
	defer iter.Release()

	var c int
	for iter.Next() {
		c++
		if c%countCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
	}
	return c, iter.Error()
}

// Put implements the storage.Store interface.
func (s *Store) Put(item storage.Item) error {
	value, err := item.Marshal()
	if err != nil {
		return fmt.Errorf("failed serializing: %w", err)
	}

	return s.db.Put(key(item), value, nil)
}

// Delete implements the storage.Store interface.
func (s *Store) Delete(item storage.Item) error {
	// this is a small hack to make the deletion of old entries work. As they
	// don't have a namespace, we need to check for that and use the ID as key without
	// the separator.
	var k []byte
	if item.Namespace() == "" {
		k = []byte(item.ID())
	} else {
		k = key(item)
	}

	return s.db.Delete(k, nil)
}
