// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cache_test

import (
	"errors"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/cache"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemstore"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
	"github.com/ethersphere/bee/v2/pkg/storage/storagetest"
)

func TestCache(t *testing.T) {
	t.Parallel()

	store, _, err := leveldbstore.New(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}

	cache, err := cache.Wrap(store, 100_000)
	if err != nil {
		t.Fatalf("create cache failed: %v", err)
	}

	storagetest.TestStore(t, cache)
}

// valueItem is a storage.Item whose key does not depend on its value, so
// two values can be written under the same key.
type valueItem struct {
	id    string
	value []byte
}

func (v *valueItem) ID() string                 { return v.id }
func (v *valueItem) Namespace() string          { return "cache-test" }
func (v *valueItem) Marshal() ([]byte, error)   { return append([]byte(nil), v.value...), nil }
func (v *valueItem) Unmarshal(buf []byte) error { v.value = append([]byte(nil), buf...); return nil }
func (v *valueItem) Clone() storage.Item {
	return &valueItem{id: v.id, value: append([]byte(nil), v.value...)}
}
func (v valueItem) String() string { return v.id }

var errWrite = errors.New("write failed")

// failingStore fails Put and PutSync while fail is set.
type failingStore struct {
	storage.IndexStore
	fail bool
}

func (f *failingStore) Put(i storage.Item) error {
	if f.fail {
		return errWrite
	}
	return f.IndexStore.Put(i)
}

func (f *failingStore) PutSync(i storage.Item) error {
	if f.fail {
		return errWrite
	}
	return f.IndexStore.PutSync(i)
}

// TestCacheFailedWriteKeepsStoredValue checks that a failed write does not
// leave the unsaved value in the cache: a following Get through the cache
// returns the value that is in the store (wasp #732).
func TestCacheFailedWriteKeepsStoredValue(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		write func(*cache.Cache, storage.Item) error
	}{
		{"Put", (*cache.Cache).Put},
		{"PutSync", (*cache.Cache).PutSync},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := &failingStore{IndexStore: inmemstore.New()}
			c, err := cache.Wrap(store, 100)
			if err != nil {
				t.Fatalf("create cache failed: %v", err)
			}

			if err := tc.write(c, &valueItem{id: "k", value: []byte("old")}); err != nil {
				t.Fatalf("first write: %v", err)
			}

			store.fail = true
			if err := tc.write(c, &valueItem{id: "k", value: []byte("new")}); !errors.Is(err, errWrite) {
				t.Fatalf("second write: got %v, want %v", err, errWrite)
			}

			got := &valueItem{id: "k"}
			if err := c.Get(got); err != nil {
				t.Fatalf("get: %v", err)
			}
			if string(got.value) != "old" {
				t.Fatalf("get after failed write: got %q, want %q", got.value, "old")
			}
		})
	}
}
