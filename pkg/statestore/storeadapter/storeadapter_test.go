// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storeadapter_test

import (
	"testing"

	"github.com/ethersphere/bee/v2/pkg/statestore/storeadapter"
	"github.com/ethersphere/bee/v2/pkg/statestore/test"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/cache"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemstore"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
)

func TestStateStoreAdapter(t *testing.T) {
	t.Parallel()

	test.Run(t, func(t *testing.T) storage.StateStorer {
		t.Helper()

		store, err := storeadapter.NewStateStorerAdapter(inmemstore.New())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})

		return store
	})

	test.RunPersist(t, func(t *testing.T, dir string) storage.StateStorer {
		t.Helper()

		leveldb, _, err := leveldbstore.New(dir, nil)
		if err != nil {
			t.Fatal(err)
		}

		store, err := storeadapter.NewStateStorerAdapter(leveldb)
		if err != nil {
			t.Fatal(err)
		}

		return store
	})
}

// TestStateStoreAdapterPutSync writes through the node's state-store path
// (leveldb wrapped in the cache, wrapped in the adapter) with PutSync and
// reads the value back after a reopen (wasp #725).
func TestStateStoreAdapterPutSync(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	open := func() storage.StateStorer {
		t.Helper()
		ldb, _, err := leveldbstore.New(dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		caching, err := cache.Wrap(ldb, 100)
		if err != nil {
			t.Fatal(err)
		}
		store, err := storeadapter.NewStateStorerAdapter(caching)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}

	store := open()
	if err := store.PutSync("key", "value"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = open()
	defer store.Close()
	var got string
	if err := store.Get("key", &got); err != nil {
		t.Fatal(err)
	}
	if got != "value" {
		t.Fatalf("got %q, want %q", got, "value")
	}
}
