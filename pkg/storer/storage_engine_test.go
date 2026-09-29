// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/storage/pebblestore"
)

// TestResolveEngine covers the datadir guard and the marker resolution: an index
// directory is bound to the engine that created it, because the two on-disk
// formats are mutually unreadable and a flipped flag would otherwise corrupt.
// An empty request resolves to the directory's own engine, so a restart or a
// `bee db` command needs no flag. See issue #185.
func TestResolveEngine(t *testing.T) {
	t.Parallel()

	t.Run("fresh dir binds to the requested engine", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		got, err := resolveEngine(dir, EnginePebble)
		if err != nil || got != EnginePebble {
			t.Fatalf("fresh pebble: got %q, %v; want pebble, nil", got, err)
		}
		marker, _ := os.ReadFile(filepath.Join(dir, ".storage-engine"))
		if string(marker) != "pebble\n" {
			t.Fatalf("marker = %q, want \"pebble\\n\"", marker)
		}
	})

	t.Run("fresh dir with no request defaults to pebble", func(t *testing.T) {
		t.Parallel()
		// #185 adopted pebble as the default for a brand new data directory.
		if got, err := resolveEngine(t.TempDir(), ""); err != nil || got != EnginePebble {
			t.Fatalf("fresh empty request: got %q, %v; want pebble, nil", got, err)
		}
	})

	t.Run("an empty request resolves to the marker", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		if _, err := resolveEngine(dir, EnginePebble); err != nil {
			t.Fatal(err)
		}
		// A restart with no flag must reopen as pebble, not error or default.
		if got, err := resolveEngine(dir, ""); err != nil || got != EnginePebble {
			t.Fatalf("reopen with no flag: got %q, %v; want pebble, nil", got, err)
		}
	})

	t.Run("a request that contradicts the marker is refused", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		if _, err := resolveEngine(dir, EngineLevelDB); err != nil {
			t.Fatal(err)
		}
		if _, err := resolveEngine(dir, EnginePebble); err == nil {
			t.Fatal("opening a leveldb datadir as pebble should have been refused")
		}
	})

	t.Run("unmarked dir with data is leveldb: pebble refused, leveldb allowed", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		// Simulate a pre-engine-selection goleveldb datadir: files, no marker.
		if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := resolveEngine(dir, EnginePebble); err == nil {
			t.Fatal("an unmarked datadir with data must not be opened as pebble")
		}
		if got, err := resolveEngine(dir, ""); err != nil || got != EngineLevelDB {
			t.Fatalf("unmarked data, no request: got %q, %v; want leveldb, nil", got, err)
		}
	})
}

// TestPebbleIndexStoreOptions checks the db-* tuning knobs reach the equivalent
// pebble.Options fields, and that a zero knob leaves Pebble's own default.
func TestPebbleIndexStoreOptions(t *testing.T) {
	t.Parallel()

	opts := defaultOptions()
	o := pebbleIndexStoreOptions(opts)

	if o.Cache == nil {
		t.Error("block cache capacity did not produce a Cache")
	}
	if o.MemTableSize != opts.LdbWriteBufferSize {
		t.Errorf("MemTableSize = %d, want %d", o.MemTableSize, opts.LdbWriteBufferSize)
	}
	// At the shared default trigger, pebble keeps its own shallow-L0 default
	// (#185) rather than taking the goleveldb-oriented default value.
	if want := pebblestore.DefaultOptions().L0CompactionThreshold; o.L0CompactionThreshold != want {
		t.Errorf("L0CompactionThreshold = %d, want the pebble default %d", o.L0CompactionThreshold, want)
	}
	if o.L0StopWritesThreshold != opts.LdbWritePauseTrigger {
		t.Errorf("L0StopWritesThreshold = %d, want %d", o.L0StopWritesThreshold, opts.LdbWritePauseTrigger)
	}
	if o.MaxOpenFiles != int(opts.LdbOpenFilesLimit) {
		t.Errorf("MaxOpenFiles = %d, want %d", o.MaxOpenFiles, opts.LdbOpenFilesLimit)
	}

	// An explicit, non-default compaction trigger overrides the pebble default.
	opts.LdbCompactionL0Trigger = 6
	if got := pebbleIndexStoreOptions(opts).L0CompactionThreshold; got != 6 {
		t.Errorf("explicit L0 trigger not honoured: L0CompactionThreshold = %d, want 6", got)
	}

	// A zero compaction knob must not override Pebble's default with 0.
	opts.LdbCompactionL0Trigger = 0
	if got := pebbleIndexStoreOptions(opts).L0CompactionThreshold; got == 0 {
		t.Error("a zero knob overrode L0CompactionThreshold with 0 instead of keeping the pebble default")
	}
}

// TestPebbleBlockCacheDefault checks that a Pebble index store gets its own,
// larger block cache when db-block-cache-capacity is left at the shared
// default, that any other value is used as given, and that goleveldb keeps the
// shared default (#555).
func TestPebbleBlockCacheDefault(t *testing.T) {
	t.Parallel()

	opts := defaultOptions()
	if got, want := pebbleIndexStoreOptions(opts).Cache.MaxSize(), int64(defaultPebbleBlockCacheCapacity); got != want {
		t.Errorf("pebble block cache at the shared default = %d, want %d", got, want)
	}
	if got, want := indexStoreOptions(opts).BlockCacheCapacity, int(defaultBlockCacheCapacity); got != want {
		t.Errorf("goleveldb block cache at the shared default = %d, want %d", got, want)
	}

	// A value other than the shared default, larger or smaller, is used as given.
	for _, v := range []uint64{64 << 20, 1 << 30, defaultBlockCacheCapacity - 1} {
		opts.LdbBlockCacheCapacity = v
		if got := pebbleIndexStoreOptions(opts).Cache.MaxSize(); got != int64(v) {
			t.Errorf("explicit block cache %d not honoured: got %d", v, got)
		}
	}
}

// TestInitStoreOpensBothEngines is the integration check that the selector
// actually constructs a working store for each engine, and errors on an unknown
// one. Each engine opens its own fresh directory.
func TestInitStoreOpensBothEngines(t *testing.T) {
	t.Parallel()

	for _, engine := range []string{EngineLevelDB, EnginePebble} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			opts := defaultOptions()
			opts.StorageEngine = engine

			store, err := initStore(t.TempDir(), opts)
			if err != nil {
				t.Fatalf("initStore(%s): %v", engine, err)
			}
			t.Cleanup(func() { _ = store.Close() })

			// Both engines must expose the neutral level-0 reader the write-stall
			// watch depends on.
			if _, ok := store.(interface{ Level0Files() int }); !ok {
				t.Errorf("%s store does not implement Level0Files()", engine)
			}
		})
	}

	t.Run("unknown engine errors", func(t *testing.T) {
		t.Parallel()
		opts := defaultOptions()
		opts.StorageEngine = "rocksdb"
		if _, err := initStore(t.TempDir(), opts); err == nil {
			t.Fatal("an unknown storage engine should have errored")
		}
	})
}
