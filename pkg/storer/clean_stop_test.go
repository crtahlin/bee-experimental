// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestStopDuringEvictionRoundLeavesStoreClean is the reproduction of wasp
// #634: an eviction round still running when Close's drain gives up used
// to read a closed store (panic: pebble: closed) and leave the dirty
// marker, so the next start ran a full recovery.
func TestStopDuringEvictionRoundLeavesStoreClean(t *testing.T) {
	for _, engine := range []string{storer.EnginePebble, storer.EngineLevelDB} {
		t.Run(engine, func(t *testing.T) {
			dir := t.TempDir()
			baseAddr := swarm.RandAddress(t)
			opts := dbTestOps(baseAddr, 1000, mock.New(), nil, time.Minute)
			opts.StorageEngine = engine
			opts.ShutdownTimeout = 100 * time.Millisecond

			db, err := storer.New(context.Background(), dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			storer.SetDrainWindow(db, 200*time.Millisecond)

			batch := postagetesting.MustNewBatch()
			putter := db.ReservePutter()
			for po := range 4 {
				for range 20 {
					ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, po).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
					if err := putter.Put(context.Background(), ch); err != nil {
						t.Fatal(err)
					}
				}
			}

			entered := make(chan struct{})
			release := make(chan struct{})
			var once bool
			reserve.RoundStall = func() {
				if once {
					return
				}
				once = true
				close(entered)
				<-release
			}
			t.Cleanup(func() { reserve.RoundStall = nil })

			readyC := make(chan struct{})
			db.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), readyC)
			<-readyC
			if err := db.EvictBatch(context.Background(), batch.ID); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(30 * time.Second):
				t.Fatal("eviction round never started")
			}

			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()

			// The drain gives up; Close must now wait for the round, not
			// close the store under it.
			select {
			case err := <-closed:
				t.Fatalf("Close returned while a round was inside the store: %v", err)
			case <-time.After(time.Second):
			}
			close(release)

			select {
			case err := <-closed:
				// The drain gave up, so Close reports the running worker;
				// what matters is that the store then closed cleanly.
				if err == nil {
					t.Fatal("Close reported no running worker; the drain did not give up, so the gate was not exercised")
				}
			case <-time.After(30 * time.Second):
				t.Fatal("Close did not return after the round was released")
			}

			if _, err := os.Stat(filepath.Join(dir, "sharky", ".DIRTY")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("dirty marker after Close: %v", err)
			}
		})
	}
}

// A stop during startup cancels New's context. New returns the context's
// error and closes what it opened, so the dirty marker is removed and the
// next start runs no recovery (wasp #635).
func TestNewCancelledDuringStartupClosesTheStore(t *testing.T) {
	dir := t.TempDir()
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 1000, mock.New(), nil, time.Minute)

	db, err := storer.New(context.Background(), dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	putter := db.ReservePutter()
	for range 50 {
		ch := chunk.GenerateTestRandomChunkAt(t, baseAddr, 2).WithStamp(postagetesting.MustNewBatchStamp(batch.ID))
		if err := putter.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if db, err := storer.New(ctx, dir, opts); err == nil {
		_ = db.Close()
		t.Fatal("New with a cancelled context succeeded, want the context's error")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("New returned %v, want context.Canceled", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "sharky", ".DIRTY")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dirty marker after a cancelled start: %v", err)
	}
}
