// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transaction_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/sharky"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
	test "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/cache"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type stopper interface {
	StopAdmitting(func(int64))
}

func newGatedStorage(t *testing.T) (transaction.Storage, *sharky.Store, string) {
	t.Helper()
	dir := t.TempDir()
	sh, err := sharky.New(&dirFS{basedir: dir}, 32, swarm.SocMaxChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	store, _, err := leveldbstore.New("", nil)
	if err != nil {
		t.Fatal(err)
	}
	st := transaction.NewStorage(sh, store)
	// Close the files, or Windows cannot remove the temporary directory.
	t.Cleanup(func() { _ = st.Close() })
	return st, sh, dir
}

func putEntries(t *testing.T, st transaction.Storage, n int) {
	t.Helper()
	for range n {
		ch := test.GenerateTestRandomChunk()
		err := st.Run(context.Background(), func(s transaction.Store) error {
			if err := s.IndexStore().Put(&cache.CacheEntryItem{Address: ch.Address(), AccessTimestamp: 1}); err != nil {
				return err
			}
			return s.ChunkStore().Put(context.Background(), ch)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func stopAsync(st transaction.Storage) chan struct{} {
	done := make(chan struct{})
	go func() {
		st.(stopper).StopAdmitting(nil)
		close(done)
	}()
	return done
}

// A long iteration ends at its next item once the store stops admitting
// work, and the stop then completes (wasp #634).
func TestGateIterationStopsAtNextItem(t *testing.T) {
	st, _, _ := newGatedStorage(t)
	putEntries(t, st, 5)

	inCallback := make(chan struct{})
	release := make(chan struct{})
	iterErr := make(chan error, 1)
	go func() {
		first := true
		iterErr <- st.IndexStore().Iterate(storage.Query{
			Factory: func() storage.Item { return &cache.CacheEntryItem{} },
		}, func(storage.Result) (bool, error) {
			if first {
				first = false
				close(inCallback)
				<-release
			}
			return false, nil
		})
	}()
	<-inCallback

	stopped := stopAsync(st)
	select {
	case <-stopped:
		t.Fatal("stop returned while an iteration was inside the store")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)

	if err := <-iterErr; !errors.Is(err, transaction.ErrClosed) {
		t.Fatalf("iteration returned %v, want ErrClosed at its next item", err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not complete after the iteration ended")
	}
}

// A call nested inside an iteration callback, as the pinning check does,
// is refused once the store is closing instead of deadlocking (wasp #634).
func TestGateNestedCallDoesNotDeadlock(t *testing.T) {
	st, _, _ := newGatedStorage(t)
	putEntries(t, st, 3)

	inCallback := make(chan struct{})
	release := make(chan struct{})
	var nestedErr error
	iterDone := make(chan struct{})
	go func() {
		defer close(iterDone)
		first := true
		_ = st.IndexStore().Iterate(storage.Query{
			Factory: func() storage.Item { return &cache.CacheEntryItem{} },
		}, func(r storage.Result) (bool, error) {
			if first {
				first = false
				close(inCallback)
				<-release
				_, nestedErr = st.IndexStore().Has(r.Entry)
				return true, nil
			}
			return false, nil
		})
	}()
	<-inCallback
	stopped := stopAsync(st)
	time.Sleep(50 * time.Millisecond) // the stop is waiting for the iteration
	close(release)

	select {
	case <-iterDone:
	case <-time.After(5 * time.Second):
		t.Fatal("iteration with a nested call deadlocked during the stop")
	}
	if !errors.Is(nestedErr, transaction.ErrClosed) {
		t.Fatalf("nested call returned %v, want ErrClosed", nestedErr)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not complete")
	}
}

// A transaction admitted before the stop runs to its done, including the
// commit's sharky releases, before the stop completes; its release is not
// refused (a refused release would leak the slot for good) (wasp #634).
func TestGateTransactionAdmittedUntilDone(t *testing.T) {
	st, _, _ := newGatedStorage(t)
	ch := test.GenerateTestRandomChunk()
	if err := st.Run(context.Background(), func(s transaction.Store) error {
		return s.ChunkStore().Put(context.Background(), ch)
	}); err != nil {
		t.Fatal(err)
	}

	tx, done := st.NewTransaction(context.Background())
	if err := tx.ChunkStore().Delete(context.Background(), ch.Address()); err != nil {
		t.Fatal(err)
	}

	stopped := stopAsync(st)
	select {
	case <-stopped:
		t.Fatal("stop returned while a transaction was admitted")
	case <-time.After(200 * time.Millisecond):
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit of an admitted transaction during the stop: %v", err)
	}
	done()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not complete after the transaction was done")
	}
}

// After the stop, every kind of call returns ErrClosed and nothing reaches
// the store (wasp #634).
func TestGateCallsAfterStopAreRefused(t *testing.T) {
	st, _, _ := newGatedStorage(t)
	putEntries(t, st, 1)
	<-stopAsync(st)

	item := &cache.CacheEntryItem{Address: swarm.RandAddress(t)}
	if err := st.IndexStore().Get(item); !errors.Is(err, transaction.ErrClosed) {
		t.Errorf("Get: %v", err)
	}
	if _, err := st.IndexStore().Has(item); !errors.Is(err, transaction.ErrClosed) {
		t.Errorf("Has: %v", err)
	}
	if _, err := st.IndexStore().Count(item); !errors.Is(err, transaction.ErrClosed) {
		t.Errorf("Count: %v", err)
	}
	if err := st.IndexStore().Iterate(storage.Query{Factory: func() storage.Item { return &cache.CacheEntryItem{} }},
		func(storage.Result) (bool, error) { return false, nil }); !errors.Is(err, transaction.ErrClosed) {
		t.Errorf("Iterate: %v", err)
	}
	if _, err := st.ChunkStore().Get(context.Background(), swarm.RandAddress(t)); !errors.Is(err, transaction.ErrClosed) {
		t.Errorf("chunk Get: %v", err)
	}
	if err := st.Run(context.Background(), func(s transaction.Store) error {
		return s.IndexStore().Put(item)
	}); !errors.Is(err, transaction.ErrClosed) {
		t.Errorf("transaction: %v", err)
	}
}

// Many goroutines using the store while it stops: each call returns
// success or ErrClosed, and the stop returns (run under -race).
func TestGateConcurrentStop(t *testing.T) {
	st, _, _ := newGatedStorage(t)
	putEntries(t, st, 20)

	var wg sync.WaitGroup
	stopAll := make(chan struct{})
	errs := make(chan error, 64)
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stopAll:
					return
				default:
				}
				var err error
				if i%2 == 0 {
					ch := test.GenerateTestRandomChunk()
					err = st.Run(context.Background(), func(s transaction.Store) error {
						return s.ChunkStore().Put(context.Background(), ch)
					})
				} else {
					err = st.IndexStore().Iterate(storage.Query{Factory: func() storage.Item { return &cache.CacheEntryItem{} }},
						func(storage.Result) (bool, error) { return false, nil })
				}
				if err != nil && !errors.Is(err, transaction.ErrClosed) {
					errs <- err
					return
				}
			}
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case <-stopAsync(st):
	case <-time.After(10 * time.Second):
		t.Fatal("stop did not return under concurrent use")
	}
	close(stopAll)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}
}
