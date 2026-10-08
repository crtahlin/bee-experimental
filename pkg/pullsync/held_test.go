// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	"github.com/ethersphere/bee/v2/pkg/soc"
	soctesting "github.com/ethersphere/bee/v2/pkg/soc/testing"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// holderMock holds every chunk offered, or refuses with err.
type holderMock struct {
	mu   sync.Mutex
	err  error
	held []swarm.Address
}

func (h *holderMock) HoldUnvalidated(_ context.Context, ch swarm.Chunk, cause error) (postage.HoldResult, error) {
	if !postage.HoldsUnvalidated(cause) {
		return postage.NotHeld, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return postage.NotHeld, h.err
	}
	res := postage.HeldNew
	for _, a := range h.held {
		if a.Equal(ch.Address()) {
			res = postage.HeldAgain
		}
	}
	h.held = append(h.held, ch.Address())
	return res, nil
}

func (h *holderMock) heldCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.held)
}

// heldSetup offers four chunks, of which the second has a stamp whose batch
// is not known (postage.ErrNotFound).
func heldSetup(t *testing.T, holder *holderMock) (*storer.BinC, []swarm.Chunk, func() (uint64, int, error), *mock.ReserveStore) {
	t.Helper()

	tChunks := testingc.GenerateTestRandomChunks(4)
	tResults := make([]*storer.BinC, len(tChunks))
	for i, c := range tChunks {
		stampHash, err := c.Stamp().Hash()
		if err != nil {
			t.Fatal(err)
		}
		tResults[i] = &storer.BinC{Address: c.Address(), BatchID: c.Stamp().BatchID(), BinID: uint64(i + 1), StampHash: stampHash}
	}
	unknown := tChunks[1].Address()
	validStamp := func(c swarm.Chunk) (swarm.Chunk, error) {
		if c.Address().Equal(unknown) {
			return nil, postage.ErrNotFound
		}
		return c, nil
	}

	ps, _ := newPullSync(t, nil, 10, mock.WithSubscribeResp(tResults, nil), mock.WithChunks(tChunks...))
	recorder := streamtest.New(streamtest.WithProtocols(ps.Protocol()))
	client, clientDb := newPullSyncWithStamperValidator(t, recorder, 0, validStamp)
	client.SetChunkHolder(holder)

	sync := func() (uint64, int, error) { return client.Sync(context.Background(), swarm.ZeroAddress, 0, 0) }
	return tResults[1], tChunks, sync, clientDb
}

// TestSyncHoldsUnknownBatch checks that a chunk of a batch not seen yet is
// held, not stored and not counted as an error, and the interval advances
// past it (#583).
func TestSyncHoldsUnknownBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		holder := &holderMock{}
		heldBin, tChunks, sync, clientDb := heldSetup(t, holder)

		topmost, count, err := sync()
		if err != nil {
			t.Fatalf("sync: %v", err)
		}
		if topmost != 4 {
			t.Fatalf("got topmost %d, want 4", topmost)
		}
		if count != 3 {
			t.Fatalf("got %d chunks put, want 3", count)
		}
		if holder.heldCount() != 1 || !holder.held[0].Equal(heldBin.Address) {
			t.Fatalf("held %v, want only %s", holder.held, heldBin.Address)
		}
		if _, err := clientDb.ReserveGet(context.Background(), tChunks[1].Address(), tChunks[1].Stamp().BatchID(), heldBin.StampHash); err == nil {
			t.Fatal("held chunk was put into the reserve")
		}
	})
}

// TestSyncNoProgressWhenNotHeld checks that a chunk which qualified to be
// held but could not be makes the page report no progress, so the puller
// retries it rather than skipping it (#583).
func TestSyncNoProgressWhenNotHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		holder := &holderMock{err: postage.ErrHeldAreaFull}
		_, _, sync, _ := heldSetup(t, holder)

		topmost, count, err := sync()
		if !errors.Is(err, postage.ErrHeldAreaFull) {
			t.Fatalf("got error %v, want %v", err, postage.ErrHeldAreaFull)
		}
		if topmost != 0 {
			t.Fatalf("got topmost %d, want 0 (no progress)", topmost)
		}
		if count != 3 {
			t.Fatalf("got %d chunks put, want 3", count)
		}
	})
}

// TestSyncWithoutHolderUnchanged checks that without a holder the stamp error is
// reported as before and the interval still advances.
func TestSyncWithoutHolderUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tChunks := testingc.GenerateTestRandomChunks(2)
		tResults := make([]*storer.BinC, len(tChunks))
		for i, c := range tChunks {
			stampHash, _ := c.Stamp().Hash()
			tResults[i] = &storer.BinC{Address: c.Address(), BatchID: c.Stamp().BatchID(), BinID: uint64(i + 1), StampHash: stampHash}
		}
		validStamp := func(c swarm.Chunk) (swarm.Chunk, error) {
			if c.Address().Equal(tChunks[0].Address()) {
				return nil, postage.ErrNotFound
			}
			return c, nil
		}
		ps, _ := newPullSync(t, nil, 10, mock.WithSubscribeResp(tResults, nil), mock.WithChunks(tChunks...))
		recorder := streamtest.New(streamtest.WithProtocols(ps.Protocol()))
		client, _ := newPullSyncWithStamperValidator(t, recorder, 0, validStamp)

		topmost, _, err := client.Sync(context.Background(), swarm.ZeroAddress, 0, 0)
		if !errors.Is(err, postage.ErrNotFound) {
			t.Fatalf("got error %v, want %v", err, postage.ErrNotFound)
		}
		if topmost != 2 {
			t.Fatalf("got topmost %d, want 2", topmost)
		}
	})
}

// TestSyncHoldRequiresContentAddress checks that a delivered chunk whose
// data does not match its full content address never reaches the holder,
// and the interval still advances past it (#583).
func TestSyncHoldRequiresContentAddress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		genuine := testingc.GenerateTestRandomChunk()
		socData := soctesting.GenerateMockSOC(t, []byte("payload")).Chunk().Data()
		foreign := swarm.NewChunk(genuine.Address(), socData).WithStamp(genuine.Stamp())
		stampHash, err := foreign.Stamp().Hash()
		if err != nil {
			t.Fatal(err)
		}
		results := []*storer.BinC{{Address: foreign.Address(), BatchID: foreign.Stamp().BatchID(), BinID: 1, StampHash: stampHash}}
		validStamp := func(swarm.Chunk) (swarm.Chunk, error) { return nil, postage.ErrNotFound }

		ps, _ := newPullSync(t, nil, 10, mock.WithSubscribeResp(results, nil), mock.WithChunks(foreign))
		recorder := streamtest.New(streamtest.WithProtocols(ps.Protocol()))
		client, _ := newPullSyncWithStamperValidator(t, recorder, 0, validStamp)
		holder := &holderMock{}
		client.SetChunkHolder(holder)

		topmost, _, err := client.Sync(context.Background(), swarm.ZeroAddress, 0, 0)
		if !errors.Is(err, postage.ErrNotFound) {
			t.Fatalf("got error %v, want %v", err, postage.ErrNotFound)
		}
		if topmost != 1 {
			t.Fatalf("got topmost %d, want 1", topmost)
		}
		if n := holder.heldCount(); n != 0 {
			t.Fatalf("holder got %d chunks, want 0", n)
		}
	})
}

// TestSyncUnwrapsHeldChunk checks that a held chunk is handed to pss like a
// stored one, so a message it carries is not delayed until catch-up (#583).
func TestSyncUnwrapsHeldChunk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := testingc.GenerateTestRandomChunk()
		stampHash, err := ch.Stamp().Hash()
		if err != nil {
			t.Fatal(err)
		}
		results := []*storer.BinC{{Address: ch.Address(), BatchID: ch.Stamp().BatchID(), BinID: 1, StampHash: stampHash}}
		ps, _ := newPullSync(t, nil, 10, mock.WithSubscribeResp(results, nil), mock.WithChunks(ch))
		recorder := streamtest.New(streamtest.WithProtocols(ps.Protocol()))

		unwrapped := make(chan swarm.Address, 1)
		client := pullsync.New(recorder, mock.NewReserve(), func(c swarm.Chunk) { unwrapped <- c.Address() }, func(*soc.SOC) {},
			func(swarm.Chunk) (swarm.Chunk, error) { return nil, postage.ErrNotFound }, log.Noop, 0, pullsync.DefaultMaxChunksPerSecond)
		t.Cleanup(func() { _ = client.Close() })
		holder := &holderMock{}
		client.SetChunkHolder(holder)

		if _, _, err := client.Sync(context.Background(), swarm.ZeroAddress, 0, 0); err != nil {
			t.Fatalf("sync: %v", err)
		}
		if holder.heldCount() != 1 {
			t.Fatalf("held %d chunks, want 1", holder.heldCount())
		}
		synctest.Wait()
		select {
		case addr := <-unwrapped:
			if !addr.Equal(ch.Address()) {
				t.Fatalf("unwrapped %s, want %s", addr, ch.Address())
			}
		default:
			t.Fatal("held chunk was not unwrapped")
		}
	})
}

// TestSyncHeldUnwrappedOnce checks that a held chunk offered again is not
// handed to pss a second time (#583).
func TestSyncHeldUnwrappedOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := testingc.GenerateTestRandomChunk()
		stampHash, err := ch.Stamp().Hash()
		if err != nil {
			t.Fatal(err)
		}
		results := []*storer.BinC{{Address: ch.Address(), BatchID: ch.Stamp().BatchID(), BinID: 1, StampHash: stampHash}}
		ps, _ := newPullSync(t, nil, 10, mock.WithSubscribeResp(results, nil), mock.WithSubscribeResp(results, nil), mock.WithChunks(ch))
		recorder := streamtest.New(streamtest.WithProtocols(ps.Protocol()))

		var unwraps atomic.Int32
		client := pullsync.New(recorder, mock.NewReserve(), func(swarm.Chunk) { unwraps.Add(1) }, func(*soc.SOC) {},
			func(swarm.Chunk) (swarm.Chunk, error) { return nil, postage.ErrNotFound }, log.Noop, 0, pullsync.DefaultMaxChunksPerSecond)
		t.Cleanup(func() { _ = client.Close() })
		holder := &holderMock{}
		client.SetChunkHolder(holder)

		for range 2 {
			if _, _, err := client.Sync(context.Background(), swarm.ZeroAddress, 0, 0); err != nil {
				t.Fatalf("sync: %v", err)
			}
		}
		synctest.Wait()
		if holder.heldCount() != 2 {
			t.Fatalf("holder offered %d times, want 2", holder.heldCount())
		}
		if n := unwraps.Load(); n != 1 {
			t.Fatalf("unwrapped %d times, want 1", n)
		}
	})
}
