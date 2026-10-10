// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/puller"
	"github.com/ethersphere/bee/v2/pkg/puller/intervalstore"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
)

// emptyOffers answers every pull-sync call for bin 0 at once with no error
// and no progress (an empty offer), and blocks on the context for any other
// bin. Its cursors are all 0, so only the live worker runs. Calls are counted
// with an atomic.
type emptyOffers struct {
	calls atomic.Int64
	epoch uint64
}

func (e *emptyOffers) Sync(ctx context.Context, _ swarm.Address, bin uint8, _ uint64) (uint64, int, error) {
	if bin != 0 {
		<-ctx.Done()
		return 0, 0, ctx.Err()
	}
	e.calls.Add(1)
	return 0, 0, nil
}

func (e *emptyOffers) GetCursors(context.Context, swarm.Address) ([]uint64, uint64, error) {
	return []uint64{0}, e.epoch, nil
}

// TestEmptyOfferRetryPaused reproduces #576: a peer that answers every call
// with an empty offer below the start was retried at once, because the pause
// added for #573 applied only to failed calls. Uses only what upstream has.
func TestEmptyOfferRetryPaused(t *testing.T) {
	t.Parallel()

	ps := &emptyOffers{}
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: swarm.RandAddress(t), PO: 0}))
	p := puller.New(swarm.RandAddress(t), mock.NewStateStore(), kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: 1})
	p.Start(context.Background())
	t.Cleanup(func() { _ = p.Close() })

	time.Sleep(500 * time.Millisecond)
	if n := ps.calls.Load(); n > 20 {
		t.Fatalf("%d calls in 500ms to a peer that answers only with empty offers, want at most 20", n)
	}
}

// failFirstEpochRead fails the first read of one key.
type failFirstEpochRead struct {
	storage.StateStorer
	key    string
	mu     sync.Mutex
	failed bool
}

func (f *failFirstEpochRead) Get(key string, i any) error {
	f.mu.Lock()
	if key == f.key && !f.failed {
		f.failed = true
		f.mu.Unlock()
		return errors.New("statestore read failed")
	}
	f.mu.Unlock()
	return f.StateStorer.Get(key, i)
}

// TestEpochCheckRetriedAfterFailure reproduces #590: when the epoch read after
// the cursors request failed once, the cursors were already kept, so the
// epoch check was skipped at every later recalculation and the peer synced
// with intervals from its previous epoch. Uses only what upstream has.
func TestEpochCheckRetriedAfterFailure(t *testing.T) {
	t.Parallel()

	peer := swarm.RandAddress(t)
	epochKey := fmt.Sprintf("%s_epoch_%s", puller.IntervalPrefix, peer.ByteString())

	base := mock.NewStateStore()
	if err := base.Put(epochKey, uint64(1)); err != nil {
		t.Fatal(err)
	}
	old := intervalstore.NewIntervals(1)
	old.Add(1, 100)
	if err := base.Put(puller.PeerIntervalKey(peer, 0), old); err != nil {
		t.Fatal(err)
	}
	st := &failFirstEpochRead{StateStorer: base, key: epochKey}

	ps := &emptyOffers{epoch: 2}
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(kadMock.AddrTuple{Addr: peer, PO: 0}))
	p := puller.New(swarm.RandAddress(t), st, kad, resMock.NewReserve(resMock.WithRadius(0)), ps, nil, log.Noop, puller.Options{Bins: 1})
	p.Start(context.Background())
	t.Cleanup(func() { _ = p.Close() })

	epoch := func() uint64 {
		var e uint64
		_ = base.Get(epochKey, &e)
		return e
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if epoch() == 2 {
			var itv intervalstore.Intervals
			if err := base.Get(puller.PeerIntervalKey(peer, 0), &itv); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("interval from the previous epoch still stored (err %v)", err)
			}
			return
		}
		kad.Trigger()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("stored epoch %d after later recalculations, want 2: the epoch check was skipped", epoch())
}
