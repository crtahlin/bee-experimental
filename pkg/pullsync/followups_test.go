// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/ethersphere/bee/v2/pkg/bitvector"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	"github.com/ethersphere/bee/v2/pkg/pullsync/pb"
	"github.com/ethersphere/bee/v2/pkg/storer"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
)

// TestUnregisteredAfterOffer pins that a request stops being tracked as
// waiting as soon as its offer is built, not when its handler ends (#645).
// The requester reads the offer and holds back its Want, so the handler sits
// in the Want phase while the test looks.
func TestUnregisteredAfterOffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5,
			mock.WithSubscribeResp([]*storer.BinC{results[1]}, nil),
			mock.WithChunks(chunks...))
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))

		stream := openRawBin(t, recorder, 0, 1)
		w, r := protobuf.NewWriterAndReader(stream)
		var offer pb.Offer
		if err := r.ReadMsg(&offer); err != nil {
			t.Fatalf("read offer: %v", err)
		}
		if len(offer.Chunks) != 1 {
			t.Fatalf("offer has %d chunks, want 1", len(offer.Chunks))
		}
		synctest.Wait()

		if got := server.WaitingTracked(); got != 0 {
			t.Fatalf("tracked %d after offer, want 0", got)
		}
		if got := server.WaitingRequests(); got != 0 {
			t.Fatalf("waiting gauge %v after offer, want 0", got)
		}

		bv, err := bitvector.New(8)
		if err != nil {
			t.Fatal(err)
		}
		bv.Set(0)
		if err := w.WriteMsg(&pb.Want{BitVector: bv.Bytes()}); err != nil {
			t.Fatalf("write want: %v", err)
		}
		var delivery pb.Delivery
		if err := r.ReadMsg(&delivery); err != nil {
			t.Fatalf("read delivery: %v", err)
		}
		if len(delivery.Data) == 0 {
			t.Fatal("delivery has no data")
		}
		_ = stream.FullClose()
	})
}

// TestWaitingKeysDeleted pins that unregistering the last request of a
// (peer, bin) removes its key, so the map does not keep a key for every peer
// and bin ever seen (#646).
func TestWaitingKeysDeleted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		client := newClient(t, server, peerA)
		ctx, cancel := context.WithCancel(context.Background())

		r1 := startSync(ctx, client, 22, 240)
		r2 := startSync(ctx, client, 23, 136)
		synctest.Wait()
		if got := server.WaitingKeys(); got != 2 {
			t.Fatalf("keys while waiting %d, want 2", got)
		}

		cancel()
		<-r1
		<-r2
		synctest.Wait()
		if got := server.WaitingTracked(); got != 0 {
			t.Fatalf("tracked after the requests ended %d, want 0", got)
		}
		if got := server.WaitingKeys(); got != 0 {
			t.Fatalf("keys after the requests ended %d, want 0", got)
		}
	})
}

// TestEndedInWaitLoopStartsNoCollection pins that a request ended while it
// still waits for the requests it replaced does not go on to build an offer,
// so it starts no collection and subscription it would cancel at once (#648).
func TestEndedInWaitLoopStartsNoCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reached := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		// Only the first call blocks: A's handler, after A was ended, so A
		// stays registered and B stays in its wait loop. Later calls pass,
		// so the skip path cannot block here.
		restore := pullsync.SetAfterMakeOffer(func() {
			if calls.Add(1) == 1 {
				close(reached)
				<-release
			}
		})
		t.Cleanup(restore)

		server, store := newPullSync(t, nil, 5)
		client := newClient(t, server, peerA)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		a := startSync(ctx, client, 22, 240)
		synctest.Wait()
		b := startSync(ctx, client, 22, 240) // ends A; waits for A to leave
		<-reached
		synctest.Wait()
		c := startSync(ctx, client, 22, 240) // ends B while B is in its wait loop
		synctest.Wait()
		close(release)
		requireEnded(t, "request A", a)
		requireEnded(t, "request B", b)
		synctest.Wait()

		if got := store.SubscribeBinCalls(); got != 2 {
			t.Fatalf("%d SubscribeBin calls, want 2 (A and C only)", got)
		}
		if got := server.RequestsReplaced("same_start"); got != 2 {
			t.Fatalf("same_start %v, want 2", got)
		}
		if got := server.RequestsAbandonedTotal(); got != 0 {
			t.Fatalf("abandoned %v, want 0", got)
		}

		cancel()
		<-c
	})
}
