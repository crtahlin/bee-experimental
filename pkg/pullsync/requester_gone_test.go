// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	"github.com/ethersphere/bee/v2/pkg/pullsync/pb"
	"github.com/ethersphere/bee/v2/pkg/storer"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

const (
	psProtocol = "pullsync"
	psVersion  = "1.4.0"
	psStream   = "pullsync"
)

// TestRequesterGoneEndsWaitingRequest reproduces #641: a requester gives up on
// a request in an empty bin and resets its stream. It asserts only that every
// server handler returns, so it runs unchanged against upstream code (with the
// recorder's read deadline), where the handler stays waiting and the test
// fails at the deadline.
func TestRequesterGoneEndsWaitingRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))
		client, _ := newPullSync(t, recorder, 0)
		ctx, cancel := context.WithCancel(context.Background())

		res := startSync(ctx, client, 22, 240)
		synctest.Wait()
		cancel()
		<-res

		// Records holds the recorder's lock while it waits for every
		// handler, so it runs on its own goroutine.
		done := make(chan error, 1)
		go func() {
			_, err := recorder.Records(swarm.ZeroAddress, psProtocol, psVersion, psStream)
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(waitDeadline):
			t.Fatalf("server handler still waiting %s after the requester reset its stream, want it ended", waitDeadline)
		}
	})
}

// openRaw opens a pull-sync stream without the requester side of Syncer and
// writes a Get.
func openRaw(t *testing.T, recorder *streamtest.Recorder, bin uint8, start uint64) p2p.Stream {
	t.Helper()
	stream, err := recorder.NewStream(context.Background(), swarm.ZeroAddress, nil, psProtocol, psVersion, psStream)
	if err != nil {
		t.Fatal(err)
	}
	w := protobuf.NewWriter(stream)
	if err := w.WriteMsg(&pb.Get{Bin: int32(bin), Start: start}); err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestRequesterDataBeforeOffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))

		stream := openRaw(t, recorder, 22, 240)
		synctest.Wait()
		if got := server.WaitingTracked(); got != 1 {
			t.Fatalf("waiting %d, want 1 before the extra byte", got)
		}
		if _, err := stream.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		if got := server.RequestsAbandoned("unexpected_data"); got != 1 {
			t.Fatalf("unexpected_data %v, want 1", got)
		}
		if got := server.WaitingTracked(); got != 0 {
			t.Fatalf("waiting %d, want 0", got)
		}
		var offer pb.Offer
		if err := protobuf.NewReader(stream).ReadMsg(&offer); err == nil {
			t.Fatal("read an offer, want the stream ended by the server")
		}
		_ = stream.Reset()
	})
}

func TestWatcherDoesNotDisturbNormalRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, serverDb := newPullSync(t, nil, 5, mock.WithChunks(chunks...))
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))
		client, clientDb := newPullSync(t, recorder, 0)

		res := startSync(context.Background(), client, 0, 3)
		synctest.Wait()
		serverDb.PublishBin(0, results[3])

		select {
		case r := <-res:
			if r.err != nil || r.count != 1 {
				t.Fatalf("err %v count %d, want the chunk", r.err, r.count)
			}
		case <-time.After(waitDeadline):
			t.Fatal("request did not complete after the chunk arrived")
		}
		haveChunks(t, clientDb, chunks[3])
		if got := server.RequestsAbandonedTotal(); got != 0 {
			t.Fatalf("abandoned %v, want 0", got)
		}
		if got := server.RequestsUnwatched(); got != 0 {
			t.Fatalf("unwatched %v, want 0", got)
		}
	})
}

// TestRequesterDataAtBoundary sends a byte after the offer was built but
// before the watcher stopped: the offer is not written.
func TestRequesterDataAtBoundary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reached := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		restore := pullsync.SetAfterMakeOffer(func() {
			if calls.Add(1) == 1 {
				close(reached)
				<-release
			}
		})
		// restored after the server Syncer closes (cleanups run in
		// reverse order), so no handler reads the hook while it changes
		t.Cleanup(restore)

		server, _ := newPullSync(t, nil, 5,
			mock.WithSubscribeResp([]*storer.BinC{results[1]}, nil),
			mock.WithChunks(chunks...))
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))

		stream := openRaw(t, recorder, 0, 1)
		<-reached
		if _, err := stream.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		close(release)
		synctest.Wait()

		if got := server.RequestsAbandoned("unexpected_data"); got != 1 {
			t.Fatalf("unexpected_data %v, want 1", got)
		}
		var offer pb.Offer
		if err := protobuf.NewReader(stream).ReadMsg(&offer); err == nil {
			t.Fatal("read an offer, want none written")
		}
		_ = stream.Reset()
	})
}

// TestReplacedWithoutResetNotAbandoned: the same peer asks again without
// resetting; the older request is replaced (#640), not abandoned.
func TestReplacedWithoutResetNotAbandoned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5,
			mock.WithSubscribeResp(nil, nil), mock.WithSubscribeResp(nil, nil))
		client := newClient(t, server, peerA)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		first := startSync(ctx, client, 22, 240)
		synctest.Wait()
		second := startSync(ctx, client, 22, 240)
		synctest.Wait()
		requireEnded(t, "first request", first)

		if got := server.RequestsReplaced("same_start"); got != 1 {
			t.Fatalf("same_start %v, want 1", got)
		}
		if got := server.RequestsAbandonedTotal(); got != 0 {
			t.Fatalf("abandoned %v, want 0", got)
		}

		cancel()
		<-second
	})
}

// TestAbandonedEntryNotReplaced: the requester resets, its request is still
// registered (held by the hook), and the requester asks again with the same
// start: the old entry is skipped, not counted as replaced.
func TestAbandonedEntryNotReplaced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reached := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		restore := pullsync.SetAfterMakeOffer(func() {
			if calls.Add(1) == 1 {
				close(reached)
				<-release
			}
		})
		// restored after the server Syncer closes (cleanups run in
		// reverse order), so no handler reads the hook while it changes
		t.Cleanup(restore)

		server, _ := newPullSync(t, nil, 5)
		client := newClient(t, server, peerA)
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()

		first := startSync(ctx1, client, 22, 240)
		synctest.Wait()
		cancel1()
		<-first
		<-reached

		second := startSync(ctx2, client, 22, 240)
		synctest.Wait()
		if got := server.RequestsReplaced("same_start"); got != 0 {
			t.Fatalf("same_start %v, want 0: the abandoned entry must be skipped", got)
		}
		close(release)
		synctest.Wait()

		if got := server.RequestsAbandoned("eof"); got != 1 {
			t.Fatalf("eof %v, want 1", got)
		}
		requireWaiting(t, "second request", second)

		cancel2()
		<-second
	})
}

// TestAbandonedEntryOutsideCap: an abandoned entry still registered does not
// count toward the cap of two, so it does not end a live request as over_cap.
func TestAbandonedEntryOutsideCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reached := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		restore := pullsync.SetAfterMakeOffer(func() {
			if calls.Add(1) == 1 {
				close(reached)
				<-release
			}
		})
		// restored after the server Syncer closes (cleanups run in
		// reverse order), so no handler reads the hook while it changes
		t.Cleanup(restore)

		server, _ := newPullSync(t, nil, 5)
		client := newClient(t, server, peerA)
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gone := startSync(ctx1, client, 22, 100)
		synctest.Wait()
		cancel1()
		<-gone
		<-reached

		r2 := startSync(ctx, client, 22, 200)
		synctest.Wait()
		r3 := startSync(ctx, client, 22, 300)
		synctest.Wait()
		if got := server.RequestsReplaced("over_cap"); got != 0 {
			t.Fatalf("over_cap %v, want 0", got)
		}
		close(release)
		requireWaiting(t, "request 2", r2)
		requireWaiting(t, "request 3", r3)

		cancel()
		<-r2
		<-r3
	})
}

func TestRequesterGoneSharedCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, serverDb := newPullSync(t, nil, 5, mock.WithChunks(chunks...))
		ra := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))
		rb := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerB))
		a, _ := newPullSync(t, ra, 0)
		b, bDb := newPullSync(t, rb, 0)
		ctxA, cancelA := context.WithCancel(context.Background())

		aRes := startSync(ctxA, a, 0, 2)
		synctest.Wait()
		// peer B joins the shared collection and settles there before A
		// goes away
		bRes := startSync(context.Background(), b, 0, 2)
		synctest.Wait()
		cancelA()
		<-aRes
		synctest.Wait()

		if got := server.RequestsAbandoned("eof"); got != 1 {
			t.Fatalf("eof %v, want 1", got)
		}
		serverDb.PublishBin(0, results[2])
		select {
		case r := <-bRes:
			if r.err != nil || r.count != 1 {
				t.Fatalf("peer B: err %v count %d, want the chunk", r.err, r.count)
			}
		case <-time.After(waitDeadline):
			t.Fatal("peer B did not receive the chunk")
		}
		haveChunks(t, bDb, chunks[2])
	})
}

// noDeadlineStream hides the stream's read deadline.
type noDeadlineStream struct{ p2p.Stream }

func TestUnwatchedWithoutReadDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		hide := func(h p2p.HandlerFunc) p2p.HandlerFunc {
			return func(ctx context.Context, p p2p.Peer, s p2p.Stream) error {
				return h(ctx, p, noDeadlineStream{s})
			}
		}
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA),
			streamtest.WithMiddlewares(hide))
		client, _ := newPullSync(t, recorder, 0)
		ctx, cancel := context.WithCancel(context.Background())

		res := startSync(ctx, client, 22, 240)
		synctest.Wait()
		cancel()
		<-res
		synctest.Wait()

		if got := server.RequestsUnwatched(); got != 1 {
			t.Fatalf("unwatched %v, want 1", got)
		}
		if got := server.WaitingTracked(); got != 1 {
			t.Fatalf("waiting %d, want the unwatched request still waiting", got)
		}
		if got := server.RequestsAbandonedTotal(); got != 0 {
			t.Fatalf("abandoned %v, want 0", got)
		}
	})
}

// TestReplacedThenResetCountedOnce: a request replaced by the same peer
// (#640) whose requester then resets before the watcher stops is counted as
// replaced only.
func TestReplacedThenResetCountedOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reached := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		restore := pullsync.SetAfterMakeOffer(func() {
			if calls.Add(1) == 1 {
				close(reached)
				<-release
			}
		})
		// restored after the server Syncer closes (cleanups run in
		// reverse order), so no handler reads the hook while it changes
		t.Cleanup(restore)

		server, _ := newPullSync(t, nil, 5,
			mock.WithSubscribeResp(nil, nil), mock.WithSubscribeResp(nil, nil))
		client := newClient(t, server, peerA)
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()

		first := startSync(ctx1, client, 22, 240)
		synctest.Wait()
		second := startSync(ctx2, client, 22, 240)
		<-reached
		// the replaced request is held after its offer step; its
		// requester now resets, which its watcher still sees
		cancel1()
		<-first
		synctest.Wait()
		close(release)
		synctest.Wait()

		if got := server.RequestsReplaced("same_start"); got != 1 {
			t.Fatalf("same_start %v, want 1", got)
		}
		if got := server.RequestsAbandonedTotal(); got != 0 {
			t.Fatalf("abandoned %v, want 0: a replaced request is counted as replaced only", got)
		}

		cancel2()
		<-second
	})
}
