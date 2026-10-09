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

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/storer"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// waitDeadline is how long a test waits for a request to end or to stay
// waiting. In a synctest bubble it costs no real time.
const waitDeadline = time.Minute

type syncResult struct {
	topmost uint64
	count   int
	err     error
}

// startSync runs Sync in a goroutine and returns its result channel.
func startSync(ctx context.Context, c *pullsync.Syncer, bin uint8, start uint64) <-chan syncResult {
	res := make(chan syncResult, 1)
	go func() {
		top, count, err := c.Sync(ctx, swarm.ZeroAddress, bin, start)
		res <- syncResult{top, count, err}
	}()
	return res
}

// requireEnded fails unless the request returns an error before the deadline.
func requireEnded(t *testing.T, name string, res <-chan syncResult) {
	t.Helper()
	select {
	case r := <-res:
		if r.err == nil {
			t.Fatalf("%s: returned without error, want an error from the server ending it", name)
		}
	case <-time.After(waitDeadline):
		t.Fatalf("%s: still waiting after %s, want it ended by the server", name, waitDeadline)
	}
}

// requireWaiting fails if the request returns before the deadline.
func requireWaiting(t *testing.T, name string, res <-chan syncResult) {
	t.Helper()
	select {
	case r := <-res:
		t.Fatalf("%s: returned (err %v), want it still waiting", name, r.err)
	case <-time.After(waitDeadline):
	}
}

// newClient returns a requester whose requests reach server as the peer addr.
func newClient(t *testing.T, server *pullsync.Syncer, addr swarm.Address) *pullsync.Syncer {
	t.Helper()
	recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(addr))
	c, _ := newPullSync(t, recorder, 0)
	return c
}

var (
	peerA = swarm.MustParseHexAddress("aa00000000000000000000000000000000000000000000000000000000000000")
	peerB = swarm.MustParseHexAddress("bb00000000000000000000000000000000000000000000000000000000000000")
)

// TestDuplicateRequestEndsOlder reproduces the pile-up of #640: one peer asks
// twice for the same bin and start in an empty bin. It asserts only what a
// requester can see, so it runs unchanged against upstream code, where the
// first request stays open and the test fails at the deadline.
func TestDuplicateRequestEndsOlder(t *testing.T) {
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
		requireWaiting(t, "second request", second)

		cancel()
		<-second
	})
}

func TestDuplicateRequestOtherPeerUnaffected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		a := newClient(t, server, peerA)
		b := newClient(t, server, peerB)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ra := startSync(ctx, a, 22, 240)
		synctest.Wait()
		rb := startSync(ctx, b, 22, 240)
		synctest.Wait()

		requireWaiting(t, "peer A", ra)
		requireWaiting(t, "peer B", rb)
		if got := server.RequestsReplaced("same_start") + server.RequestsReplaced("over_cap"); got != 0 {
			t.Fatalf("replaced %v requests, want 0", got)
		}

		cancel()
		<-ra
		<-rb
	})
}

func TestDuplicateRequestOtherBinUnaffected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		client := newClient(t, server, peerA)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r22 := startSync(ctx, client, 22, 240)
		synctest.Wait()
		r23 := startSync(ctx, client, 23, 240)
		synctest.Wait()

		requireWaiting(t, "bin 22", r22)
		requireWaiting(t, "bin 23", r23)

		cancel()
		<-r22
		<-r23
	})
}

func TestWaitingRequestCompletesWhenChunkArrives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, serverDb := newPullSync(t, nil, 5, mock.WithChunks(chunks...))
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))
		client, clientDb := newPullSync(t, recorder, 0)

		res := startSync(context.Background(), client, 0, 3)
		synctest.Wait()
		serverDb.PublishBin(0, results[3])

		select {
		case r := <-res:
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.topmost != 3 || r.count != 1 {
				t.Fatalf("got topmost %d count %d, want 3 and 1", r.topmost, r.count)
			}
		case <-time.After(waitDeadline):
			t.Fatal("request did not complete after the chunk arrived")
		}
		haveChunks(t, clientDb, chunks[3])
	})
}

func TestWaitingRequestsCapPerBin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		client := newClient(t, server, peerA)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r1 := startSync(ctx, client, 22, 100)
		synctest.Wait()
		r2 := startSync(ctx, client, 22, 200)
		synctest.Wait()
		r3 := startSync(ctx, client, 22, 300)
		synctest.Wait()

		requireEnded(t, "oldest request", r1)
		requireWaiting(t, "second request", r2)
		requireWaiting(t, "newest request", r3)
		if got := server.RequestsReplaced("over_cap"); got != 1 {
			t.Fatalf("over_cap %v, want 1", got)
		}
		if got := server.RequestsReplaced("same_start"); got != 0 {
			t.Fatalf("same_start %v, want 0", got)
		}

		cancel()
		<-r2
		<-r3
	})
}

func TestDuplicateRequestSharedCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, serverDb := newPullSync(t, nil, 5, mock.WithChunks(chunks...))
		ra := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))
		rb := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerB))
		a, aDb := newPullSync(t, ra, 0)
		b, bDb := newPullSync(t, rb, 0)

		aOld := startSync(context.Background(), a, 0, 2)
		synctest.Wait()
		bRes := startSync(context.Background(), b, 0, 2)
		synctest.Wait()
		aNew := startSync(context.Background(), a, 0, 2)
		synctest.Wait()

		requireEnded(t, "peer A old request", aOld)
		serverDb.PublishBin(0, results[2])

		for name, res := range map[string]<-chan syncResult{"peer A new request": aNew, "peer B": bRes} {
			select {
			case r := <-res:
				if r.err != nil || r.count != 1 {
					t.Fatalf("%s: err %v count %d, want the chunk", name, r.err, r.count)
				}
			case <-time.After(waitDeadline):
				t.Fatalf("%s: did not receive the chunk", name)
			}
		}
		haveChunks(t, aDb, chunks[2])
		haveChunks(t, bDb, chunks[2])
	})
}

// TestReplacedAfterOfferStillCompletes ends a request just after its offer was
// built: it must still complete, because only building the offer can be ended.
func TestReplacedAfterOfferStillCompletes(t *testing.T) {
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
		client, clientDb := newPullSync(t, recorder, 0)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		older := startSync(context.Background(), client, 0, 1)
		<-reached
		newer := startSync(ctx, client, 0, 1)
		synctest.Wait()
		if got := server.RequestsReplaced("same_start"); got != 1 {
			t.Fatalf("same_start %v, want 1", got)
		}
		close(release)

		select {
		case r := <-older:
			if r.err != nil || r.count != 1 {
				t.Fatalf("older request: err %v count %d, want it completed with the chunk", r.err, r.count)
			}
		case <-time.After(waitDeadline):
			t.Fatal("older request did not complete")
		}
		haveChunks(t, clientDb, chunks[1])

		cancel()
		<-newer
	})
}

func TestWaitingRequestsGauge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// built without the cleanup Close, because the test closes it to
		// end the waiting handlers and then reads the gauge
		server := pullsync.New(nil, mock.NewReserve(), func(swarm.Chunk) {}, func(*soc.SOC) {},
			func(ch swarm.Chunk) (swarm.Chunk, error) { return ch, nil }, log.Noop, 5, pullsync.DefaultMaxChunksPerSecond)
		client := newClient(t, server, peerA)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r1 := startSync(ctx, client, 22, 240)
		synctest.Wait()
		r2 := startSync(ctx, client, 22, 240)
		synctest.Wait()
		r3 := startSync(ctx, client, 23, 136)
		synctest.Wait()
		requireEnded(t, "replaced request", r1)

		if got := server.WaitingRequests(); got != 2 {
			t.Fatalf("waiting gauge %v, want 2", got)
		}
		if got := server.RequestsReplaced("same_start"); got != 1 {
			t.Fatalf("same_start %v, want 1", got)
		}

		cancel()
		<-r2
		<-r3
		_ = server.Close()
		synctest.Wait()
		if got := server.WaitingRequests(); got != 0 {
			t.Fatalf("waiting gauge after close %v, want 0", got)
		}
		if got := server.WaitingTracked(); got != 0 {
			t.Fatalf("tracked after close %d, want 0", got)
		}
	})
}
