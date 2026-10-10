// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	"github.com/ethersphere/bee/v2/pkg/soc"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// streamCtxs gives every inbound stream its own handler context, which the
// test cancels to play the part of libp2p ending the connection (#672).
// streamtest itself passes context.Background() to handlers.
type streamCtxs struct {
	mu      sync.Mutex
	cancels []context.CancelFunc
	hide    bool // hide the stream's read deadline: no watcher
}

func (c *streamCtxs) middleware(h p2p.HandlerFunc) p2p.HandlerFunc {
	return func(_ context.Context, p p2p.Peer, s p2p.Stream) error {
		ctx, cancel := context.WithCancel(context.Background())
		c.mu.Lock()
		c.cancels = append(c.cancels, cancel)
		c.mu.Unlock()
		defer cancel()
		if c.hide {
			s = noDeadlineStream{s}
		}
		return h(ctx, p, s)
	}
}

// drop cancels the handler context of the i-th stream.
func (c *streamCtxs) drop(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancels[i]()
}

func newDisconnectServer(t *testing.T, ctxs *streamCtxs) (*pullsync.Syncer, *pullsync.Syncer) {
	t.Helper()
	server, _ := newPullSync(t, nil, 5)
	recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA),
		streamtest.WithMiddlewares(ctxs.middleware))
	client, _ := newPullSync(t, recorder, 0)
	return server, client
}

// TestDisconnectCountedWhenParentFirst: the connection ends (the handler
// context is cancelled) with no stream error, so the watcher sees nothing. The
// request is counted once as disconnect; before #672 it was not counted.
func TestDisconnectCountedWhenParentFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctxs := &streamCtxs{}
		server, client := newDisconnectServer(t, ctxs)
		res := startSync(context.Background(), client, 22, 240)
		synctest.Wait()

		ctxs.drop(0)
		synctest.Wait()

		if got := server.RequestsAbandoned("disconnect"); got != 1 {
			t.Fatalf("disconnect %v, want 1", got)
		}
		if got := server.RequestsAbandonedTotal(); got != 1 {
			t.Fatalf("abandoned %v in all, want 1", got)
		}
		requireEnded(t, "request", res)
	})
}

// TestDisconnectWatcherFirst: the requester gives up on its stream, which the
// watcher sees first; counted once under the watcher's reason, as before (the
// test recorder's reset is a full close, so the reason is eof), and not also
// as disconnect.
func TestDisconnectWatcherFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctxs := &streamCtxs{}
		server, client := newDisconnectServer(t, ctxs)
		ctx, cancel := context.WithCancel(context.Background())
		res := startSync(ctx, client, 22, 240)
		synctest.Wait()
		cancel()
		<-res
		synctest.Wait()

		if got := server.RequestsAbandoned("eof"); got != 1 {
			t.Fatalf("eof %v, want 1", got)
		}
		if got := server.RequestsAbandoned("disconnect"); got != 0 {
			t.Fatalf("disconnect %v, want 0", got)
		}
		if got := server.RequestsAbandonedTotal(); got != 1 {
			t.Fatalf("abandoned %v in all, want 1", got)
		}
	})
}

// TestDisconnectWithoutWatcher: a stream without a read deadline has no
// watcher; when its connection ends the request is counted as disconnect.
func TestDisconnectWithoutWatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctxs := &streamCtxs{hide: true}
		server, client := newDisconnectServer(t, ctxs)
		res := startSync(context.Background(), client, 22, 240)
		synctest.Wait()
		if got := server.RequestsUnwatched(); got != 1 {
			t.Fatalf("unwatched %v, want 1", got)
		}

		ctxs.drop(0)
		synctest.Wait()

		if got := server.RequestsAbandoned("disconnect"); got != 1 {
			t.Fatalf("disconnect %v for an unwatched request, want 1", got)
		}
		requireEnded(t, "request", res)
	})
}

// TestDisconnectNotCountedOnShutdown: the syncer stops while a request waits.
// Nothing is counted, also when the connection's context ends during the
// shutdown (the hook cancels it between building the offer and the count).
func TestDisconnectNotCountedOnShutdown(t *testing.T) {
	ctxs := &streamCtxs{}
	var once atomic.Bool
	t.Cleanup(pullsync.SetAfterMakeOffer(func() {
		if once.CompareAndSwap(false, true) {
			ctxs.drop(0)
		}
	}))
	synctest.Test(t, func(t *testing.T) {
		// A server without the cleanup Close: the test closes it itself.
		server := pullsync.New(nil, mock.NewReserve(), func(swarm.Chunk) {}, func(*soc.SOC) {},
			func(ch swarm.Chunk) (swarm.Chunk, error) { return ch, nil }, log.Noop, 5, pullsync.DefaultMaxChunksPerSecond)
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA),
			streamtest.WithMiddlewares(ctxs.middleware))
		client, _ := newPullSync(t, recorder, 0)
		_ = startSync(context.Background(), client, 22, 240)
		synctest.Wait()

		_ = server.Close()
		synctest.Wait()

		if got := server.RequestsAbandonedTotal(); got != 0 {
			t.Fatalf("abandoned %v while stopping, want 0", got)
		}
	})
}

// TestReplacedThenDisconnected: a request the same peer replaced, whose
// connection then ends, is counted only as replaced.
func TestReplacedThenDisconnected(t *testing.T) {
	ctxs := &streamCtxs{}
	var once atomic.Bool
	t.Cleanup(pullsync.SetAfterMakeOffer(func() {
		// The first request to leave makeOffer is the replaced one.
		if once.CompareAndSwap(false, true) {
			ctxs.drop(0)
		}
	}))
	synctest.Test(t, func(t *testing.T) {
		server, client := newDisconnectServer(t, ctxs)
		first := startSync(context.Background(), client, 22, 240)
		synctest.Wait()
		second := startSync(context.Background(), client, 22, 240)
		synctest.Wait()

		requireEnded(t, "first request", first)
		if got := server.RequestsReplaced("same_start"); got != 1 {
			t.Fatalf("replaced %v, want 1", got)
		}
		if got := server.RequestsAbandonedTotal(); got != 0 {
			t.Fatalf("abandoned %v for a replaced request, want 0", got)
		}
		_ = second
	})
}
