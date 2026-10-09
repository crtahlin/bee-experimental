// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/ethersphere/bee/v2/pkg/bitvector"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	"github.com/ethersphere/bee/v2/pkg/pullsync/pb"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/libp2p/go-libp2p/core/network"
	yamuxmux "github.com/libp2p/go-libp2p/p2p/muxer/yamux"
)

// muxedStream gives a libp2p muxed stream (yamux, with libp2p's error
// mapping) the p2p.Stream methods, so the handler sees the errors and read
// deadline a production stream has.
type muxedStream struct{ network.MuxedStream }

func (muxedStream) Headers() p2p.Headers         { return nil }
func (muxedStream) ResponseHeaders() p2p.Headers { return nil }
func (muxedStream) Version() (*semver.Version, error) {
	return semver.NewVersion(psVersion)
}
func (s muxedStream) FullClose() error {
	_ = s.CloseWrite()
	return s.Close()
}

// yamuxPair opens a stream over a yamux session pair on net.Pipe, writes a
// Get on the requester side, and runs the server's pull-sync handler on the
// accepted side. It returns the requester's stream, the requester's
// connection and the handler's result.
func yamuxPair(t *testing.T, server *pullsync.Syncer, bin uint8, start uint64) (network.MuxedStream, network.MuxedConn, <-chan error) {
	t.Helper()
	c1, c2 := net.Pipe()
	cli, err := yamuxmux.DefaultTransport.NewConn(c1, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := yamuxmux.DefaultTransport.NewConn(c2, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
		_ = srv.Close()
	})

	cs, err := cli.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := protobuf.NewWriter(cs).WriteMsg(&pb.Get{Bin: int32(bin), Start: start}); err != nil {
		t.Fatal(err)
	}
	ss, err := srv.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}

	var handler p2p.HandlerFunc
	for _, spec := range server.Protocol().StreamSpecs {
		if spec.Name == psStream {
			handler = spec.Handler
		}
	}
	res := make(chan error, 1)
	go func() {
		res <- handler(context.Background(), p2p.Peer{Address: peerA, FullNode: true}, muxedStream{ss})
	}()
	return cs, cli, res
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func handlerReturned(t *testing.T, res <-chan error) error {
	t.Helper()
	select {
	case err := <-res:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not return")
		return nil
	}
}

func TestWatcherYamuxReasons(t *testing.T) {
	for _, tc := range []struct {
		name   string
		end    func(cs network.MuxedStream, cli network.MuxedConn)
		reason string
	}{
		{"requester resets", func(cs network.MuxedStream, _ network.MuxedConn) { _ = cs.Reset() }, "reset"},
		{"connection closes", func(_ network.MuxedStream, cli network.MuxedConn) { _ = cli.Close() }, "disconnect"},
		{"requester closes its write side", func(cs network.MuxedStream, _ network.MuxedConn) { _ = cs.CloseWrite() }, "eof"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newPullSync(t, nil, 5)
			cs, cli, res := yamuxPair(t, server, 22, 240)
			eventually(t, "the request to wait", func() bool { return server.WaitingTracked() == 1 })

			tc.end(cs, cli)

			if err := handlerReturned(t, res); err == nil {
				t.Fatal("handler returned nil, want the request ended")
			}
			if got := server.RequestsAbandoned(tc.reason); got != 1 {
				t.Fatalf("%s %v, want 1 (total %v)", tc.reason, got, server.RequestsAbandonedTotal())
			}
			if got := server.RequestsAbandonedTotal(); got != 1 {
				t.Fatalf("abandoned total %v, want 1", got)
			}
		})
	}
}

// TestWatcherYamuxNormalRequest: the real deadline error at the stop is
// ignored, the offer is written and the Want is read intact.
func TestWatcherYamuxNormalRequest(t *testing.T) {
	server, serverDb := newPullSync(t, nil, 5, mock.WithChunks(chunks...))
	cs, _, res := yamuxPair(t, server, 0, 3)
	eventually(t, "the request to wait", func() bool { return server.WaitingTracked() == 1 })
	// let the watcher block in its read
	time.Sleep(20 * time.Millisecond)

	serverDb.PublishBin(0, results[3])

	w, r := protobuf.NewWriter(cs), protobuf.NewReader(cs)
	var offer pb.Offer
	if err := r.ReadMsg(&offer); err != nil {
		t.Fatalf("read offer: %v", err)
	}
	if len(offer.Chunks) != 1 {
		t.Fatalf("offer has %d chunks, want 1", len(offer.Chunks))
	}
	bv, err := bitvector.New(1)
	if err != nil {
		t.Fatal(err)
	}
	bv.Set(0)
	if err := w.WriteMsg(&pb.Want{BitVector: bv.Bytes()}); err != nil {
		t.Fatal(err)
	}
	var delivery pb.Delivery
	if err := r.ReadMsg(&delivery); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if !chunks[3].Address().Equal(swarmAddress(delivery.Address)) {
		t.Fatal("delivered the wrong chunk")
	}
	_ = cs.CloseWrite()
	if err := handlerReturned(t, res); err != nil {
		t.Fatalf("handler: %v", err)
	}
	_ = cs.Close()
	if got := server.RequestsAbandonedTotal(); got != 0 {
		t.Fatalf("abandoned %v, want 0", got)
	}
	if got := server.RequestsUnwatched(); got != 0 {
		t.Fatalf("unwatched %v, want 0", got)
	}
}

func swarmAddress(b []byte) swarm.Address { return swarm.NewAddress(b) }
