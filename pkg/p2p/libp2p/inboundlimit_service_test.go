// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	"github.com/libp2p/go-libp2p/core/network"
)

// newLimitedFullNode returns a full node whose total inbound rate applies
// to loopback, with almost no refill, so the burst is all it admits.
func newLimitedFullNode(t *testing.T, burst int, notifier p2p.PickyNotifier) *libp2p.Service {
	t.Helper()
	s, _ := newService(t, 1, libp2pServiceOpts{
		notifier: notifier,
		libp2pOpts: libp2p.WithInboundLimitOnLoopback(libp2p.Options{
			FullNode:                  true,
			InboundConnectionRate:     0.0001,
			InboundConnectionBurst:    burst,
			InboundConnectionLimitSet: true,
		}),
	})
	return s
}

func refusals(s *libp2p.Service) float64 {
	return s.InboundRefusals(libp2p.InboundBucketGeneral) + s.InboundRefusals(libp2p.InboundBucketKnownFull)
}

// TestInboundLimitServiceRefusesBeforeHandshake checks that inbound
// connections above the rate are refused before the security handshake,
// and that the node's own outbound dials still succeed while its inbound
// buckets are empty.
func TestInboundLimitServiceRefusesBeforeHandshake(t *testing.T) {
	t.Parallel()

	sf := newLimitedFullNode(t, 2, mockNotifier(noopCf, noopDf, true))

	refused := false
	for range 10 {
		sd, _ := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
		before := sf.HandledConnections()
		_, err := sd.Connect(context.Background(), serviceUnderlayAddress(t, sf))
		if err == nil {
			continue
		}
		if refusals(sf) == 0 {
			t.Fatalf("connect error without a refusal: %v", err)
		}
		// Give a connection that got past the limit time to be counted.
		time.Sleep(200 * time.Millisecond)
		if after := sf.HandledConnections(); after != before {
			t.Fatalf("a refused connection completed the connection setup: handled %v, then %v", before, after)
		}
		refused = true
		break
	}
	if !refused {
		t.Fatal("no connection refused above the burst")
	}

	target, _ := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
	if _, err := sf.Connect(context.Background(), serviceUnderlayAddress(t, target)); err != nil {
		t.Fatalf("outbound dial while the inbound buckets are empty: %v", err)
	}
}

// TestInboundLimitServiceRecordsAcceptedFullPeers checks that a full peer
// kademlia refuses is not recorded as known, and one it accepts is.
func TestInboundLimitServiceRecordsAcceptedFullPeers(t *testing.T) {
	t.Parallel()

	refuse := func(context.Context, p2p.Peer, bool) error { return errors.New("test: kademlia refuses") }
	sRefusing := newLimitedFullNode(t, 100, mockNotifier(refuse, noopDf, true))
	sd, _ := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
	_, _ = sd.Connect(context.Background(), serviceUnderlayAddress(t, sRefusing))
	time.Sleep(200 * time.Millisecond)
	if got := sRefusing.KnownFullAddresses(); got != 0 {
		t.Fatalf("refused full peer recorded: %v known addresses", got)
	}

	sAccepting := newLimitedFullNode(t, 100, mockNotifier(noopCf, noopDf, true))
	sd2, _ := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
	if _, err := sd2.Connect(context.Background(), serviceUnderlayAddress(t, sAccepting)); err != nil {
		t.Fatal(err)
	}
	if err := spinlock.Wait(5*time.Second, func() bool { return sAccepting.KnownFullAddresses() == 1 }); err != nil {
		t.Fatalf("accepted full peer not recorded: %v known addresses", sAccepting.KnownFullAddresses())
	}
	// The dialling side records the peer it dialled, too.
	if got := sd2.KnownFullAddresses(); got != 1 {
		t.Fatalf("outbound full peer not recorded: %v known addresses", got)
	}
}

// TestInboundLimitServiceReachabilityMetrics checks the reachability gauge
// and the counter of switches to private.
func TestInboundLimitServiceReachabilityMetrics(t *testing.T) {
	t.Parallel()

	s, _ := newService(t, 1, libp2pServiceOpts{})
	if got := s.ReachabilityPublic(); got != 0 {
		t.Fatalf("gauge %v before any event, want 0", got)
	}
	s.ObserveReachability(network.ReachabilityPublic)
	if got := s.ReachabilityPublic(); got != 1 {
		t.Fatalf("gauge %v when public, want 1", got)
	}
	s.ObserveReachability(network.ReachabilityPrivate)
	// Two AutoNAT clients can report the same change: it counts once.
	s.ObserveReachability(network.ReachabilityPrivate)
	s.ObserveReachability(network.ReachabilityPublic)
	s.ObserveReachability(network.ReachabilityPrivate)
	if got := s.ReachabilityPublic(); got != 0 {
		t.Fatalf("gauge %v when private, want 0", got)
	}
	if got := s.ReachabilityToPrivate(); got != 2 {
		t.Fatalf("got %v switches to private, want 2", got)
	}
}

// TestInboundLimitServiceWarnsWhenPrivateAfterRefusal checks that the
// warning is logged when the node turns private shortly after a refusal,
// and not without one.
func TestInboundLimitServiceWarnsWhenPrivateAfterRefusal(t *testing.T) {
	t.Parallel()

	const warning = "node judged itself unreachable shortly after the total inbound connection rate refused connections"

	var quiet, loud syncBuffer
	sQuiet, _ := newService(t, 1, libp2pServiceOpts{
		Logger:     log.NewLogger("test", log.WithSink(&quiet), log.WithSynchronousSink()),
		libp2pOpts: libp2p.Options{FullNode: true},
	})
	sQuiet.ObserveReachability(network.ReachabilityPrivate)
	if strings.Contains(quiet.String(), warning) {
		t.Fatal("warning logged without any refusal")
	}

	sLoud, _ := newService(t, 1, libp2pServiceOpts{
		Logger:   log.NewLogger("test", log.WithSink(&loud), log.WithSynchronousSink()),
		notifier: mockNotifier(noopCf, noopDf, true),
		libp2pOpts: libp2p.WithInboundLimitOnLoopback(libp2p.Options{
			FullNode:                  true,
			InboundConnectionRate:     0.0001,
			InboundConnectionBurst:    1,
			InboundConnectionLimitSet: true,
		}),
	})
	for range 5 {
		sd, _ := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
		_, _ = sd.Connect(context.Background(), serviceUnderlayAddress(t, sLoud))
		if refusals(sLoud) > 0 {
			break
		}
	}
	if refusals(sLoud) == 0 {
		t.Fatal("no refusal")
	}
	sLoud.ObserveReachability(network.ReachabilityPrivate)
	var line string
	for _, l := range strings.Split(loud.String(), "\n") {
		if strings.Contains(l, warning) {
			line = l
		}
	}
	if line == "" {
		t.Fatal("warning not logged after a refusal")
	}
	if !strings.Contains(line, `"level"="warning"`) {
		t.Fatalf("logged at the wrong level: %s", line)
	}
}

// TestInboundLimitServiceHourlyPass checks that the hourly pass keeps a
// connected full peer known past the 24-hour expiry.
func TestInboundLimitServiceHourlyPass(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	s, _ := newService(t, 1, libp2pServiceOpts{
		notifier: mockNotifier(noopCf, noopDf, true),
		libp2pOpts: libp2p.WithClock(libp2p.WithInboundLimitOnLoopback(libp2p.Options{
			FullNode:                  true,
			InboundConnectionLimitSet: true,
		}), clock.Now),
	})
	sd, _ := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
	if _, err := sd.Connect(context.Background(), serviceUnderlayAddress(t, s)); err != nil {
		t.Fatal(err)
	}
	loopback := mustAddr(t, "/ip4/127.0.0.1/tcp/1634")
	if err := spinlock.Wait(5*time.Second, func() bool { return s.KnownFullContains(loopback) }); err != nil {
		t.Fatal("connected full peer not recorded")
	}

	clock.Add(libp2p.KnownFullPeerTTL - time.Hour)
	s.RefreshConnectedFullPeers()
	clock.Add(2 * time.Hour)
	if !s.KnownFullContains(loopback) {
		t.Fatal("connected full peer expired despite the hourly pass")
	}
	clock.Add(libp2p.KnownFullPeerTTL)
	if s.KnownFullContains(loopback) {
		t.Fatal("full peer still known 24 hours after its last pass")
	}
}

// TestInboundLimitServiceOffRecordsNothing checks that with the limit off
// no known full peers are recorded.
func TestInboundLimitServiceOffRecordsNothing(t *testing.T) {
	t.Parallel()

	s, _ := newService(t, 1, libp2pServiceOpts{
		notifier: mockNotifier(noopCf, noopDf, true),
		libp2pOpts: libp2p.Options{
			FullNode:                  true,
			InboundConnectionRate:     -1,
			InboundConnectionLimitSet: true,
		},
	})
	// An outbound connect records the full peer before it returns, so
	// the check right after it is deterministic.
	target, _ := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
	if _, err := s.Connect(context.Background(), serviceUnderlayAddress(t, target)); err != nil {
		t.Fatal(err)
	}
	if got := s.KnownFullAddresses(); got != 0 {
		t.Fatalf("got %v known addresses with the limit off, want 0", got)
	}
}

// syncBuffer is a bytes.Buffer safe for the logger's concurrent writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
