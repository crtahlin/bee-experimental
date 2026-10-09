// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"context"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
)

// TestStreamScanReadsResourceManager checks that a scan reads the inbound
// streams a peer holds from the node's own resource manager.
func TestStreamScanReadsResourceManager(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	s1, overlay1 := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.Options{FullNode: true}})
	s2, _ := newService(t, 1, libp2pServiceOpts{})

	release := make(chan struct{})
	defer close(release)
	if err := s1.AddProtocol(newTestProtocol(func(ctx context.Context, _ p2p.Peer, _ p2p.Stream) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Connect(ctx, serviceUnderlayAddress(t, s1)); err != nil {
		t.Fatal(err)
	}

	const open = 5
	for range open {
		st, err := s2.NewStream(ctx, overlay1, nil, testProtocolName, testProtocolVersion, testStreamName)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = st.Reset() }()
	}

	var got float64
	if err := spinlock.Wait(5*time.Second, func() bool {
		got = s1.ScanStreams()
		return got >= open
	}); err != nil {
		t.Fatalf("got a largest per-peer inbound count of %v, want at least %d", got, open)
	}
}

// TestStreamRefusedByPeerCounted checks that a stream the peer refuses at
// its resource limit is counted by protocol and stream, and that a stream
// failing for another reason is not.
func TestStreamRefusedByPeerCounted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	blocked := p2p.NewSwarmStreamName(testProtocolName, testProtocolVersion, testStreamName)

	s1, overlay1 := newService(t, 1, libp2pServiceOpts{libp2pOpts: libp2p.WithBlockedInboundProtocol(libp2p.Options{
		FullNode: true,
	}, blocked)})
	s2, _ := newService(t, 1, libp2pServiceOpts{})

	if err := s1.AddProtocol(newTestProtocol(func(_ context.Context, _ p2p.Peer, _ p2p.Stream) error {
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Connect(ctx, serviceUnderlayAddress(t, s1)); err != nil {
		t.Fatal(err)
	}

	label := testProtocolName + "/" + testProtocolVersion
	if _, err := s2.NewStream(ctx, overlay1, nil, testProtocolName, testProtocolVersion, testStreamName); err == nil {
		t.Fatal("stream to a protocol the peer refuses opened")
	}
	if got := s2.StreamsRefusedByPeer(label, testStreamName); got != 1 {
		t.Fatalf("got %v refusals counted, want 1", got)
	}

	// A protocol the peer does not offer fails at negotiation, not at the
	// peer's resource limit, and is not counted.
	if _, err := s2.NewStream(ctx, overlay1, nil, "unknown", "1.0.0", "unknown"); err == nil {
		t.Fatal("stream to an unknown protocol opened")
	}
	if got := s2.StreamsRefusedByPeer("unknown/1.0.0", "unknown"); got != 0 {
		t.Fatalf("got %v refusals counted for an unknown protocol, want 0", got)
	}
	if got := s2.StreamsRefusedByPeer(label, testStreamName); got != 1 {
		t.Fatalf("got %v refusals counted after the unknown protocol, want 1", got)
	}
}
