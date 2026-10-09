// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	dto "github.com/prometheus/client_model/go"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) lines(substr string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, l := range strings.Split(s.b.String(), "\n") {
		if strings.Contains(l, substr) {
			out = append(out, l)
		}
	}
	return out
}

func gaugeOf(t *testing.T, g interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

type registryConn struct {
	network.Conn
	peer libp2ppeer.ID
}

func (c *registryConn) RemotePeer() libp2ppeer.ID { return c.peer }

func testPeerID(t *testing.T, i int) libp2ppeer.ID {
	t.Helper()
	return libp2ppeer.ID(fmt.Sprintf("test-peer-%02d", i))
}

func newTestWatch(t *testing.T, identify peerIdentifier, now *time.Time) (*streamWatch, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	w := &streamWatch{
		identify: identify,
		logger:   log.NewLogger("test", log.WithSink(logs), log.WithSynchronousSink(), log.WithVerbosity(log.VerbosityDebug)).Build(),
		metrics:  newMetrics(),
		now:      func() time.Time { return *now },
		logged:   make(map[libp2ppeer.ID]time.Time),
	}
	return w, logs
}

// TestStreamWatchGauges checks the largest per-peer count and the number
// of peers above each threshold.
func TestStreamWatchGauges(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_000_000, 0)
	w, _ := newTestWatch(t, func(libp2ppeer.ID) (swarm.Address, []byte, bool, bool) {
		return swarm.ZeroAddress, nil, false, false
	}, &now)

	w.report(map[libp2ppeer.ID]network.ScopeStat{
		testPeerID(t, 1): {NumStreamsInbound: 3},
		testPeerID(t, 2): {NumStreamsInbound: 70},
	})

	if got := gaugeOf(t, w.metrics.InboundStreamsPerPeerMax); got != 70 {
		t.Fatalf("got max %v, want 70", got)
	}
	for threshold, want := range map[string]float64{"64": 1, "256": 0, "1000": 0} {
		if got := gaugeOf(t, w.metrics.InboundStreamPeersOver.WithLabelValues(threshold)); got != want {
			t.Fatalf("threshold %s: got %v peers, want %v", threshold, got, want)
		}
	}

	w.report(map[libp2ppeer.ID]network.ScopeStat{})
	if got := gaugeOf(t, w.metrics.InboundStreamsPerPeerMax); got != 0 {
		t.Fatalf("got max %v with no peers, want 0", got)
	}
}

// TestStreamWatchNamesPeers checks the info line for a full peer, a light
// peer and a peer that is not in the registry, the once-per-hour limit,
// and the debug lines for the top three peers.
func TestStreamWatchNamesPeers(t *testing.T) {
	t.Parallel()

	full, light, unknown, small := testPeerID(t, 1), testPeerID(t, 2), testPeerID(t, 3), testPeerID(t, 4)
	fullOverlay, lightOverlay := swarm.RandAddress(t), swarm.RandAddress(t)
	fullEth := common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()
	lightEth := common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes()

	identify := func(p libp2ppeer.ID) (swarm.Address, []byte, bool, bool) {
		switch p {
		case full:
			return fullOverlay, fullEth, true, true
		case light:
			return lightOverlay, lightEth, false, true
		}
		return swarm.ZeroAddress, nil, false, false
	}

	now := time.Unix(1_000_000, 0)
	w, logs := newTestWatch(t, identify, &now)
	stats := map[libp2ppeer.ID]network.ScopeStat{
		full:    {NumStreamsInbound: 4500},
		light:   {NumStreamsInbound: 700},
		unknown: {NumStreamsInbound: 300},
		small:   {NumStreamsInbound: 30},
	}

	w.report(stats)
	info := logs.lines("peer holds many inbound streams")
	if len(info) != 3 {
		t.Fatalf("got %d info lines, want 3:\n%s", len(info), strings.Join(info, "\n"))
	}
	assertLine(t, info, full.String(), fullOverlay.String(), common.BytesToAddress(fullEth).Hex(), `"full_node"=true`, `"inbound_streams"=4500`)
	assertLine(t, info, light.String(), lightOverlay.String(), common.BytesToAddress(lightEth).Hex(), `"full_node"=false`, `"inbound_streams"=700`)
	assertLine(t, info, unknown.String(), `"peer_address"=""`, `"ethereum_address"=""`, `"inbound_streams"=300`)
	for _, l := range info {
		if strings.Contains(l, small.String()) {
			t.Fatalf("peer below the threshold named: %s", l)
		}
	}
	if got := len(logs.lines(`"msg"="peer inbound streams"`)); got != 3 {
		t.Fatalf("got %d debug lines for the top peers, want 3", got)
	}

	// Within the hour the same peers are not named again.
	now = now.Add(59 * time.Minute)
	w.report(stats)
	if got := len(logs.lines("peer holds many inbound streams")); got != 3 {
		t.Fatalf("got %d info lines within the hour, want 3", got)
	}

	// After the hour they are.
	now = now.Add(2 * time.Minute)
	w.report(stats)
	if got := len(logs.lines("peer holds many inbound streams")); got != 6 {
		t.Fatalf("got %d info lines after the hour, want 6", got)
	}
}

// TestStreamWatchForgetsAbsentPeers checks that a peer absent from a scan
// is named again as soon as it returns above the threshold.
func TestStreamWatchForgetsAbsentPeers(t *testing.T) {
	t.Parallel()

	p := testPeerID(t, 1)
	now := time.Unix(1_000_000, 0)
	w, logs := newTestWatch(t, func(libp2ppeer.ID) (swarm.Address, []byte, bool, bool) {
		return swarm.ZeroAddress, nil, false, false
	}, &now)

	w.report(map[libp2ppeer.ID]network.ScopeStat{p: {NumStreamsInbound: 500}})
	w.report(map[libp2ppeer.ID]network.ScopeStat{})
	if len(w.logged) != 0 {
		t.Fatalf("got %d remembered peers after an empty scan, want 0", len(w.logged))
	}
	w.report(map[libp2ppeer.ID]network.ScopeStat{p: {NumStreamsInbound: 500}})
	if got := len(logs.lines("peer holds many inbound streams")); got != 2 {
		t.Fatalf("got %d info lines, want 2", got)
	}
}

func assertLine(t *testing.T, lines []string, parts ...string) {
	t.Helper()
	for _, l := range lines {
		ok := true
		for _, p := range parts {
			if !strings.Contains(l, p) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
	}
	t.Fatalf("no line contains %q in:\n%s", parts, strings.Join(lines, "\n"))
}

// TestPeerRegistryIdentity checks that the registry returns the overlay,
// the Ethereum address and the full/light flag, for full and light peers,
// and nothing after the peer disconnects.
func TestPeerRegistryIdentity(t *testing.T) {
	t.Parallel()

	r := newPeerRegistry()
	r.setDisconnecter(noopDisconnecter{})

	cases := []struct {
		peer    libp2ppeer.ID
		overlay swarm.Address
		eth     []byte
		full    bool
	}{
		{testPeerID(t, 1), swarm.RandAddress(t), common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes(), true},
		{testPeerID(t, 2), swarm.RandAddress(t), common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes(), false},
	}
	conns := make([]*registryConn, len(cases))
	for i, c := range cases {
		conns[i] = &registryConn{peer: c.peer}
		if exists := r.addIfNotExists(conns[i], c.overlay, c.full, c.eth); exists {
			t.Fatalf("peer %d reported as existing", i)
		}
	}

	for i, c := range cases {
		overlay, eth, full, found := r.peerIdentity(c.peer)
		if !found || !overlay.Equal(c.overlay) || !bytes.Equal(eth, c.eth) || full != c.full {
			t.Fatalf("peer %d: got %v %x %v %v, want %v %x %v true", i, overlay, eth, full, found, c.overlay, c.eth, c.full)
		}
	}

	if _, _, _, found := r.peerIdentity(testPeerID(t, 9)); found {
		t.Fatal("unknown peer found")
	}

	r.Disconnected(nil, conns[1])
	if _, eth, _, found := r.peerIdentity(cases[1].peer); found || eth != nil {
		t.Fatalf("disconnected peer still found (found %v, eth %x)", found, eth)
	}
	if _, ok := r.ethAddresses[cases[1].peer]; ok {
		t.Fatal("Ethereum address of a disconnected peer kept")
	}
}

// TestIsRemoteStreamLimit checks which errors count as a peer refusing a
// stream at its resource limit, through the wrapping NewStream adds.
func TestIsRemoteStreamLimit(t *testing.T) {
	t.Parallel()

	wrap := func(err error) error {
		return fmt.Errorf("send headers: %w", fmt.Errorf("read message: %w", err))
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"remote limit", wrap(&network.StreamError{ErrorCode: network.StreamResourceLimitExceeded, Remote: true}), true},
		{"local limit", wrap(&network.StreamError{ErrorCode: network.StreamResourceLimitExceeded, Remote: false}), false},
		{"remote other code", wrap(&network.StreamError{ErrorCode: network.StreamProtocolNegotiationFailed, Remote: true}), false},
		{"remote code 0", wrap(&network.StreamError{ErrorCode: 0, Remote: true}), false},
		{"not a stream error", wrap(fmt.Errorf("protocols not supported")), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := isRemoteStreamLimit(c.err); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

type noopDisconnecter struct{}

func (noopDisconnecter) disconnected(swarm.Address) {}
