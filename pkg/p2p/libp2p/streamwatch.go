// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

const (
	// streamScanInterval is how often the inbound streams per peer are
	// read from the resource manager. Pull-sync streams live for
	// minutes, so the pattern is visible at this interval.
	streamScanInterval = 30 * time.Second
	// streamLogThreshold is the inbound stream count above which a peer
	// is named in an info line.
	streamLogThreshold = 256
	// streamLogInterval is how often one peer is named at most.
	streamLogInterval = time.Hour
	// streamTopPeers is how many peers each scan names at debug level.
	streamTopPeers = 3
)

// streamPeerThresholds are the inbound stream counts the per-peer gauge
// counts peers above.
var streamPeerThresholds = []int{64, 256, 1000}

// isRemoteStreamLimit reports whether err is a stream reset sent by the
// remote peer because its resource manager refused the stream.
func isRemoteStreamLimit(err error) bool {
	var se *network.StreamError
	return errors.As(err, &se) && se.Remote && se.ErrorCode == network.StreamResourceLimitExceeded
}

// countRemoteStreamRefusal counts err if a peer refused the stream at
// its resource limit, and logs it at debug level.
func (s *Service) countRemoteStreamRefusal(err error, overlay swarm.Address, protocolName, protocolVersion, streamName string) {
	if !isRemoteStreamLimit(err) {
		return
	}
	protocol := protocolName + "/" + protocolVersion
	s.metrics.StreamsRefusedByPeer.WithLabelValues(protocol, streamName).Inc()
	s.logger.Debug("stream refused by peer", "peer_address", overlay, "protocol", protocol, "stream", streamName)
}

// peerIdentifier returns what the registry knows about a peer.
type peerIdentifier func(libp2ppeer.ID) (overlay swarm.Address, ethAddress []byte, full, found bool)

// streamWatch reads the inbound streams per peer from the resource
// manager, sets the per-peer gauges and names peers that hold many.
type streamWatch struct {
	state    rcmgr.ResourceManagerState
	identify peerIdentifier
	logger   log.Logger
	metrics  metrics
	now      func() time.Time
	logged   map[libp2ppeer.ID]time.Time
}

type peerStreams struct {
	peer    libp2ppeer.ID
	inbound int
}

// scan reads the resource manager's per-peer accounting once.
func (w *streamWatch) scan() {
	w.report(w.state.Stat().Peers)
}

// report sets the gauges from per-peer stats and logs the peers above
// the threshold, each at most once per streamLogInterval.
func (w *streamWatch) report(stats map[libp2ppeer.ID]network.ScopeStat) {
	counts := make([]peerStreams, 0, len(stats))
	for p, st := range stats {
		counts = append(counts, peerStreams{peer: p, inbound: st.NumStreamsInbound})
	}
	sort.Slice(counts, func(i, j int) bool { return counts[i].inbound > counts[j].inbound })

	maxInbound := 0
	if len(counts) > 0 {
		maxInbound = counts[0].inbound
	}
	w.metrics.InboundStreamsPerPeerMax.Set(float64(maxInbound))
	for _, t := range streamPeerThresholds {
		n := 0
		for _, c := range counts {
			if c.inbound <= t {
				break
			}
			n++
		}
		w.metrics.InboundStreamPeersOver.WithLabelValues(strconv.Itoa(t)).Set(float64(n))
	}

	now := w.now()
	for p := range w.logged {
		if _, ok := stats[p]; !ok {
			delete(w.logged, p)
		}
	}
	for _, c := range counts {
		if c.inbound <= streamLogThreshold {
			break
		}
		if last, ok := w.logged[c.peer]; ok && now.Sub(last) < streamLogInterval {
			continue
		}
		w.logged[c.peer] = now
		w.logger.Info("peer holds many inbound streams", w.fields(c)...)
	}
	for i := 0; i < len(counts) && i < streamTopPeers; i++ {
		w.logger.Debug("peer inbound streams", append([]any{"rank", i + 1}, w.fields(counts[i])...)...)
	}
}

// fields returns the log fields that name a peer. A peer that is not in
// the registry, whose handshake never finished or which has just
// disconnected, is named by its peer ID only.
func (w *streamWatch) fields(c peerStreams) []any {
	f := []any{"peer_id", c.peer}
	overlay, eth, full, found := w.identify(c.peer)
	if found {
		ethAddress := ""
		if len(eth) > 0 {
			ethAddress = common.BytesToAddress(eth).Hex()
		}
		f = append(f, "peer_address", overlay, "ethereum_address", ethAddress, "full_node", full)
	} else {
		f = append(f, "peer_address", "", "ethereum_address", "", "full_node", "")
	}
	return append(f, "inbound_streams", c.inbound)
}

// streamWatchWorker scans every streamScanInterval until the service
// closes.
func (s *Service) streamWatchWorker(w *streamWatch) {
	t := time.NewTicker(streamScanInterval)
	defer t.Stop()
	for {
		select {
		case <-s.knownFullQuit:
			return
		case <-s.ctx.Done():
			return
		case <-t.C:
			w.scan()
		}
	}
}
