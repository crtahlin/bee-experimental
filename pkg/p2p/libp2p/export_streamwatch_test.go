// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"github.com/libp2p/go-libp2p/core/protocol"
	dto "github.com/prometheus/client_model/go"
)

// WithBlockedInboundProtocol makes the service's resource manager refuse
// every inbound stream of the protocol, as a peer at its limit does.
func WithBlockedInboundProtocol(o Options, id string) Options {
	o.blockedInboundProtocols = append(o.blockedInboundProtocols, protocol.ID(id))
	return o
}

// StreamsRefusedByPeer reads the refused-by-peer counter for a protocol
// ("name/version") and stream.
func (s *Service) StreamsRefusedByPeer(protocol, stream string) float64 {
	var m dto.Metric
	if err := s.metrics.StreamsRefusedByPeer.WithLabelValues(protocol, stream).Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// ScanStreams runs one scan of the inbound streams per peer and returns
// the largest per-peer inbound count it reported; -1 when the scan is
// disabled.
func (s *Service) ScanStreams() float64 {
	if s.streams == nil {
		return -1
	}
	s.streams.scan()
	var m dto.Metric
	if err := s.metrics.InboundStreamsPerPeerMax.Write(&m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}
