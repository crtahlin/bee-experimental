// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	m "github.com/ethersphere/bee/v2/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection
	CreatedConnectionCount     prometheus.Counter
	HandledConnectionCount     prometheus.Counter
	CreatedStreamCount         prometheus.Counter
	ClosedStreamCount          prometheus.Counter
	StreamResetCount           prometheus.Counter
	HandledStreamCount         prometheus.Counter
	BlocklistedPeerCount       prometheus.Counter
	BlocklistedPeerErrCount    prometheus.Counter
	DisconnectCount            prometheus.Counter
	ConnectBreakerCount        prometheus.Counter
	UnexpectedProtocolReqCount prometheus.Counter
	KickedOutPeersCount        prometheus.Counter
	LightPeerRefusals          *prometheus.CounterVec
	LightAnnouncementsSkipped  prometheus.Counter
	InboundAdmitted            *prometheus.CounterVec
	InboundRefusals            *prometheus.CounterVec
	KnownFullAddresses         prometheus.Gauge
	ReachabilityPublic         prometheus.Gauge
	ReachabilityToPrivate      prometheus.Counter
	StreamHandlerErrResetCount prometheus.Counter
	HeadersExchangeDuration    prometheus.Histogram
	StreamsRefusedByPeer       *prometheus.CounterVec
	InboundStreamsPerPeerMax   prometheus.Gauge
	InboundStreamPeersOver     *prometheus.GaugeVec
}

func newMetrics() metrics {
	subsystem := "libp2p"

	return metrics{
		CreatedConnectionCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "created_connection_count",
			Help:      "Number of initiated outgoing libp2p connections.",
		}),
		HandledConnectionCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handled_connection_count",
			Help:      "Number of handled incoming libp2p connections.",
		}),
		CreatedStreamCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "created_stream_count",
			Help:      "Number of initiated outgoing libp2p streams.",
		}),
		ClosedStreamCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "closed_stream_count",
			Help:      "Number of closed outgoing libp2p streams.",
		}),
		StreamResetCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "stream_reset_count",
			Help:      "Number of outgoing libp2p streams resets.",
		}),
		HandledStreamCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "handled_stream_count",
			Help:      "Number of handled incoming libp2p streams.",
		}),
		BlocklistedPeerCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "blocklisted_peer_count",
			Help:      "Number of peers we've blocklisted.",
		}),
		BlocklistedPeerErrCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "blocklisted_peer_err_count",
			Help:      "Number of peers we've been unable to blocklist.",
		}),
		DisconnectCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "disconnect_count",
			Help:      "Number of peers we've disconnected from (initiated locally).",
		}),
		ConnectBreakerCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "connect_breaker_count",
			Help:      "Number of times we got a closed breaker while connecting to another peer.",
		}),
		UnexpectedProtocolReqCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "unexpected_protocol_request_count",
			Help:      "Number of requests the peer is not expecting.",
		}),
		KickedOutPeersCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "kickedout_peers_count",
			Help:      "Number of total kicked-out peers.",
		}),
		LightPeerRefusals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "light_peer_refusals",
			Help:      "Number of inbound light peers refused at a light limit, by reason.",
		}, []string{"reason"}),
		LightAnnouncementsSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "light_announcements_skipped",
			Help:      "Number of announcements not sent to a light peer because it received one recently.",
		}),
		InboundAdmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "inbound_admitted",
			Help:      "Number of new inbound connections the total inbound rate admitted, by bucket.",
		}, []string{"bucket"}),
		InboundRefusals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "inbound_refusals",
			Help:      "Number of new inbound connections refused before the security handshake because the total inbound rate was used up, by bucket.",
		}, []string{"bucket"}),
		KnownFullAddresses: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "known_full_addresses",
			Help:      "Number of addresses of full peers that completed a handshake with this node, kept for the total inbound rate.",
		}),
		ReachabilityPublic: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "reachability_public",
			Help:      "1 when the node's last reachability event said it is publicly reachable, 0 otherwise.",
		}),
		ReachabilityToPrivate: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "reachability_private_switches",
			Help:      "Number of times the node's reachability changed to private.",
		}),
		StreamHandlerErrResetCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "stream_handler_error_reset_count",
			Help:      "Number of total stream handler error resets.",
		}),
		HeadersExchangeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "headers_exchange_duration",
			Help:      "The duration spent exchanging the headers.",
		}),
		StreamsRefusedByPeer: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "streams_refused_by_peer_total",
			Help:      "Number of streams this node opened that the peer refused at its resource limit (stream reset code 0x1002), by protocol and stream. Peers on older libp2p versions reset with code 0 and are not counted.",
		}, []string{"protocol", "stream"}),
		InboundStreamsPerPeerMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "inbound_streams_per_peer_max",
			Help:      "The largest number of inbound streams one peer holds, from the resource manager, read every 30 s.",
		}),
		InboundStreamPeersOver: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "inbound_stream_peers_over",
			Help:      "Number of peers holding more inbound streams than the threshold, read every 30 s.",
		}, []string{"threshold"}),
	}
}

func (s *Service) Metrics() []prometheus.Collector {
	collectors := append(m.PrometheusCollectorsFromFields(s.metrics), s.handshakeService.Metrics()...)
	if mc, ok := s.reacher.(interface{ Metrics() []prometheus.Collector }); ok {
		collectors = append(collectors, mc.Metrics()...)
	}
	return collectors
}

// StatusMetrics exposes metrics that are exposed on the status protocol.
func (s *Service) StatusMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		s.metrics.HeadersExchangeDuration,
	}
}
