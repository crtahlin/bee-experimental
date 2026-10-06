// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package retrieval

import (
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/ethersphere/bee/v2/pkg/metrics"
)

type metrics struct {
	// all metrics fields must be exported
	// to be able to return them by Metrics()
	// using reflection

	RequestCounter        prometheus.Counter
	RequestSuccessCounter prometheus.Counter
	RequestFailureCounter prometheus.Counter
	RequestDurationTime   prometheus.Histogram
	RequestAttempts       prometheus.Histogram
	PeerRequestCounter    prometheus.Counter
	TotalRetrieved        prometheus.Counter
	InvalidChunkRetrieved prometheus.Counter
	ChunkPrice            prometheus.Summary
	TotalErrors           prometheus.Counter
	ChunkRetrieveTime     prometheus.Histogram

	// content providers (wasp)
	PreferredAttempts prometheus.Counter
	PreferredHits     prometheus.Counter
	PreferredMisses   prometheus.Counter
	// PreferredOverdrafts counts preferred attempts refused for credit. Until
	// #324 this branch incremented nothing, so an operator could not see a
	// provider being refused; accounting_blocks_count is node-wide and cannot
	// attribute a refusal to one peer.
	PreferredOverdrafts prometheus.Counter
	// PreferredCandidatesSelected counts flights that began with at least one
	// preferred candidate, once per flight rather than per attempt.
	//
	// It exists because no other counter can answer whether the preferred set
	// reached a fetch at all. PreferredAttempts increments only after
	// prepareCredit succeeds, so it is capped by credit rather than by whether
	// the set was present; and attempts plus overdrafts is inflated by the #324
	// readmit path, which keeps a peer without consuming the candidate, so one
	// refusal can be counted up to nine times. See issue #299.
	PreferredCandidatesSelected prometheus.Counter
	// PreferredReadmits counts preferred peers kept for a later attempt after
	// an overdraft, rather than dropped for that chunk.
	PreferredReadmits prometheus.Counter
	// PreferredRebuilds counts REBUILDS that added a peer, not flights. One
	// flight can raise it more than once, up to the per-chunk cap, so it must
	// not be divided by a flight count. Measured at two for a single flight
	// over a six-peer set. See issue #435.
	//
	// No existing counter can show this path: PreferredCandidatesSelected is
	// once per flight and fires on the first build, and PreferredAttempts is
	// capped by credit. Without it an operator cannot tell a chunk served by
	// a provider that connected late from one served by a provider that was
	// connected all along, which is the whole difference the fix makes.
	PreferredRebuilds prometheus.Counter
	LocalOnlyMisses   prometheus.Counter
	LocalOnlyLimited  prometheus.Counter
	// LightPeerDebitRefusals counts light-peer requests refused before any
	// work because serving them would have overdrawn the peer. Most are a
	// disconnect that did not happen. Some, in the second in which the peer's
	// allowance was refreshed, are requests the old path would have served,
	// because the check then allows no refresh headroom (#603). See wasp #596.
	LightPeerDebitRefusals prometheus.Counter
}

func newMetrics() metrics {
	subsystem := "retrieval"

	return metrics{
		RequestCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "request_count",
			Help:      "Number of requests to retrieve chunks.",
		}),
		RequestSuccessCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "request_success_count",
			Help:      "Number of requests which succeeded to retrieve chunk.",
		}),
		RequestFailureCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "request_failure_count",
			Help:      "Number of requests which failed to retrieve chunk.",
		}),
		RequestDurationTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "request_duration_time",
			Help:      "Histogram for time taken to complete retrieval request",
		}),
		RequestAttempts: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "request_attempts",
			Help:      "Histogram for total retrieval attempts pre each request.",
		}),
		PeerRequestCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "peer_request_count",
			Help:      "Number of request to single peer.",
		}),
		TotalRetrieved: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_retrieved",
			Help:      "Total chunks retrieved.",
		}),
		InvalidChunkRetrieved: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "invalid_chunk_retrieved",
			Help:      "Invalid chunk retrieved from peer.",
		}),
		ChunkPrice: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunk_price",
			Help:      "The price of the chunk that was paid.",
		}),
		TotalErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "total_errors",
			Help:      "Total number of errors while retrieving chunk.",
		}),
		ChunkRetrieveTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "retrieve_chunk_time",
			Help:      "Histogram for time taken to retrieve a chunk.",
		},
		),
		PreferredAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "preferred_attempts",
			Help:      "Retrieval attempts sent to a preferred peer with the local-only header.",
		}),
		PreferredHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "preferred_hits",
			Help:      "Chunks delivered by a preferred peer.",
		}),
		PreferredMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "preferred_misses",
			Help:      "Preferred attempts that did not deliver the chunk.",
		}),
		PreferredOverdrafts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "preferred_overdrafts",
			Help:      "Preferred attempts refused because the peer could not be credited.",
		}),
		PreferredCandidatesSelected: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "preferred_candidates_selected",
			Help:      "Flights that began with at least one preferred candidate, counted once per flight.",
		}),
		PreferredReadmits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "preferred_readmits",
			Help:      "Preferred peers kept for a later attempt after an overdraft.",
		}),
		PreferredRebuilds: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "preferred_rebuilds",
			Help:      "Rebuilds of a preferred candidate list that found a peer not already offered.",
		}),
		LocalOnlyMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "local_only_misses",
			Help:      "Local-only requests from peers answered with a miss.",
		}),
		LocalOnlyLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "local_only_limited",
			Help:      "Local-only misses answered with the limit error.",
		}),
		LightPeerDebitRefusals: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "light_peer_debit_refusals",
			Help:      "Light-peer requests refused before serving because the debit would have passed the peer's disconnect limit.",
		}),
	}
}

func (s *Service) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}

// StatusMetrics exposes metrics that are exposed on the status protocol.
func (s *Service) StatusMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		s.metrics.RequestAttempts,
		s.metrics.ChunkRetrieveTime,
		s.metrics.RequestDurationTime,
	}
}
