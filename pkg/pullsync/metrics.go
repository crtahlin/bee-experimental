// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync

import (
	m "github.com/ethersphere/bee/v2/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	Offered              prometheus.Counter     // number of chunks offered
	Wanted               prometheus.Counter     // number of chunks wanted
	MissingChunks        prometheus.Counter     // number of reserve get errs
	ReceivedZeroAddress  prometheus.Counter     // number of delivered chunks with invalid address
	ReceivedInvalidChunk prometheus.Counter     // number of delivered chunks with invalid address
	Delivered            prometheus.Counter     // number of chunk deliveries
	SentOffered          prometheus.Counter     // number of chunks offered
	SentWanted           prometheus.Counter     // number of chunks wanted
	Sent                 prometheus.Counter     // number of chunks sent
	DuplicateRuid        prometheus.Counter     // number of duplicate RUID requests we got
	LastReceived         *prometheus.CounterVec // last timestamp of the received chunks per bin
	Held                 prometheus.Counter     // chunks held while the batch store is stale (#583)
	RequestsReplaced     *prometheus.CounterVec // waiting requests ended because the peer asked again (#640)
	WaitingRequests      prometheus.Gauge       // requests waiting for their first chunk (#640)
}

func newMetrics() metrics {
	subsystem := "pullsync"

	return metrics{
		Offered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_offered",
			Help:      "Total chunks offered.",
		}),
		Wanted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_wanted",
			Help:      "Total chunks wanted.",
		}),
		MissingChunks: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "missing_chunks",
			Help:      "Total reserve get errors.",
		}),
		ReceivedZeroAddress: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "received_zero_address",
			Help:      "Total chunks delivered with zero address and no chunk data.",
		}),
		Held: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_held",
			Help:      "Delivered chunks held, not dropped, because their stamp names a batch not seen yet while the batch store is stale (#583).",
		}),
		ReceivedInvalidChunk: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "received_invalid_chunks",
			Help:      "Total invalid chunks delivered.",
		}),
		Delivered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_delivered",
			Help:      "Total chunks delivered.",
		}),
		SentOffered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_sent_offered",
			Help:      "Total chunks offered to peers.",
		}),
		SentWanted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_sent_wanted",
			Help:      "Total chunks wanted by peers.",
		}),
		Sent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "chunks_sent",
			Help:      "Total chunks sent.",
		}),
		DuplicateRuid: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "duplicate_ruids",
			Help:      "Total duplicate RUIDs.",
		}),
		LastReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.Namespace,
				Subsystem: subsystem,
				Name:      "last_received",
				Help:      `The last timestamp of the received chunks per bin.`,
			}, []string{"bin"}),
		RequestsReplaced: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: m.Namespace,
				Subsystem: subsystem,
				Name:      "requests_replaced_total",
				Help:      "Inbound requests still waiting for their first chunk that were ended because the same peer asked again for the same bin: reason same_start (same start) or over_cap (more than two waiting for the bin). Counted by the new request. A request ended just as its offer was ready still completes, and is counted (#640).",
			}, []string{"reason"}),
		WaitingRequests: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "waiting_requests",
			Help:      "Inbound requests currently waiting for their first chunk (#640).",
		}),
	}
}

func (s *Syncer) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(s.metrics)
}
