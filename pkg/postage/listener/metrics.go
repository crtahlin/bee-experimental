// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener

import (
	m "github.com/ethersphere/bee/v2/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	// aggregate events handled
	EventsProcessed prometheus.Counter
	EventErrors     prometheus.Counter
	PagesProcessed  prometheus.Counter

	// individual event counters
	CreatedCounter prometheus.Counter
	TopupCounter   prometheus.Counter
	DepthCounter   prometheus.Counter
	PriceCounter   prometheus.Counter

	// total calls to chain backend
	BackendCalls  prometheus.Counter
	BackendErrors prometheus.Counter

	// processing durations
	PageProcessDuration  prometheus.Counter
	EventProcessDuration prometheus.Counter

	// sync health (#583)
	Stale                prometheus.Gauge
	SecondsSinceProgress prometheus.Gauge
	BlocksBehind         prometheus.Gauge
	PageBlocks           prometheus.Gauge
}

func newMetrics() metrics {
	subsystem := "postage_listener"

	return metrics{
		// aggregate events handled
		EventsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "events_processed",
			Help:      "total events processed",
		}),
		EventErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "event_errors",
			Help:      "total event errors while processing",
		}),
		PagesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "pages_processed",
			Help:      "total pages processed",
		}),

		// individual event counters
		CreatedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "created_events",
			Help:      "total batch created events processed",
		}),

		TopupCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "topup_events",
			Help:      "total batch topup events handled",
		}),

		DepthCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "depth_events",
			Help:      "total batch depth change events handled",
		}),

		PriceCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "price_events",
			Help:      "total price change events handled",
		}),

		// total call
		BackendCalls: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "backend_calls",
			Help:      "total chain backend calls",
		}),
		BackendErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "backend_errors",
			Help:      "total chain backend errors",
		}),

		// processing durations
		PageProcessDuration: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "page_duration",
			Help:      "how long it took to process a page",
		}),

		EventProcessDuration: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "event_duration",
			Help:      "how long it took to process a single event",
		}),
		Stale: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "stale",
			Help:      "1 while the batch store is stale: no page applied for the stall timeout and not caught up since; the node stays up and serves content but does not play the storage lottery",
		}),
		SecondsSinceProgress: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "seconds_since_progress",
			Help:      "seconds since the listener last applied a page",
		}),
		BlocksBehind: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "blocks_behind",
			Help:      "confirmed chain head minus the listener's next block; -1 while unknown, that is, while stale and no block number call has succeeded since",
		}),
		PageBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "page_blocks",
			Help:      "blocks per log query; halves when a log query fails, down to 100, and doubles back after 3 pages applied in a row",
		}),
	}
}

func (l *listener) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(l.metrics)
}
