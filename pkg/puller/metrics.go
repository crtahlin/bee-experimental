// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller

import (
	m "github.com/ethersphere/bee/v2/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	SyncWorkerIterCounter prometheus.Counter     // counts the number of syncing iterations
	SyncWorkerCounter     prometheus.Gauge       // count number of syncing jobs
	SyncedCounter         *prometheus.CounterVec // number of synced chunks
	SyncWorkerErrCounter  prometheus.Counter     // count number of errors
	MaxUintErrCounter     prometheus.Counter     // how many times we got maxuint as topmost
	PullsyncRate          prometheus.GaugeFunc   // rate of historical syncing
	OnChangeRuns          prometheus.Counter     // recalculations of the sync peers
	OnChangeStarted       prometheus.Gauge       // start time of the recalculation in progress, 0 if none
	OnChangeDuration      prometheus.Histogram   // duration of completed recalculations
	CursorRequestsFailed  *prometheus.CounterVec // failed cursors requests by reason
}

func newMetrics(pullsyncRate func() float64) metrics {
	subsystem := "puller"

	return metrics{
		SyncWorkerIterCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "worker_iterations",
			Help:      "Total worker iterations.",
		}),
		SyncWorkerCounter: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "worker",
			Help:      "Total active worker jobs.",
		}),
		SyncedCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "synced_chunks",
			Help:      "Total synced chunks.",
		}, []string{"type"}),
		SyncWorkerErrCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "worker_errors",
			Help:      "Total worker errors.",
		}),
		MaxUintErrCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "max_uint_errors",
			Help:      "Total max uint errors.",
		}),
		PullsyncRate: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "pullsync_rate",
			Help:      "Rate of historical syncing in chunks.",
		}, pullsyncRate),
		OnChangeRuns: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "on_change_runs",
			Help:      "Number of times the sync peers were recalculated, after a topology change or on the timer.",
		}),
		OnChangeStarted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "on_change_started_timestamp_seconds",
			Help:      "Unix time the sync-peer recalculation in progress started, 0 when none runs; now minus this is how long a blocked run has run.",
		}),
		OnChangeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "on_change_duration_seconds",
			Help:      "Duration of completed sync-peer recalculations.",
			Buckets:   []float64{0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 900, 1800, 3600},
		}),
		CursorRequestsFailed: cursorRequestsFailed(subsystem),
	}
}

func cursorRequestsFailed(subsystem string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: m.Namespace,
		Subsystem: subsystem,
		Name:      "cursor_requests_failed_total",
		Help:      "Cursors requests to peers that failed, by reason: timeout (no answer within the deadline) or error.",
	}, []string{"reason"})
	c.WithLabelValues(cursorFailTimeout)
	c.WithLabelValues(cursorFailError)
	return c
}

func (p *Puller) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(p.metrics)
}
