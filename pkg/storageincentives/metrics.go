// Copyright 2022 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives

import (
	m "github.com/ethersphere/bee/v2/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	// phase gauge and counter
	CurrentPhase            prometheus.Gauge
	RevealPhase             prometheus.Counter
	CommitPhase             prometheus.Counter
	ClaimPhase              prometheus.Counter
	Winner                  prometheus.Counter
	NeighborhoodSelected    prometheus.Counter
	SampleDuration          prometheus.Gauge
	Round                   prometheus.Gauge
	InsufficientFundsToPlay prometheus.Counter
	SkippedWhileEvicting    prometheus.Counter
	SkippedRadiusIncrease   prometheus.Counter
	PlayedAfterDecrease     prometheus.Counter

	// total calls to chain backend
	BackendCalls  prometheus.Counter
	BackendErrors prometheus.Counter

	// metrics for err processing
	ErrReveal         prometheus.Counter
	ErrCommit         prometheus.Counter
	ErrClaim          prometheus.Counter
	ErrWinner         prometheus.Counter
	ErrCheckIsPlaying prometheus.Counter
	// RoundMismatch counts commits skipped because the round the node computed
	// disagreed with the contract's (#540).
	RoundMismatch prometheus.Counter

	// Counts persisted in the state store across restarts (#725): a stop
	// knows its outcome only after /metrics is gone.
	StopWaitedForReveal prometheus.Gauge
	RevealMissedOnStop  prometheus.Gauge
	RevealAfterRestart  prometheus.Gauge
}

func newMetrics() metrics {
	subsystem := "storageincentives"

	return metrics{
		CurrentPhase: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "current_phase",
			Help:      "Enum value of the current phase.",
		}),
		RevealPhase: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "reveal_phases",
			Help:      "Count of reveal phases entered.",
		}),
		CommitPhase: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "commit_phases",
			Help:      "Count of commit phases entered.",
		}),
		InsufficientFundsToPlay: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "insufficient_funds_to_play",
			Help:      "Count of games skipped due to insufficient balance to participate.",
		}),
		SkippedWhileEvicting: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "skipped_while_evicting",
			Help:      "Selected rounds skipped because the node had been evicting from its reserve for at least a minute (wasp #649).",
		}),
		SkippedRadiusIncrease: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "skipped_radius_increase_total",
			Help:      "Selected rounds skipped because the storage radius increased between reading the round's depth and the end of the sample (wasp #658).",
		}),
		PlayedAfterDecrease: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "played_after_radius_decrease_total",
			Help:      "Selected rounds played although the storage radius decreased while the sample was taken; a decrease cannot change the sampled range (wasp #658).",
		}),
		ClaimPhase: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "claim_phases",
			Help:      "Count of claim phases entered.",
		}),
		Winner: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "winner",
			Help:      "Count of won rounds.",
		}),
		NeighborhoodSelected: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "neighborhood_selected",
			Help:      "Count of the neighborhood being selected.",
		}),
		SampleDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "reserve_sample_duration",
			Help:      "Time taken to produce a reserve sample.",
		}),
		Round: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "round",
			Help:      "Current round calculated from the block height.",
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

		// phase errors
		ErrReveal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "reveal_phase_errors",
			Help:      "total reveal phase errors while processing",
		}),
		RoundMismatch: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "round_mismatch_total",
			Help:      "Number of commits skipped because the node's round disagreed with the contract's.",
		}),
		ErrCommit: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "commit_phase_errors",
			Help:      "total commit phase errors while processing",
		}),
		ErrClaim: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "claim_phase_errors",
			Help:      "total claim phase errors while processing",
		}),
		ErrWinner: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "win_phase_errors",
			Help:      "total win phase while processing",
		}),
		ErrCheckIsPlaying: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "is_playing_errors",
			Help:      "total neighborhood selected errors while processing",
		}),
		StopWaitedForReveal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "stop_waited_for_reveal",
			Help:      "Stops that waited for a pending storage-lottery reveal, over the node's lifetime.",
		}),
		RevealMissedOnStop: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "reveal_missed_on_stop",
			Help:      "Stops whose wait ended without the reveal mined, over the node's lifetime.",
		}),
		RevealAfterRestart: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: m.Namespace,
			Subsystem: subsystem,
			Name:      "reveal_after_restart",
			Help:      "Reveals mined for a commit made before the last start, over the node's lifetime.",
		}),
	}
}

// TODO: register metric
func (a *Agent) Metrics() []prometheus.Collector {
	return m.PrometheusCollectorsFromFields(a.metrics)
}
