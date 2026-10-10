// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"errors"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func metricValue(m prometheus.Metric) float64 {
	var d dto.Metric
	if err := m.Write(&d); err != nil {
		return -1
	}
	switch {
	case d.Counter != nil:
		return d.Counter.GetValue()
	case d.Gauge != nil:
		return d.Gauge.GetValue()
	}
	return -1
}

// EvictedChunkCountForTest returns bee_localstore_evicted_count.
func (db *DB) EvictedChunkCountForTest() float64 {
	return metricValue(db.metrics.EvictedChunkCount)
}

// ExpiredChunkCountForTest returns bee_localstore_expired_count.
func (db *DB) ExpiredChunkCountForTest() float64 {
	return metricValue(db.metrics.ExpiredChunkCount)
}

// ReserveSizeGaugeForTest returns the reserve size gauge.
func (db *DB) ReserveSizeGaugeForTest() float64 {
	return metricValue(db.metrics.ReserveSize)
}

// EvictBatchBinForTest evicts like the reserve worker does, with the node's
// eviction hooks, and calls observe after each round with the node's own
// After hook already applied.
func (db *DB) EvictBatchBinForTest(ctx context.Context, batchID []byte, bin uint8, expired bool, observe func(n int)) (int, error) {
	return db.EvictBatchBinWithExpiryForTest(ctx, batchID, bin, expired, nil, observe)
}

// EvictBatchBinWithExpiryForTest is EvictBatchBinForTest with the batch
// expiry channel unreserve passes to the hooks.
func (db *DB) EvictBatchBinWithExpiryForTest(ctx context.Context, batchID []byte, bin uint8, expired bool, expiry <-chan struct{}, observe func(n int)) (int, error) {
	h := db.evictionHooks(ctx, expired, expiry)
	after := h.After
	h.After = func(n int, took time.Duration) {
		after(n, took)
		observe(n)
	}
	return db.reserve.EvictBatchBin(ctx, batchID, int(^uint(0)>>1), bin, h)
}

// ReservePutForTest stores a chunk straight into the reserve, without the
// over-capacity trigger of ReservePutter.
func (db *DB) ReservePutForTest(ctx context.Context, ch swarm.Chunk) error {
	return db.reserve.Put(ctx, ch)
}

// ResetReserveSizeWithinRadiusForTest clears the within-radius count, which
// is a package variable shared by every DB in the process. A test that runs
// a reserve worker on a large reserve sets it for the tests after it.
func ResetReserveSizeWithinRadiusForTest() {
	reserveSizeWithinRadius.Store(0)
}

// IsEvictionExpiry reports whether err is the error a batch expiry ends an
// eviction with.
func IsEvictionExpiry(err error) bool { return errors.Is(err, errEvictionExpiry) }

// SampleStartedForTest registers a running sample as reserveSample does,
// including the wait at the eviction barrier (#649).
func (db *DB) SampleStartedForTest() { db.sampleStarted() }

// SampleDoneForTest ends a sample SampleStartedForTest registered.
func (db *DB) SampleDoneForTest() { db.sampleDone() }

// SetMaxEvictionPauseForTest sets the cap on how long eviction waits for one
// stretch of sampling and returns a function that restores it.
func SetMaxEvictionPauseForTest(d time.Duration) func() {
	old := maxEvictionPause
	maxEvictionPause = d
	return func() { maxEvictionPause = old }
}

// EvictionPausedSecondsForTest returns bee_localstore_eviction_paused_seconds_total.
func (db *DB) EvictionPausedSecondsForTest() float64 {
	return metricValue(db.metrics.EvictionPausedSeconds)
}

// EvictionPauseCappedForTest returns bee_localstore_eviction_pause_capped_total.
func (db *DB) EvictionPauseCappedForTest() float64 {
	return metricValue(db.metrics.EvictionPauseCapped)
}

// EvictionPacedForTest reports whether eviction is paced (#651).
func (db *DB) EvictionPacedForTest() bool { return db.evictionPacer != nil }

// ExpiryRunningSecondsForTest returns the active time of the expiry run in
// progress, as the eviction_expiry_running_seconds gauge does (#663).
func (db *DB) ExpiryRunningSecondsForTest() float64 {
	return db.episode.expiryActiveFor(time.Now()).Seconds()
}

// EvictionExpirySecondsForTest reads eviction_expiry_seconds_total (#663).
func (db *DB) EvictionExpirySecondsForTest() float64 {
	return metricValue(db.metrics.EvictionExpirySeconds)
}

// EpisodeWorkLeftForTest reports what keeps an eviction episode open (#663).
func (db *DB) EpisodeWorkLeftForTest() bool { return db.episodeWorkLeft() }
