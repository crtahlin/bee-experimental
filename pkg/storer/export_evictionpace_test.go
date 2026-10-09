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
