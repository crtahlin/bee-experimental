// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"time"

	"github.com/ethersphere/bee/v2/pkg/postage"
)

// radiusDecreaseHold is how long after the batch store stops being stale the
// reserve worker still skips the radius decrease: one sync-rate window, so a
// rate that is still 0 from the pause cannot lower the radius (#583).
var radiusDecreaseHold = 15 * time.Minute

// SetPostageSyncHealth gives the reserve worker the postage listener's sync
// health (#583). While the batch store is stale, and for one sync-rate window
// after, the worker does not lower the storage radius: the reserve size is not
// reliable while chunks are held outside it, and the sync rate is not either
// while pulled chunks are held or retried. Call it before StartReserveWorker. A nil health keeps
// today's behaviour.
func (db *DB) SetPostageSyncHealth(h postage.SyncHealth) {
	if h == nil {
		return
	}
	db.postageSyncHealth.Store(&h)
}

// radiusDecreaseBlocked reports whether the reserve worker must skip the
// radius decrease now.
func (db *DB) radiusDecreaseBlocked() bool {
	p := db.postageSyncHealth.Load()
	if p == nil {
		return false
	}
	h := *p
	if h.Stale() {
		return true
	}
	ended := h.StaleEndedAt()
	return !ended.IsZero() && time.Since(ended) < radiusDecreaseHold
}
