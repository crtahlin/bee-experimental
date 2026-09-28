// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node

import (
	"context"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/transaction"
)

// The node turns blocks into time in three places: the stamp TTL the API
// reports, how often the storage-incentives agent checks for a phase change,
// and how long the postage listener sleeps. Each reads the block time from
// blockTimeFunc, so a change in the chain's block time, such as Gnosis Chain's
// move from 5 s to 2 s blocks, is followed without a restart (#540).

const (
	// blockTimeDriftTolerance is how far, as a fraction, the observed block
	// time may differ from the configured one before the operator is warned.
	blockTimeDriftTolerance = 0.2
	// blockTimeDriftPeriod is how long the difference must last before the
	// warning, so one noisy measurement does not produce it.
	blockTimeDriftPeriod = 10 * time.Minute
	// blockTimeCheckInterval is how often the difference is checked.
	blockTimeCheckInterval = time.Minute
)

// blockTimeFunc returns the block time the node uses. An explicitly set
// block-time wins, exactly as before. Otherwise the configured value is only
// the starting point, used until the backend has measured the chain.
func blockTimeFunc(backend transaction.Backend, configured time.Duration, explicit bool) func() time.Duration {
	if explicit {
		return func() time.Duration { return configured }
	}
	return transaction.ObservedBlockTime(backend, configured)
}

// blockTimeDrift decides when to warn that the observed block time differs
// from the configured one. It warns once, after the difference has lasted
// blockTimeDriftPeriod.
type blockTimeDrift struct {
	configured time.Duration
	since      time.Time // when the current difference began, zero if none
	warned     bool
}

// check records one observation and reports whether to warn now. An
// observation of 0 means nothing has been measured yet and is ignored.
func (d *blockTimeDrift) check(now time.Time, observed time.Duration) bool {
	if d.warned || observed <= 0 || d.configured <= 0 {
		return false
	}

	diff := float64(observed-d.configured) / float64(d.configured)
	if diff < 0 {
		diff = -diff
	}
	if diff <= blockTimeDriftTolerance {
		d.since = time.Time{}
		return false
	}

	if d.since.IsZero() {
		d.since = now
		return false
	}
	if now.Sub(d.since) < blockTimeDriftPeriod {
		return false
	}

	d.warned = true
	return true
}

// watchBlockTime warns once when the chain's observed block time has differed
// from the configured one for blockTimeDriftPeriod. It returns when ctx ends.
func watchBlockTime(ctx context.Context, logger log.Logger, backend transaction.Backend, configured time.Duration, explicit bool) {
	timer, ok := backend.(transaction.BlockTimer)
	if !ok {
		return
	}

	drift := &blockTimeDrift{configured: configured}
	ticker := time.NewTicker(blockTimeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			observed := timer.AverageBlockTime()
			if !drift.check(now, observed) {
				continue
			}
			if explicit {
				logger.Warning("the chain's observed block time differs from the block-time setting, which is used because it is set explicitly",
					"observed", observed, "configured", configured)
			} else {
				logger.Warning("the chain's observed block time differs from the default for this network; the observed value is used",
					"observed", observed, "default", configured)
			}
			return
		}
	}
}
