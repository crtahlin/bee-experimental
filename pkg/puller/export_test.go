// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller

import (
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	dto "github.com/prometheus/client_model/go"
)

// SyncWorkers returns the number of running sync workers, from the
// SyncWorkerCounter gauge.
func (p *Puller) SyncWorkers() float64 {
	var m dto.Metric
	if err := p.metrics.SyncWorkerCounter.Write(&m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

// SetRetryBackoff sets the wait bounds after a failed pull-sync call. Call it
// before Start.
func (p *Puller) SetRetryBackoff(base, maxWait time.Duration) {
	p.retryBackoffBase = base
	p.retryBackoffMax = maxWait
}

// RetryBackoff returns the wait after n consecutive failed calls.
func (p *Puller) RetryBackoff(n int) time.Duration { return p.retryBackoff(n) }

var PeerIntervalKey = peerIntervalKey

func (p *Puller) IsSyncing(addr swarm.Address) bool {
	p.syncPeersMtx.Lock()
	defer p.syncPeersMtx.Unlock()
	_, ok := p.syncPeers[addr.ByteString()]
	return ok
}

func (p *Puller) IsBinSyncing(addr swarm.Address, bin uint8) bool {
	p.syncPeersMtx.Lock()
	defer p.syncPeersMtx.Unlock()
	if peer, ok := p.syncPeers[addr.ByteString()]; ok {
		return peer.isBinSyncing(bin)
	}
	return false
}

// SetCursorsTimeout sets the cursors request deadline. Call it before Start.
func (p *Puller) SetCursorsTimeout(d time.Duration) { p.cursorsTimeout = d }

// SetBlockedRunWatch sets the blocked-run threshold and the watcher interval.
// Call it before Start.
func (p *Puller) SetBlockedRunWatch(threshold, interval time.Duration) {
	p.blockedRunThreshold = threshold
	p.runWatchInterval = interval
}

// CursorRequestsFailed returns the failed cursors requests counted for reason.
func (p *Puller) CursorRequestsFailed(reason string) float64 {
	var m dto.Metric
	if err := p.metrics.CursorRequestsFailed.WithLabelValues(reason).Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// OnChangeStarted returns the start-time gauge of the run in progress.
func (p *Puller) OnChangeStarted() float64 {
	var m dto.Metric
	if err := p.metrics.OnChangeStarted.Write(&m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

// OnChangeDurationCount returns how many completed runs were observed.
func (p *Puller) OnChangeDurationCount() uint64 {
	var m dto.Metric
	if err := p.metrics.OnChangeDuration.Write(&m); err != nil {
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}

const (
	CursorFailTimeout = cursorFailTimeout
	CursorFailError   = cursorFailError
)

// PruneRunDumps exposes pruneRunDumps.
var PruneRunDumps = pruneRunDumps

// NoProgress returns the sync calls counted with no error and no progress.
func (p *Puller) NoProgress() float64 {
	var m dto.Metric
	if err := p.metrics.NoProgress.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}
