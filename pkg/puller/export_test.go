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
