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

// Historical sync state hooks (#588).

const HistoricalStallBound = historicalStallBound

// HistoricalEntry returns the state label of a peer and bin entry as it is
// stored, without evaluating the stall bound, and whether the entry exists.
func (p *Puller) HistoricalEntry(addr swarm.Address, bin uint8) (string, bool) {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()
	e, ok := p.histEntries[histKey{peer: addr.ByteString(), bin: bin}]
	if !ok {
		return "", false
	}
	return e.label(), true
}

// HistoricalEntries returns how many entries the peer has.
func (p *Puller) HistoricalEntries(addr swarm.Address) int {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()
	n := 0
	for k := range p.histEntries {
		if k.peer == addr.ByteString() {
			n++
		}
	}
	return n
}

// HistoricalEntriesTotal returns the size of the map.
func (p *Puller) HistoricalEntriesTotal() int {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()
	return len(p.histEntries)
}

// PublishedHistoricalRadius returns the published radius, or -1 before
// the first publication.
func (p *Puller) PublishedHistoricalRadius() int {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()
	return p.histRadius
}

// HistoricalBinsMetric reads the historical_bins gauge for a state.
func (p *Puller) HistoricalBinsMetric(state string) float64 {
	var m dto.Metric
	if err := p.metrics.HistoricalBins.WithLabelValues(state).Write(&m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

// HistStartWorker, HistWorkerFinished and HistRemovePeer drive the map the
// way syncPeerBin, a worker and disconnectPeer do, so the token rule can be
// tested without a race between a worker and a recalculation.
func (p *Puller) HistStartWorker(addr swarm.Address, bin uint8) uint64 {
	return p.histStartWorker(addr, bin)
}

func (p *Puller) HistWorkerFinished(addr swarm.Address, bin uint8, token uint64) {
	histWorker{p: p, historical: true, addr: addr, bin: bin, token: token}.finished()
}

func (p *Puller) HistRemovePeer(addr swarm.Address) { p.histRemovePeer(addr) }
