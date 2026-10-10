// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller

import (
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storer"
)

// actedState records the storage radius the puller has acted on and since
// when, for the reserve worker's radius decrease (#696).
type actedState struct {
	mu     sync.Mutex
	ok     bool
	radius uint8
	since  time.Time
}

// ActedOnRadius returns the storage radius the puller has acted on and the
// completion time of the first recalculation that acted on it. ok is false
// until it has acted on any radius. It implements storer.RadiusSyncReporter.
func (p *Puller) ActedOnRadius() (radius uint8, since time.Time, ok bool) {
	p.acted.mu.Lock()
	defer p.acted.mu.Unlock()
	return p.acted.radius, p.acted.since, p.acted.ok
}

// recordActed updates the acted-on state after a recalculation at radius:
// since moves only when the acted radius changes, whether it went down or up.
// Later recalculations at the same radius leave it as it is.
func (p *Puller) recordActed(radius uint8, now time.Time) {
	p.acted.mu.Lock()
	defer p.acted.mu.Unlock()
	if p.acted.ok && p.acted.radius == radius {
		return
	}
	p.acted.ok, p.acted.radius, p.acted.since = true, radius, now
	p.metrics.ActedRadius.Set(float64(radius))
}

// actedOn reports whether, after a recalculation at radius, at least one
// neighbour (a peer with proximity order at or above the radius) has its
// cursors and every bin at or above the radius syncing, started in this run
// or earlier. Must be called under syncPeersMtx, after recalcPeers.
func (p *Puller) actedOn(radius uint8) bool {
	for _, peer := range p.syncPeers {
		if peer.po < radius {
			continue
		}
		if peer.syncingFrom(radius, p.bins) {
			return true
		}
	}
	return false
}

// syncingFrom reports whether the peer has its cursors and every bin from
// radius up to bins syncing.
func (p *syncPeer) syncingFrom(radius, bins uint8) bool {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	if p.cursors == nil {
		return false
	}
	for bin := radius; bin < bins; bin++ {
		if !p.isBinSyncing(bin) {
			return false
		}
	}
	return true
}

// The puller reports the radius it acted on to the reserve worker (#696).
var _ storer.RadiusSyncReporter = (*Puller)(nil)
