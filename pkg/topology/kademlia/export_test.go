// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kademlia

import (
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology"
	im "github.com/ethersphere/bee/v2/pkg/topology/kademlia/internal/metrics"
	"github.com/ethersphere/bee/v2/pkg/topology/pslice"
	dto "github.com/prometheus/client_model/go"
)

var (
	PruneOversaturatedBinsFunc = func(k *Kad) func(uint8) {
		return k.pruneOversaturatedBins
	}
	GenerateCommonBinPrefixes = generateCommonBinPrefixes
)

// MarkConnectedPeersSeen runs the sweep the manage loop performs on every
// lastSeenRefreshInterval tick.
func (k *Kad) MarkConnectedPeersSeen() error {
	return k.markConnectedPeersSeen()
}

const (
	DefaultBitSuffixLength     = defaultBitSuffixLength
	DefaultSaturationPeers     = defaultSaturationPeers
	DefaultOverSaturationPeers = defaultOverSaturationPeers
	MaxConnAttempts            = maxConnAttempts
)

type (
	PeerExcludeFunc = peerExcludeFunc
	ExcludeFunc     = excludeFunc
)

func (k *Kad) IsWithinConnectionDepth(addr swarm.Address) bool {
	return swarm.Proximity(k.base.Bytes(), addr.Bytes()) >= k.ConnectionDepth()
}

func (k *Kad) ConnectionDepth() uint8 {
	k.depthMu.RLock()
	defer k.depthMu.RUnlock()
	return k.depth
}

func (k *Kad) StorageRadius() uint8 {
	k.depthMu.RLock()
	defer k.depthMu.RUnlock()
	return k.storageRadius
}

// IsBalanced returns if Kademlia is balanced to bin.
func (k *Kad) IsBalanced(bin uint8) bool {
	if int(bin) >= len(k.commonBinPrefixes) {
		return false
	}

	// for each pseudo address
	for i := range k.commonBinPrefixes[bin] {
		pseudoAddr := k.commonBinPrefixes[bin][i]
		closestConnectedPeer, err := closestPeer(k.connectedPeers, pseudoAddr)
		if err != nil {
			return false
		}

		closestConnectedPO := swarm.ExtendedProximity(closestConnectedPeer.Bytes(), pseudoAddr.Bytes())
		if int(closestConnectedPO) < int(bin)+k.opt.BitSuffixLength+1 {
			return false
		}
	}

	return true
}

func closestPeer(peers *pslice.PSlice, addr swarm.Address) (swarm.Address, error) {
	closest := swarm.ZeroAddress
	err := peers.EachBinRev(func(peer swarm.Address, po uint8) (bool, bool, error) {
		if closest.IsZero() {
			closest = peer
			return false, false, nil
		}

		closer, err := peer.Closer(addr, closest)
		if err != nil {
			return false, false, err
		}
		if closer {
			closest = peer
		}
		return false, false, nil
	})
	if err != nil {
		return closest, err
	}

	// check if found
	if closest.IsZero() {
		return closest, topology.ErrNotFound
	}

	return closest, nil
}

func (k *Kad) Trigger() {
	k.manageC <- struct{}{}
}

// MarkAsBootnode marks the given address as a bootnode in the metrics
// collector, mirroring what the production connectBootNodes path does. Used by
// tests to assert iterator filtering behavior.
func (k *Kad) MarkAsBootnode(addr swarm.Address) {
	k.collector.Record(addr, im.IsBootnode(true))
}

// setDuration sets a package duration for one test and restores it when the
// test ends. The durations are read in New, so call it before New, and do
// not use it in a test that calls t.Parallel.
func setDuration(t *testing.T, v *time.Duration, d time.Duration) {
	t.Helper()
	old := *v
	*v = d
	t.Cleanup(func() { *v = old })
}

// SetStableConnection sets how long a connection must last to count as
// stable, for one test.
func SetStableConnection(t *testing.T, d time.Duration) {
	t.Helper()
	setDuration(t, &stableConnection, d)
}

// SetMaxShortLivedBackoff sets the cap on the wait after short-lived
// connections, for one test.
func SetMaxShortLivedBackoff(t *testing.T, d time.Duration) {
	t.Helper()
	setDuration(t, &maxShortLivedBackoff, d)
}

// SetPrunedEntryTTL sets how long a pruned peer's reduced retry entry is
// kept, for one test.
func SetPrunedEntryTTL(t *testing.T, d time.Duration) {
	t.Helper()
	setDuration(t, &prunedEntryTTL, d)
}

// ShortLivedConnections returns the value of the short-lived connection
// counter.
func (k *Kad) ShortLivedConnections() float64 {
	m := &dto.Metric{}
	if err := k.metrics.ShortLivedConnections.Write(m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// RetryWaitShortLived returns the short-lived count kept for a peer.
func (k *Kad) RetryWaitShortLived(addr swarm.Address) int { return k.waitNext.ShortLived(addr) }

// RetryWaitAttempts returns the failed-dial count kept for a peer.
func (k *Kad) RetryWaitAttempts(addr swarm.Address) int { return k.waitNext.Attempts(addr) }

// RetryWaitTryAfter returns when a peer may be dialled again.
func (k *Kad) RetryWaitTryAfter(addr swarm.Address) (time.Time, bool) {
	return k.waitNext.TryAfter(addr)
}
