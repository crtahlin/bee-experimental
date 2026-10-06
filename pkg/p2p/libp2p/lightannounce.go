// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	// lightAnnounceInterval is how long a light peer that received an
	// announcement gets no new one when it reconnects. A client that
	// reconnects every few seconds would otherwise get the same list of
	// peers each time.
	lightAnnounceInterval = 10 * time.Minute
	// lightAnnounceMaxPeers bounds the number of light peers remembered.
	// The least recently announced peer is forgotten first, and then
	// simply gets its next announcement early.
	lightAnnounceMaxPeers = 10_000
)

// lightAnnounceGate remembers when each light peer last received an
// announcement. It stores the time with each entry and compares it on
// lookup, rather than using a cache that expires entries by itself,
// because such a cache runs a cleanup goroutine that never stops.
type lightAnnounceGate struct {
	cache    *lru.Cache[string, time.Time]
	interval time.Duration
	now      func() time.Time
}

func newLightAnnounceGate(size int, interval time.Duration) *lightAnnounceGate {
	cache, err := lru.New[string, time.Time](size)
	if err != nil {
		// lru.New fails only for a size below one, and the size is a
		// compiled-in constant.
		panic(err)
	}
	return &lightAnnounceGate{cache: cache, interval: interval, now: time.Now}
}

// recent reports whether the peer received an announcement within the
// interval.
func (g *lightAnnounceGate) recent(overlay swarm.Address) bool {
	at, ok := g.cache.Peek(overlay.ByteString())
	return ok && g.now().Sub(at) < g.interval
}

// record notes that the peer has just received an announcement.
func (g *lightAnnounceGate) record(overlay swarm.Address) {
	g.cache.Add(overlay.ByteString(), g.now())
}

func (g *lightAnnounceGate) len() int {
	return g.cache.Len()
}
