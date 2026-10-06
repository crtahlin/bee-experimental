// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	LightRefusalPicker     = lightRefusalPicker
	LightRefusalReserve    = lightRefusalReserve
	LightRefusalUltraLight = lightRefusalUltraLight
)

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// LightPeerRefusals returns how many light peers were refused for reason.
func (s *Service) LightPeerRefusals(reason string) float64 {
	return counterValue(s.metrics.LightPeerRefusals.WithLabelValues(reason))
}

// KickedOutPeers returns how many light peers were evicted.
func (s *Service) KickedOutPeers() float64 {
	return counterValue(s.metrics.KickedOutPeersCount)
}

// LightAnnouncementsSkipped returns how many announcements to light peers
// were skipped.
func (s *Service) LightAnnouncementsSkipped() float64 {
	return counterValue(s.metrics.LightAnnouncementsSkipped)
}

// LightAnnounceGate exposes the announcement gate for tests.
type LightAnnounceGate struct{ g *lightAnnounceGate }

func NewLightAnnounceGate(size int, interval time.Duration, now func() time.Time) LightAnnounceGate {
	g := newLightAnnounceGate(size, interval)
	g.now = now
	return LightAnnounceGate{g: g}
}

func (g LightAnnounceGate) Recent(overlay swarm.Address) bool { return g.g.recent(overlay) }
func (g LightAnnounceGate) Record(overlay swarm.Address)      { g.g.record(overlay) }
func (g LightAnnounceGate) Len() int                          { return g.g.len() }

const (
	LightAnnounceInterval = lightAnnounceInterval
	LightAnnounceMaxPeers = lightAnnounceMaxPeers
)
