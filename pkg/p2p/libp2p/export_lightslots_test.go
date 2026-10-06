// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	libp2prate "github.com/libp2p/go-libp2p/x/rate"
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

var UltraLightNodeLimit = ultraLightNodeLimit

// PerIPLimits returns the per-IP limits built for the configured values:
// the IPv4 and IPv6 connection counts with their prefix lengths, and the
// rate limiter.
func PerIPLimits(maxConns int, rate float64, burst int) (ipv4, ipv6 []rcmgr.ConnLimitPerSubnet, limiter *libp2prate.Limiter) {
	l := buildPerIPLimits(maxConns, rate, burst)
	return l.ipv4Conns, l.ipv6Conns, l.rate
}
