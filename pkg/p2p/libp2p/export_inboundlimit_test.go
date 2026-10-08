// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"net/netip"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	DefaultInboundConnectionRate  = defaultInboundConnectionRate
	DefaultInboundConnectionBurst = defaultInboundConnectionBurst
	InboundBucketGeneral          = inboundBucketGeneral
	InboundBucketKnownFull        = inboundBucketKnownFull
	KnownFullPeerTTL              = knownFullPeerTTL
	KnownFullPeersMax             = knownFullPeersMax
	RecentRefusalWindow           = recentRefusalWindow
)

var ErrInboundRateLimited = errInboundRateLimited

func gaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

// InboundLimitConfig is the total inbound connection rate in effect.
type InboundLimitConfig struct {
	Enabled bool
	Rate    float64
	Burst   int
}

func BuildInboundLimit(r float64, burst int, bootnode, set bool) (InboundLimitConfig, error) {
	c, err := buildInboundLimit(r, burst, bootnode, set)
	return InboundLimitConfig{Enabled: c.enabled, Rate: c.rate, Burst: c.burst}, err
}

func AddressKey(m ma.Multiaddr) (netip.Prefix, bool) { return addressKey(m) }

// KnownFullPeers exposes the set of known full peers for tests.
type KnownFullPeers struct{ k *knownFullPeers }

func NewKnownFullPeers(size int, ttl time.Duration, now func() time.Time) KnownFullPeers {
	return KnownFullPeers{k: newKnownFullPeers(size, ttl, now)}
}

func (k KnownFullPeers) Contains(key netip.Prefix) bool { return k.k.contains(key) }
func (k KnownFullPeers) Record(m ma.Multiaddr)          { k.k.record(m) }
func (k KnownFullPeers) Refresh(m ma.Multiaddr)         { k.k.refresh(m) }
func (k KnownFullPeers) Sweep()                         { k.k.sweep() }
func (k KnownFullPeers) Len() int                       { return k.k.len() }

// InboundLimiter exposes the resource-manager wrapper for tests.
type InboundLimiter struct{ l *inboundLimiter }

func NewInboundLimiter(inner network.ResourceManager, r float64, burst int, known KnownFullPeers, now func() time.Time) InboundLimiter {
	cfg := inboundLimitConfig{enabled: r > 0 && burst > 0, rate: r, burst: burst}
	return InboundLimiter{l: newInboundLimiter(inner, cfg, known.k, newMetrics(), now)}
}

func (l InboundLimiter) OpenConnection(dir network.Direction, usefd bool, endpoint ma.Multiaddr) (network.ConnManagementScope, error) {
	return l.l.OpenConnection(dir, usefd, endpoint)
}

func (l InboundLimiter) Admitted(bucket string) float64 {
	return counterValue(l.l.metrics.InboundAdmitted.WithLabelValues(bucket))
}

func (l InboundLimiter) Refused(bucket string) float64 {
	return counterValue(l.l.metrics.InboundRefusals.WithLabelValues(bucket))
}

func (l InboundLimiter) PrivateAfterRefusal() (time.Time, bool) { return privateAfterRefusal(l.l) }

// WithInboundLimitOnLoopback applies the total inbound rate to loopback
// connections, so services on one machine can reach it.
func WithInboundLimitOnLoopback(o Options) Options {
	o.inboundLimitLoopback = true
	return o
}

func (s *Service) InboundRefusals(bucket string) float64 {
	return counterValue(s.metrics.InboundRefusals.WithLabelValues(bucket))
}

func (s *Service) InboundAdmitted(bucket string) float64 {
	return counterValue(s.metrics.InboundAdmitted.WithLabelValues(bucket))
}

func (s *Service) KnownFullAddresses() float64 { return gaugeValue(s.metrics.KnownFullAddresses) }

func (s *Service) HandledConnections() float64 { return counterValue(s.metrics.HandledConnectionCount) }

func (s *Service) ObserveReachability(r network.Reachability) { s.observeReachability(r) }

func (s *Service) ReachabilityPublic() float64 { return gaugeValue(s.metrics.ReachabilityPublic) }

func (s *Service) ReachabilityToPrivate() float64 {
	return counterValue(s.metrics.ReachabilityToPrivate)
}
