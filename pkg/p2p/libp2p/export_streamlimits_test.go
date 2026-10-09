// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	DefaultStreamsPerPeer          = defaultStreamsPerPeer
	DefaultStreamReservePullSync   = defaultStreamReservePullSync
	DefaultStreamReserveOther      = defaultStreamReserveOther
	DefaultStreamsTransient        = defaultStreamsTransient
	DefaultStreamsUnnegotiatedPeer = defaultStreamsUnnegotiatedPeer

	StreamLimitPeerUnnegotiated = streamLimitPeerUnnegotiated
	StreamLimitGroupPullSync    = streamLimitGroupPullSync
	StreamLimitGroupOther       = streamLimitGroupOther
	StreamReasonAboveThreshold  = streamReasonAboveThreshold
	StreamReasonPeerLimit       = streamReasonPeerLimit
)

var (
	ErrUnnegotiatedPerPeer = errUnnegotiatedPerPeer
	ErrStreamGroupFull     = errStreamGroupFull
)

// StreamLimitsConfig is the resolved stream limit configuration.
type StreamLimitsConfig struct {
	Enabled      bool
	PerPeer      int
	Transient    int
	Unnegotiated int
	PullSyncMax  int
	OtherMax     int
}

func BuildStreamLimits(o StreamLimitOptions, streamCap int) (StreamLimitsConfig, error) {
	c, err := buildStreamLimits(o, streamCap)
	return StreamLimitsConfig{
		Enabled:      c.enabled,
		PerPeer:      c.perPeer,
		Transient:    c.transient,
		Unnegotiated: c.unnegotiated,
		PullSyncMax:  c.pullSyncMax,
		OtherMax:     c.otherMax,
	}, err
}

// BuiltLimits returns the concrete limits the node builds for o, with
// the node-wide stream counts it uses, so tests can compare them with
// the limits built without stream limits.
func BuiltLimits(o StreamLimitOptions) (rcmgr.ConcreteLimitConfig, error) {
	cfg := baseLimitConfig()
	c, err := buildStreamLimits(o, IncomingStreamCountLimit)
	if err != nil {
		return rcmgr.ConcreteLimitConfig{}, err
	}
	c.apply(&cfg)
	return cfg.Build(rcmgr.InfiniteLimits), nil
}

// StreamLimitStack is the node's resource-manager stack for stream
// limits: the resource manager built from the limits, with the high-water
// reporter, behind the wrapper.
type StreamLimitStack struct {
	RM        network.ResourceManager
	State     rcmgr.ResourceManagerState
	metrics   metrics
	limits    *streamLimits
	high      *streamHighWater
	attr      *streamAttribution
	watch     *streamWatch
	Collector []prometheus.Collector
}

// NewStreamLimitStack builds the stack with a node-wide inbound stream cap
// of streamCap, as libp2p.New does with IncomingStreamCountLimit. Protocols in
// blocked have every inbound stream refused by the resource manager.
func NewStreamLimitStack(o StreamLimitOptions, streamCap int, blocked ...protocol.ID) (*StreamLimitStack, error) {
	sc, err := buildStreamLimits(o, streamCap)
	if err != nil {
		return nil, err
	}
	cfg := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Streams:         rcmgr.LimitVal(2 * streamCap),
			StreamsOutbound: rcmgr.LimitVal(streamCap),
			StreamsInbound:  rcmgr.LimitVal(streamCap),
		},
	}
	for _, p := range blocked {
		if cfg.Protocol == nil {
			cfg.Protocol = make(map[protocol.ID]rcmgr.ResourceLimits)
		}
		cfg.Protocol[p] = rcmgr.ResourceLimits{StreamsInbound: rcmgr.BlockAllLimit}
	}
	sc.apply(&cfg)
	str, err := rcmgr.NewStatsTraceReporter()
	if err != nil {
		return nil, err
	}
	attr := newStreamAttribution()
	high := newStreamHighWater(str, attr)
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(cfg.Build(rcmgr.InfiniteLimits)), rcmgr.WithTraceReporter(high))
	if err != nil {
		return nil, err
	}
	m := newMetrics()
	l := &inboundLimiter{ResourceManager: rm}
	if sc.wrapperNeeded() {
		l.streams = newStreamLimits(sc, m, attr)
	}
	s := &StreamLimitStack{
		RM:      l,
		State:   rm.(rcmgr.ResourceManagerState),
		metrics: m,
		limits:  l.streams,
		high:    high,
		attr:    attr,
	}
	s.watch = &streamWatch{
		state:      s.State,
		identify:   func(libp2ppeer.ID) (swarm.Address, []byte, bool, bool) { return swarm.ZeroAddress, nil, false, false },
		logger:     log.Noop,
		metrics:    m,
		now:        time.Now,
		logged:     make(map[libp2ppeer.ID]time.Time),
		high:       high,
		attr:       attr,
		limits:     l.streams,
		attributed: make(map[libp2ppeer.ID]time.Time),
	}
	return s, nil
}

// Counts returns the wrapper's pull-sync and other group counts and the
// number of peers holding un-negotiated slots.
func (s *StreamLimitStack) Counts() (pullSync, other, unnegotiatedPeers int) {
	if s.limits == nil {
		return 0, 0, 0
	}
	s.limits.mu.Lock()
	defer s.limits.mu.Unlock()
	return s.limits.group[streamGroupPullSync], s.limits.group[streamGroupOther], len(s.limits.unnegotiated)
}

// Refused returns the wrapper's refusal counter for a limit label.
func (s *StreamLimitStack) Refused(limit string) float64 {
	var m dto.Metric
	if err := s.metrics.InboundStreamsRefused.WithLabelValues(limit).Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// Scan runs one stream-watch scan and returns the high-water gauges.
func (s *StreamLimitStack) Scan() (peerHigh, transientHigh, unnegotiatedMax float64) {
	s.watch.scan()
	return gaugeValue(s.metrics.InboundStreamsPerPeerHighWater),
		gaugeValue(s.metrics.InboundStreamsTransientHighWater),
		gaugeValue(s.metrics.InboundStreamsUnnegotiatedPeerMax)
}

// TakeAttribution returns the peers noted for the attribution log since
// the previous call.
func (s *StreamLimitStack) TakeAttribution() map[libp2ppeer.ID]string {
	return s.attr.take()
}
