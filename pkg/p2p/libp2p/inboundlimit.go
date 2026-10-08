// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"golang.org/x/time/rate"
)

// Defaults of the total inbound connection rate. The rate is three times
// the per-IP rate, so one address, held to the per-IP rate first, can take
// at most a third of a bucket. The burst covers the light peers and the
// inbound full peers that reconnect at once after a restart.
const (
	defaultInboundConnectionRate  = 30.0
	defaultInboundConnectionBurst = 200

	// inboundLimitOff is the setting value that turns the limit off.
	inboundLimitOff = -1
)

// Bounds of the set of known full peers.
const (
	knownFullPeersMax      = 10_000
	knownFullPeerTTL       = 24 * time.Hour
	knownFullSweepInterval = 10 * time.Minute
	knownFullRefreshPeriod = time.Hour
)

// recentRefusalWindow is how close to a refusal a switch to private
// reachability must come for the node to log both together.
const recentRefusalWindow = 5 * time.Minute

// Bucket labels of the inbound connection metrics.
const (
	inboundBucketGeneral   = "general"
	inboundBucketKnownFull = "known_full"
)

var errInboundRateLimited = errors.New("total inbound connection rate exceeded")

// inboundLimitConfig is the total inbound connection rate in effect. A
// disabled config applies no limit.
type inboundLimitConfig struct {
	enabled bool
	rate    float64
	burst   int
}

// buildInboundLimit returns the total inbound connection rate for the
// configured values. Zero uses the default, -1 turns the limit off, and any
// other negative value is an error. A bootnode applies the limit only when
// the operator set either value.
func buildInboundLimit(r float64, burst int, bootnode, set bool) (inboundLimitConfig, error) {
	if r < 0 && r != inboundLimitOff {
		return inboundLimitConfig{}, fmt.Errorf("p2p-inbound-connection-rate: %v is not allowed: use 0 for the default or -1 to turn the limit off", r)
	}
	if burst < 0 && burst != inboundLimitOff {
		return inboundLimitConfig{}, fmt.Errorf("p2p-inbound-connection-burst: %d is not allowed: use 0 for the default or -1 to turn the limit off", burst)
	}
	if bootnode && !set {
		return inboundLimitConfig{}, nil
	}
	if r == inboundLimitOff || burst == inboundLimitOff {
		return inboundLimitConfig{}, nil
	}
	if r == 0 {
		r = defaultInboundConnectionRate
	}
	if burst == 0 {
		burst = defaultInboundConnectionBurst
	}
	return inboundLimitConfig{enabled: true, rate: r, burst: burst}, nil
}

// addressKey returns the unit the inbound limits count by: the IPv4
// address, or the /56 prefix of an IPv6 address. An IPv4 address carried as
// IPv6 gets its IPv4 key.
func addressKey(m ma.Multiaddr) (netip.Prefix, bool) {
	if m == nil {
		return netip.Prefix{}, false
	}
	ip, err := manet.ToIP(m)
	if err != nil {
		return netip.Prefix{}, false
	}
	a, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Prefix{}, false
	}
	a = a.Unmap()
	if a.Is4() {
		return netip.PrefixFrom(a, 32), true
	}
	return netip.PrefixFrom(a, 56).Masked(), true
}

// knownFullPeers is the bounded set of address keys of full peers that
// completed a handshake with this node, each with the time it was last
// seen. The accept path only reads it; writers take mu, so a sweep cannot
// undo a refresh that happens between its read and its removal.
type knownFullPeers struct {
	mu    sync.Mutex
	cache *lru.Cache[netip.Prefix, time.Time]
	ttl   time.Duration
	now   func() time.Time
}

func newKnownFullPeers(size int, ttl time.Duration, now func() time.Time) *knownFullPeers {
	cache, err := lru.New[netip.Prefix, time.Time](size)
	if err != nil {
		// lru.New fails only for a size below one, a programming error.
		panic(err)
	}
	return &knownFullPeers{cache: cache, ttl: ttl, now: now}
}

// contains reports whether key belongs to a full peer seen within the TTL.
// It uses Peek, which takes only the cache's read lock and does not change
// the eviction order.
func (k *knownFullPeers) contains(key netip.Prefix) bool {
	seen, ok := k.cache.Peek(key)
	return ok && k.now().Sub(seen) < k.ttl
}

// record marks the address a full peer is connected from as seen now.
func (k *knownFullPeers) record(m ma.Multiaddr) {
	key, ok := addressKey(m)
	if !ok {
		return
	}
	k.mu.Lock()
	k.cache.Add(key, k.now())
	k.mu.Unlock()
}

// refresh marks the address as seen now only if it is already known, so a
// disconnect never adds a peer that kademlia did not accept.
func (k *knownFullPeers) refresh(m ma.Multiaddr) {
	key, ok := addressKey(m)
	if !ok {
		return
	}
	k.mu.Lock()
	if _, known := k.cache.Peek(key); known {
		k.cache.Add(key, k.now())
	}
	k.mu.Unlock()
}

// sweep removes the keys not seen within the TTL.
func (k *knownFullPeers) sweep() {
	k.mu.Lock()
	defer k.mu.Unlock()
	now := k.now()
	for _, key := range k.cache.Keys() {
		if seen, ok := k.cache.Peek(key); ok && now.Sub(seen) >= k.ttl {
			k.cache.Remove(key)
		}
	}
}

func (k *knownFullPeers) len() int {
	return k.cache.Len()
}

// inboundLimiter wraps the resource manager and adds a total rate of new
// inbound connections, in two token buckets: one for known full peers and
// one for every other address. It runs after the inner resource manager's
// per-IP limits, so one address cannot use up a bucket, and before the
// security handshake, so a refused connection costs almost nothing.
// Outbound connections are passed through unchanged.
type inboundLimiter struct {
	network.ResourceManager

	general   *rate.Limiter
	knownFull *rate.Limiter
	known     *knownFullPeers
	metrics   metrics
	now       func() time.Time

	// limitLoopback applies the buckets to loopback addresses too, which
	// are otherwise exempt. Only tests set it, to reach the limit from
	// services on one machine.
	limitLoopback bool

	// lastRefusal is the time of the last refusal in Unix nanoseconds,
	// zero if none.
	lastRefusal atomic.Int64
}

func newInboundLimiter(inner network.ResourceManager, cfg inboundLimitConfig, known *knownFullPeers, m metrics, now func() time.Time) *inboundLimiter {
	l := &inboundLimiter{
		ResourceManager: inner,
		known:           known,
		metrics:         m,
		now:             now,
	}
	if cfg.enabled {
		l.general = rate.NewLimiter(rate.Limit(cfg.rate), cfg.burst)
		l.knownFull = rate.NewLimiter(rate.Limit(cfg.rate), cfg.burst)
	}
	return l
}

// OpenConnection admits an inbound connection only when the inner resource
// manager admits it and its bucket has a token. Allow never blocks: this
// runs inside each listener's single accept goroutine.
func (l *inboundLimiter) OpenConnection(dir network.Direction, usefd bool, endpoint ma.Multiaddr) (network.ConnManagementScope, error) {
	scope, err := l.ResourceManager.OpenConnection(dir, usefd, endpoint)
	if err != nil || dir != network.DirInbound || l.general == nil {
		return scope, err
	}
	key, ok := addressKey(endpoint)
	if !ok || (key.Addr().IsLoopback() && !l.limitLoopback) {
		return scope, nil
	}
	bucket, label := l.general, inboundBucketGeneral
	if l.known.contains(key) {
		bucket, label = l.knownFull, inboundBucketKnownFull
	}
	if !bucket.AllowN(l.now(), 1) {
		scope.Done()
		l.lastRefusal.Store(l.now().UnixNano())
		l.metrics.InboundRefusals.WithLabelValues(label).Inc()
		return nil, errInboundRateLimited
	}
	l.metrics.InboundAdmitted.WithLabelValues(label).Inc()
	return scope, nil
}

// refusedWithin reports whether a connection was refused within d before
// now, and when.
func (l *inboundLimiter) refusedWithin(d time.Duration) (time.Time, bool) {
	n := l.lastRefusal.Load()
	if n == 0 {
		return time.Time{}, false
	}
	at := time.Unix(0, n)
	return at, l.now().Sub(at) <= d
}

// recordKnownFull marks the address a full peer is connected from as a
// known full peer's.
func (s *Service) recordKnownFull(m ma.Multiaddr) {
	s.knownFull.record(m)
	s.metrics.KnownFullAddresses.Set(float64(s.knownFull.len()))
}

// refreshKnownFull marks a known full peer's address as seen now, when the
// peer disconnects.
func (s *Service) refreshKnownFull(m ma.Multiaddr) {
	s.knownFull.refresh(m)
}

// knownFullWorker removes expired known full peers every sweep interval
// and refreshes the full peers that stay connected every refresh period,
// so a full peer connected for days does not expire.
func (s *Service) knownFullWorker() {
	sweep := time.NewTicker(knownFullSweepInterval)
	defer sweep.Stop()
	refresh := time.NewTicker(knownFullRefreshPeriod)
	defer refresh.Stop()
	for {
		select {
		case <-s.knownFullQuit:
			return
		case <-s.ctx.Done():
			return
		case <-sweep.C:
			s.knownFull.sweep()
			s.metrics.KnownFullAddresses.Set(float64(s.knownFull.len()))
		case <-refresh.C:
			for _, m := range s.peers.fullPeerConnAddrs() {
				s.recordKnownFull(m)
			}
		}
	}
}

// observeReachability records a reachability event in the metrics and, on
// a change to private shortly after the total inbound rate refused
// connections, logs both together so the cause can be seen.
func (s *Service) observeReachability(r network.Reachability) {
	if r == network.ReachabilityPublic {
		s.metrics.ReachabilityPublic.Set(1)
	} else {
		s.metrics.ReachabilityPublic.Set(0)
	}
	if r != network.ReachabilityPrivate {
		return
	}
	s.metrics.ReachabilityToPrivate.Inc()
	if at, ok := privateAfterRefusal(s.inbound); ok {
		s.logger.Warning("node judged itself unreachable shortly after the total inbound connection rate refused connections; its reachability checks may have been refused", "last_refusal", at, "window", recentRefusalWindow)
	}
}

// privateAfterRefusal reports whether the limiter refused a connection
// within the recent-refusal window, and when.
func privateAfterRefusal(l *inboundLimiter) (time.Time, bool) {
	if l == nil {
		return time.Time{}, false
	}
	return l.refusedWithin(recentRefusalWindow)
}
