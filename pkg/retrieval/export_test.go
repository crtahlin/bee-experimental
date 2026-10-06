// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package retrieval

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/ratelimit"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

func (s *Service) Handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) error {
	return s.handler(ctx, p, stream)
}

func (s *Service) ClosestPeer(addr swarm.Address, skipPeers []swarm.Address, allowUpstream bool) (swarm.Address, error) {
	return s.closestPeer(addr, skipPeers, allowUpstream)
}

var (
	ErrNotHeldLocally = errNotHeldLocally
	ErrLocalOnlyLimit = errLocalOnlyLimit
	DemoteAfterMisses = demoteAfterMisses
	Fingerprint       = fingerprint
)

// ErrSkipped returns the peers on the service-wide error skip list for chunk.
func (s *Service) ErrSkipped(chunk swarm.Address) []swarm.Address {
	return s.errSkip.ChunkPeers(chunk)
}

// SetMissLimiters replaces the local-only miss limiters.
func (s *Service) SetMissLimiters(peer, node *ratelimit.Limiter) {
	s.peerMissLimiter = peer
	s.nodeMissLimiter = node
}

func (p *PreferredSet) Hit(peer swarm.Address)    { p.hit(peer) }
func (p *PreferredSet) Miss(peer swarm.Address)   { p.miss(peer) }
func (p *PreferredSet) Demote(peer swarm.Address) { p.demote(peer) }

func (p *PreferredSet) SetNow(f func() time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.now = f
}

// PreferredCandidatesSelected reports the counter, so a test can assert it
// moves once per flight rather than once per attempt. It fails the test rather
// than returning zero on a read error, because a silent zero would make a
// "this did not move" assertion vacuous. See issue #299.
func (s *Service) PreferredCandidatesSelected(tb testing.TB) float64 {
	tb.Helper()
	var m dto.Metric
	if err := s.metrics.PreferredCandidatesSelected.Write(&m); err != nil {
		tb.Fatalf("reading the counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// PreferredAttemptsForTest reports the preferred-attempt counter, so a test can
// assert that a flight really made more than one attempt before asserting that
// the per-flight counter still moved once.
func (s *Service) PreferredAttemptsForTest(tb testing.TB) float64 {
	tb.Helper()
	var m dto.Metric
	if err := s.metrics.PreferredAttempts.Write(&m); err != nil {
		tb.Fatalf("reading the counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// PreferredRebuildsForTest reports the rebuild counter. It counts rebuilds
// that added a peer, not flights, which is the thing its Help string got wrong
// once and which a test should therefore be able to see. See issue #435.
func (s *Service) PreferredRebuildsForTest(tb testing.TB) float64 {
	tb.Helper()
	var m dto.Metric
	if err := s.metrics.PreferredRebuilds.Write(&m); err != nil {
		tb.Fatalf("reading the counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// MaxPreferredAttemptsForTest exposes the per-chunk cap, so a test can assert
// the rebuild does not raise it without hard-coding the number. See #435.
const MaxPreferredAttemptsForTest = maxPreferredAttempts

// FirstOverdraft exposes the retention clock helper for tests. See issue #392.
func FirstOverdraft(since map[string]time.Time, peer swarm.Address, now time.Time) time.Time {
	return firstOverdraft(since, peer, now)
}

// SetProviderCreditWait sets the retention window on one service. Every test
// that depends on the window sets it rather than reading the shipped constant,
// so a change to that constant, or making it a configuration option, does not
// silently change what the tests assert or how long they take. Per service, so
// parallel tests do not race.
func (s *Service) SetProviderCreditWait(d time.Duration) {
	s.providerWait = d
}

// ErrLightPeerDebtLimit is the refusal a light peer receives when serving it
// would overdraw it. See wasp #596.
var ErrLightPeerDebtLimit = errLightPeerDebtLimit

// LightPeerDebitRefusalsForTest reports the refusal counter. See wasp #596.
func (s *Service) LightPeerDebitRefusalsForTest(tb testing.TB) float64 {
	tb.Helper()
	var m dto.Metric
	if err := s.metrics.LightPeerDebitRefusals.Write(&m); err != nil {
		tb.Fatalf("reading the counter: %v", err)
	}
	return m.GetCounter().GetValue()
}
