// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package retrieval

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// debitChecker asks accounting, before serving, whether debiting a peer for a
// request would put it past the point where the debit disconnects it (#596).
//
// Declared here rather than added to accounting.Interface so that the next
// upstream sync sees an added file and an added method on the concrete type,
// not a changed interface that every implementation has to follow. An
// accounting that does not have the method, such as the mock, is not asked,
// and the handler behaves as it did before.
type debitChecker interface {
	// CanDebit reports whether debiting peer by price now would stay below
	// its disconnect limit. It reserves nothing, so the answer is advisory.
	CanDebit(ctx context.Context, peer swarm.Address, price uint64) bool
}

// errLightPeerDebtLimit is returned to a light peer whose request would put
// it past its disconnect limit. The handler's deferred writer sends it as the
// Err of the delivery. Err is free text, so this changes nothing on the wire.
var errLightPeerDebtLimit = errors.New("debt limit reached, retry later or ask another node")

// checkLightPeerDebit refuses a light peer's request before any work is done
// for it when serving it would overdraw the peer.
//
// Without it, the chunk is looked up, possibly forwarded at this node's cost,
// sent, and then debit.Apply finds the overdraw and returns a BlockPeerError,
// which blocklists and disconnects the peer. Many ultra-light clients overdraw
// often and re-dial at once, so each such disconnect is churn. Refused here,
// the peer receives an error delivery and the connection stays.
//
// Only light peers are checked. The light flag is p.FullNode, which the p2p
// layer takes from its peer registry, filled from the handshake; a stream from
// a peer without that entry is reset before any handler runs. The accounting
// record's own copy of the flag is set by Connect, which runs in a goroutine
// and may not have run yet, so it is not used here. Full peers are not
// checked, and their overdraws still disconnect, as in Bee.
//
// No PrepareDebit is made for a refused request, so no Cleanup runs and no
// ghost overdraw is recorded for it.
func (s *Service) checkLightPeerDebit(ctx context.Context, p p2p.Peer, addr swarm.Address) error {
	if p.FullNode {
		return nil
	}
	checker, ok := s.accounting.(debitChecker)
	if !ok {
		return nil
	}
	// The price of the requested address. The debit made after serving
	// prices chunk.Address(), which is the same address.
	if checker.CanDebit(ctx, p.Address, s.pricer.Price(addr)) {
		return nil
	}
	s.metrics.LightPeerDebitRefusals.Inc()
	return fmt.Errorf("light peer %s: %w", p.Address, errLightPeerDebtLimit)
}
