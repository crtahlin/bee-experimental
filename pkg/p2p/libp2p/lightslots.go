// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"errors"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/topology/lightnode"
)

// Reasons for refusing a light peer, used as the label of the light
// refusal metric.
const (
	// lightRefusalPicker: refused in the handshake, before the signature
	// in the peer's address was checked, because every light slot was
	// used or reserved.
	lightRefusalPicker = "light_limit_picker"
	// lightRefusalReserve: refused after the handshake, because no light
	// slot could be reserved.
	lightRefusalReserve = "light_limit_reserve"
	// lightRefusalUltraLight: refused after the handshake, because no
	// slot for ultra-light peers could be reserved.
	lightRefusalUltraLight = "ultra_light_limit"
)

// lightLimitPicker is the picker the handshake calls. It refuses a light
// peer when every light slot is used or reserved, and passes every other
// decision to the notifier's own Pick. It reserves nothing: it only saves
// the signature check and the rest of the setup for a peer that could not
// get a slot anyway.
type lightLimitPicker struct {
	next p2p.Picker
	s    *Service
}

func (p *lightLimitPicker) Pick(peer p2p.Peer) bool {
	if !peer.FullNode && !p.s.bootnodeMode && p.s.lightNodes.AtLimit() {
		p.s.metrics.LightPeerRefusals.WithLabelValues(lightRefusalPicker).Inc()
		return false
	}
	return p.next.Pick(peer)
}

// lightRefusalReason maps a TryReserve error to its metric label.
func lightRefusalReason(err error) string {
	if errors.Is(err, lightnode.ErrUltraLightNodeLimit) {
		return lightRefusalUltraLight
	}
	return lightRefusalReserve
}
