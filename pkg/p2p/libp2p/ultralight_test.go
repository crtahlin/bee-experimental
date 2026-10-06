// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
)

// TestUltraLightNodeLimit checks that an ultra-light peer is refused at
// its own limit, while a light peer with a chequebook is still accepted.
func TestUltraLightNodeLimit(t *testing.T) {
	t.Parallel()

	sf, container := newLightLimitedNode(t, 3, mockNotifier(noopCf, noopDf, true), libp2p.Options{UltraLightNodeLimit: 1})
	addr := serviceUnderlayAddress(t, sf)

	ultra, ultraOverlay := newLightNode(t)
	if _, err := ultra.Connect(t.Context(), addr); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf, ultraOverlay)

	refused, _ := newLightNode(t)
	_, _ = refused.Connect(t.Context(), addr)
	expectPeersEventually(t, refused)
	expectPeersEventually(t, sf, ultraOverlay)

	paying, payingOverlay := newLightNode(t)
	paying.HandshakeService().SetChequebookAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	if _, err := paying.Connect(t.Context(), addr); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf, ultraOverlay, payingOverlay)

	if got := sf.LightPeerRefusals(libp2p.LightRefusalUltraLight); got != 1 {
		t.Fatalf("got %v ultra-light refusals, want 1", got)
	}
	used, reserved, usedUltra, reservedUltra := container.Slots()
	if used != 2 || reserved != 0 || usedUltra != 1 || reservedUltra != 0 {
		t.Fatalf("got slots %d %d %d %d, want 2 0 1 0", used, reserved, usedUltra, reservedUltra)
	}
}

// TestUltraLightNodeLimitValue checks how the configured value is applied.
func TestUltraLightNodeLimitValue(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		light, configured, want int
		reduced                 bool
	}{
		{light: 100, configured: 0, want: 0},
		{light: 100, configured: -1, want: 0},
		{light: 100, configured: 40, want: 40},
		{light: 100, configured: 100, want: 100},
		{light: 100, configured: 150, want: 100, reduced: true},
	} {
		got, reduced := libp2p.UltraLightNodeLimit(tc.light, tc.configured)
		if got != tc.want || reduced != tc.reduced {
			t.Errorf("light %d, configured %d: got %d %v, want %d %v", tc.light, tc.configured, got, reduced, tc.want, tc.reduced)
		}
	}
}
