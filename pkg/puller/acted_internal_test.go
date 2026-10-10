// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller

import (
	"testing"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestSyncingFromNeedsEveryBin checks that a neighbour counts as syncing
// from a radius only when every bin from there up is syncing, not just the
// lowest one.
func TestSyncingFromNeedsEveryBin(t *testing.T) {
	t.Parallel()

	p := newSyncPeer(swarm.RandAddress(t), 4, 3)
	p.cursors = make([]uint64, 4)
	p.setBinCancel(func() {}, 1)
	p.setBinCancel(func() {}, 3)
	if p.syncingFrom(1, 4) {
		t.Fatal("syncing from 1 with bin 2 not syncing")
	}
	p.setBinCancel(func() {}, 2)
	if !p.syncingFrom(1, 4) {
		t.Fatal("not syncing from 1 with bins 1 to 3 syncing")
	}
}
