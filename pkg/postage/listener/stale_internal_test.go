// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener

import (
	"testing"
	"time"
)

// TestStaleWriteAfterCatchUp replays an interleaving of Stale with the loop:
// Stale reads a last progress older than the stall timeout, the loop then
// applies a caught-up page, and only then does Stale record the stale
// period. The node must not stay stale (#583).
func TestStaleWriteAfterCatchUp(t *testing.T) {
	t.Parallel()

	l := &listener{stallingTimeout: time.Minute}
	l.listening.Store(true)

	old := monoNow() - int64(2*time.Minute)
	l.lastProgress.Store(old)
	if !l.Stale() {
		t.Fatal("not stale two minutes after the last progress")
	}

	// The read in Stale happened here; the loop applies a caught-up page.
	l.lastProgress.Store(monoNow())
	l.markCaughtUp()
	// The delayed write from that read, a moment later.
	time.Sleep(time.Millisecond)
	l.markStale(old)

	if l.Stale() {
		t.Fatal("still stale after a caught-up page")
	}
}
