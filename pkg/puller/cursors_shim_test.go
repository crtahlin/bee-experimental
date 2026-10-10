// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"time"

	"github.com/ethersphere/bee/v2/pkg/puller"
)

// setCursorsTimeout is the per-tree shim for the reproduction in
// cursors_block_repro_test.go. On a tree without the cursors deadline
// (#695) it is a function that does nothing, and the reproduction fails.
func setCursorsTimeout(p *puller.Puller, d time.Duration) { p.SetCursorsTimeout(d) }
