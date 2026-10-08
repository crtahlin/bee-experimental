// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postage

import "time"

// SyncHealth reports how current the batch store is (#583). The postage
// listener implements it.
//
// A stale batch store no longer stops the node. It degrades instead: it stays
// on the network and keeps accepting and serving chunks, does not play the
// storage lottery and does not lower its storage radius, until the listener
// is caught up again.
type SyncHealth interface {
	// Stale reports whether the batch store is stale: no page applied for
	// the stall timeout, or not caught up since that happened.
	Stale() bool
	// SinceProgress returns the time since the listener last applied a
	// page.
	SinceProgress() time.Duration
	// StaleEndedAt returns when the stale state last ended, or the zero
	// time if it never has.
	StaleEndedAt() time.Time
}
