// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener

import "time"

var (
	TailSize          = DefaultConfirmationDepth
	BatchFactor       = defaultBatchFactor
	BlockPage         = uint64(blockPage)
	BlockPageSnapshot = uint64(blockPageSnapshot)
)

// SetCallTimeouts replaces the deadlines of the listener's chain calls for a
// test and returns a function that restores them.
func SetCallTimeouts(blockNumber, filterLogs time.Duration) func() {
	oldB, oldF := blockNumberTimeout, filterLogsTimeout
	blockNumberTimeout, filterLogsTimeout = blockNumber, filterLogs
	return func() { blockNumberTimeout, filterLogsTimeout = oldB, oldF }
}
