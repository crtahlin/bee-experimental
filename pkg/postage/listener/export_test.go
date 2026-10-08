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

// SetStaleTimings replaces how often the watcher checks the sync health and
// how often the stale Warning repeats, for a test, and returns a function
// that restores them. New copies them, so set them before New.
func SetStaleTimings(watch, warnRepeat time.Duration) func() {
	oldW, oldR := staleWatchInterval, staleWarnRepeat
	staleWatchInterval, staleWarnRepeat = watch, warnRepeat
	return func() { staleWatchInterval, staleWarnRepeat = oldW, oldR }
}

// MinBlockPage is the smallest page the listener falls back to.
const MinBlockPage = minBlockPage

// SetStaleMaxBackoff replaces the cap of the wait after a failed call while
// stale, for a test, and returns a function that restores it. New copies it.
func SetStaleMaxBackoff(d time.Duration) func() {
	old := staleMaxBackoff
	staleMaxBackoff = d
	return func() { staleMaxBackoff = old }
}
