// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package failover

import "time"

// SetActive forces the active endpoint, so a test can arrange a state that
// would otherwise need a real failure to reach.
func SetActive(b *Backend, i int) { b.active.Store(int32(i)) }

// IsTransportFailure exposes the classifier, which is the piece that decides
// whether an error means "no answer" or "an answer you did not like".
var IsTransportFailure = isTransportFailure

// SetMinAttemptTimeout replaces the floor of a per-attempt deadline for a
// test and returns a function that restores it.
func SetMinAttemptTimeout(d time.Duration) func() {
	old := minAttemptTimeout
	minAttemptTimeout = d
	return func() { minAttemptTimeout = old }
}
