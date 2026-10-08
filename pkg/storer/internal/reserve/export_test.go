// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package reserve

type (
	RadiusItem = radiusItem
)

var (
	ErrMarshalInvalidAddress = errMarshalInvalidAddress
	ErrUnmarshalInvalidSize  = errUnmarshalInvalidSize
)

// SetEvictionRound sets the round size of EvictBatchBin for a test and
// returns a function that restores it.
func SetEvictionRound(n int) func() {
	old := evictionRound
	evictionRound = n
	return func() { evictionRound = old }
}
