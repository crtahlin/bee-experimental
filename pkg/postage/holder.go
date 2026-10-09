// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postage

import (
	"context"
	"errors"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// ErrHeldAreaFull is returned when a chunk qualifies to be held but the held
// area is full (#583).
var ErrHeldAreaFull = errors.New("postage: held area full")

// ErrHeldAddressFull is returned when a chunk qualifies to be held but its
// address already has the most held entries one address may have (#583).
var ErrHeldAddressFull = errors.New("postage: held entries for address full")

// HoldResult says what HoldUnvalidated did with a chunk.
type HoldResult int

const (
	// NotHeld: the chunk was not held.
	NotHeld HoldResult = iota
	// HeldNew: the chunk is held, and its address was not held before.
	HeldNew
	// HeldAgain: the chunk is held, and its address was held already,
	// with this stamp or another one.
	HeldAgain
)

// Held reports whether the chunk is held.
func (r HoldResult) Held() bool { return r != NotHeld }

// ChunkHolder keeps chunks whose stamp cannot be validated yet because the
// batch store is stale (#583). The storer implements it.
//
// A stale node keeps accepting and serving chunks: a chunk whose data is
// genuine but whose stamp names a batch the node has not seen yet is held,
// served by retrieval, and validated once the batch store has caught up.
type ChunkHolder interface {
	// HoldUnvalidated holds ch, whose stamp failed validation with cause,
	// when the batch store is stale and cause says the batch or its depth
	// is not known yet (ErrNotFound or ErrInvalidIndex). It reports whether
	// the chunk is held, and whether its address is newly held, so a caller
	// acts on the chunk's content once. A chunk that qualifies but finds the
	// held area full returns ErrHeldAreaFull, and one whose address has the
	// most entries it may have returns ErrHeldAddressFull. Otherwise it
	// returns NotHeld and nil, and the caller handles the stamp error as
	// before.
	//
	// The holder verifies the chunk's full content address (cac.Valid or
	// soc.Valid) itself and holds nothing that fails it.
	HoldUnvalidated(ctx context.Context, ch swarm.Chunk, cause error) (HoldResult, error)
}

// HoldsUnvalidated reports whether a stamp validation error is one a held
// chunk can recover from once the batch store catches up: the batch is not
// known yet, or the stamp index exceeds the depth known for it, since a
// dilution may not have been seen yet.
func HoldsUnvalidated(cause error) bool {
	return errors.Is(cause, ErrNotFound) || errors.Is(cause, ErrInvalidIndex)
}
