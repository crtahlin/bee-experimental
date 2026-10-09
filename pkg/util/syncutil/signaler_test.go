// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package syncutil

import (
	"errors"
	"testing"
)

// TestSignalWithError checks that the first cause is kept, that it is
// visible once C is closed, and that a plain Signal carries none (#583).
func TestSignalWithError(t *testing.T) {
	t.Parallel()

	first, second := errors.New("first"), errors.New("second")

	s := NewSignaler()
	s.SignalWithError(first)
	<-s.C
	s.SignalWithError(second)
	if !errors.Is(s.Err(), first) {
		t.Fatalf("got cause %v, want the first one", s.Err())
	}

	plain := NewSignaler()
	plain.Signal()
	<-plain.C
	if plain.Err() != nil {
		t.Fatalf("a plain Signal carries cause %v", plain.Err())
	}

	nilCause := NewSignaler()
	nilCause.SignalWithError(nil)
	nilCause.SignalWithError(first)
	if !errors.Is(nilCause.Err(), first) {
		t.Fatalf("a nil cause must not block a later one, got %v", nilCause.Err())
	}
}
