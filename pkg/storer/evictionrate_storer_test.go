// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import "testing"

// TestStorerRateZeroIsUnpaced checks that storer.New applies no default of
// its own: a zero ReserveEvictionRate leaves eviction unpaced, and 500
// paces it. The default of 500 lives only in the start command's flag
// (#651).
func TestStorerRateZeroIsUnpaced(t *testing.T) {
	t.Parallel()
	if st, _ := newEvictionStore(t, 0); st.EvictionPacedForTest() {
		t.Fatal("eviction paced at rate 0, want no limit")
	}
	if st, _ := newEvictionStore(t, 500); !st.EvictionPacedForTest() {
		t.Fatal("eviction not paced at rate 500")
	}
}
