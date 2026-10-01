// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"testing"
	"time"

	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestReserveSampleScansOnlyAnchorBins checks that bounding the sampler's index
// scan to the anchor's bins keeps exactly the chunks the unbounded scan kept,
// and walks only the entries in those bins. See issue #568 and
// docs/experiments/sampler-bin-range/spec.md.
//
// The reference is the loop the sampler used before: iterate from the storage
// radius to the end and keep chunks within committedDepth of the anchor. The
// bounded scan walks a subset of those entries and applies the same filter, so
// equal TotalIterated means equal sets of chunks, and so an equal sample.
// ChunkBinScanned is checked as well, because equal samples alone cannot show a
// range that is too wide. The spec also asks to compare sample items and hash
// with a reference; that follows from the equal set of chunks, since every
// stage after the iteration is unchanged.
func TestReserveSampleScansOnlyAnchorBins(t *testing.T) {
	t.Parallel()

	const (
		committedDepth uint8 = 5
		chunksPerBin   int   = 8
		maxGeneratedPO int   = 8
		anchorCount    int   = 3
	)

	var (
		baseAddr = swarm.RandAddress(t)
		timeVar  = uint64(time.Now().UnixNano())
	)

	opts := dbTestOps(baseAddr, 10000, nil, nil, time.Second)
	opts.ValidStamp = func(ch swarm.Chunk) (swarm.Chunk, error) { return ch, nil }
	st, err := diskStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}

	stamped := func(ch swarm.Chunk) swarm.Chunk {
		return ch.WithBatch(3, 2, false).WithStamp(postagetesting.MustNewStampWithTimestamp(timeVar - 1))
	}
	chs := make([]swarm.Chunk, 0, chunksPerBin*(maxGeneratedPO+1+anchorCount))
	// Chunks across the low bins of the overlay. GenerateValidRandomChunkAt
	// returns a chunk with proximity above po, so these land in bins 1 and up.
	for po := range maxGeneratedPO {
		for range chunksPerBin {
			chs = append(chs, stamped(chunk.GenerateValidRandomChunkAt(t, baseAddr, po)))
		}
	}
	// Chunks in bin 31, which random chunks almost never reach, so that the
	// upper end of the range is exercised. They are not valid content-addressed
	// chunks, so the sampler counts them in TotalIterated and then drops them
	// as rogue, which is enough to see whether the scan reached them.
	for range chunksPerBin {
		chs = append(chs, stamped(chunk.GenerateTestRandomChunkAt(t, baseAddr, int(swarm.MaxPO))))
	}

	// Anchors at a chosen proximity p to the overlay, each with chunks of its own
	// neighbourhood (proximity above committedDepth to the anchor), so that a
	// sample with p < committedDepth is not empty.
	anchors := map[uint8]swarm.Address{}
	for _, p := range []uint8{3, 5, 7} {
		a := swarm.RandAddressAt(t, baseAddr, int(p))
		anchors[p] = a
		for range chunksPerBin {
			chs = append(chs, stamped(chunk.GenerateValidRandomChunkAt(t, a, int(committedDepth))))
		}
	}

	putter := st.ReservePutter()
	for _, ch := range chs {
		if err := putter.Put(context.Background(), ch); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name          string
		storageRadius uint8
		p             uint8
		windowed      bool
	}{
		{name: "doubled node, radius <= p < depth", storageRadius: 2, p: 3},
		{name: "radius above p < depth, nothing to walk", storageRadius: 4, p: 3},
		{name: "p equals depth", storageRadius: 2, p: 5},
		{name: "p above depth", storageRadius: 2, p: 7},
		{name: "doubled node, windowed", storageRadius: 2, p: 3, windowed: true},
		{name: "p above depth, windowed", storageRadius: 2, p: 7, windowed: true},
	}

	// Sequential: the cases share one reserve and change its radius.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := st.Reserve().SetRadius(tc.storageRadius); err != nil {
				t.Fatal(err)
			}
			anchor := anchors[tc.p].Bytes()
			if got := swarm.Proximity(baseAddr.Bytes(), anchor); got != tc.p {
				t.Fatalf("test setup: anchor proximity %d, want %d", got, tc.p)
			}

			var inWindow func([]byte) bool
			if tc.windowed {
				inWindow = func(addr []byte) bool {
					return swarm.Proximity(addr, anchor) >= committedDepth+1
				}
			}

			// The range, derived here independently of the code under test.
			lo, hi := committedDepth, swarm.MaxPO
			if tc.p < committedDepth {
				lo, hi = tc.p, tc.p
			}
			lo = max(lo, tc.storageRadius)

			var wantIterated, wantScanned, binMaxPO int64
			err := st.Reserve().IterateChunksItems(tc.storageRadius, func(ch *reserve.ChunkBinItem) (bool, error) {
				if ch.Bin >= lo && ch.Bin <= hi {
					wantScanned++
				}
				if swarm.Proximity(ch.Address.Bytes(), anchor) < committedDepth {
					return false, nil
				}
				if inWindow != nil && !inWindow(ch.Address.Bytes()) {
					return false, nil
				}
				wantIterated++
				if ch.Bin == swarm.MaxPO {
					binMaxPO++
				}
				return false, nil
			})
			if err != nil {
				t.Fatal(err)
			}

			sample, err := st.ReserveSampleWithWindow(context.Background(), anchor, committedDepth, timeVar, nil, inWindow)
			if err != nil {
				t.Fatal(err)
			}

			if sample.Stats.TotalIterated != wantIterated {
				t.Fatalf("TotalIterated %d, want %d: the bounded scan kept a different set of chunks than the full scan",
					sample.Stats.TotalIterated, wantIterated)
			}
			if sample.Stats.ChunkBinScanned != wantScanned {
				t.Fatalf("ChunkBinScanned %d, want %d: the scan walked entries outside bins %d to %d",
					sample.Stats.ChunkBinScanned, wantScanned, lo, hi)
			}

			// Guard the test itself: each case must exercise what it is named for.
			switch {
			case tc.storageRadius > tc.p && tc.p < committedDepth:
				if wantIterated != 0 || wantScanned != 0 {
					t.Fatalf("test setup: expected an empty range, got %d kept and %d in range", wantIterated, wantScanned)
				}
			case tc.p >= committedDepth:
				if binMaxPO == 0 {
					t.Fatal("test setup: no chunk in bin 31 was kept, so the upper bound is not exercised")
				}
			default:
				if wantIterated == 0 {
					t.Fatal("test setup: nothing kept, so the case does not exercise the range")
				}
			}
		})
	}
}
