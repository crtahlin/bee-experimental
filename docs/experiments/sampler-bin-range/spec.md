# Scan only the bins that can hold the anchor's neighbourhood

Issue: [#568](https://github.com/crtahlin/wasp/issues/568)

## Problem

The reserve sampler walks every `chunkBin` entry from the storage radius to the
end of the index, and only afterwards drops the chunks outside the anchor's
neighbourhood (`pkg/storer/sample.go:237-240`):

```go
err := db.reserve.IterateChunksItems(db.StorageRadius(), func(ch *reserve.ChunkBinItem) (bool, error) {
	if swarm.Proximity(ch.Address.Bytes(), anchor) < committedDepth {
		return false, nil
	}
```

`IterateChunksItems` (`pkg/storer/internal/reserve/reserve.go:588-603`) seeks to
`chunkBin/<startBin>` and walks to the end of the namespace.

- **Doubling 0:** the storage radius equals the committed depth, so almost every
  entry walked belongs to the sampled neighbourhood.
- **Doubling d:** the storage radius is the committed depth minus d. The node
  holds 2^d neighbourhoods and the sampler walks all of them to sample one. At
  doubling 3 that is about 30 million entries of 106 bytes each, about 3.3 GB of
  index, to sample 3.85 million chunks.
- **The cost is invisible today.** `TotalIterated` is counted after the anchor
  filter, so it reports the same 3.85 million at every doubling.

On bench-1 (#566) the doubling-3 sample took 102 s against about 54 s at doubling
0, for the same 3,852,237 chunks, and read 47 GB against 32 to 35 GB. Part of
that is probably readahead on the shard files, which #567 measures separately.
This spec covers the index scan only.

Upstream v2.8.1 has the same loop (`pkg/storer/sample.go:105`).

## Hypothesis

A chunk's bin is its proximity to the node's overlay address
(`reserve.go:123`, `swarm.Proximity(r.baseAddr, chunkAddr)`). Let `p` be the
proximity of the overlay to the anchor, and `D` the committed depth. `Proximity`
is capped at `swarm.MaxPO` (31); bins are computed with the same function, so the
cap applies equally to both.

- **If `p < D`:** a chunk within `D` of the anchor agrees with the anchor on its
  first `D` bits. The overlay agrees with the anchor on its first `p` bits and
  differs at bit `p`, with `p < D`. So the chunk agrees with the overlay on
  exactly `p` bits: it is in bin `p`, and only bin `p` can hold such chunks.
- **If `p >= D`:** a chunk within `D` of the anchor agrees with the overlay on at
  least its first `D` bits, so it is in bin `D` or above. Conversely, every chunk
  in bin `D` or above agrees with the overlay, and so with the anchor, on its
  first `D` bits. The sampled chunks are exactly bins `D` to 31.

Walking only those bins visits every chunk the current loop keeps, and skips only
chunks the current filter rejects. At doubling 3 that is 3.85 to 15 million
entries instead of about 30 million: bin 6 holds four neighbourhoods, bin 7 two,
bin 8 and above one.

## Design

In `reserveSample` (`pkg/storer/sample.go`):

1. Compute `p := swarm.Proximity(db.baseAddr.Bytes(), anchor)` and the bin range:
   `lo, hi := p, p` if `p < committedDepth`, otherwise `committedDepth, swarm.MaxPO`.
   Raise `lo` to `db.StorageRadius()` as today. If `lo > hi`, there is nothing to
   walk.
2. Iterate with `IterateChunksItems(lo, ...)` and stop (return `true`) at the
   first item with `Bin > hi`. Keys are ordered by bin first, so everything after
   that item is outside the range.
3. Keep the anchor filter unchanged inside the loop. It becomes a check that
   always passes inside the range. It costs one proximity computation per entry,
   and it keeps the sample correct if the reasoning above is ever wrong.
4. Add `ChunkBinScanned int64` to `SampleStats`: entries walked, counted before
   any filter. It is added in `SampleStats.add` and logged with the other fields,
   so measurements can show the scan directly.

`db.baseAddr` is the address the reserve uses for its bins: both come from
`opts.Address` (`pkg/storer/storer.go:917,960`).

No new configuration. There is nothing to tune: the range is fully determined by
the overlay, the anchor and the committed depth.

## Protocol impact

None. The set of chunks sampled, the sample's contents and its hash are
unchanged. Only how much of the local index the sampler reads changes. Nothing
on the wire or on disk changes, and `.github/protocol-freeze.lock` is untouched.

## Measurement

1. **Equivalence (unit test, `pkg/storer`).** Build a reserve at several storage
   radii, and for anchors giving `p < D`, `p = D` and `p > D`, check that the
   sample items, `TotalIterated` and the sample hash equal those of the unchanged
   loop. The old loop is kept in the test as a reference implementation.
2. **Mutation checks** on the bounds: `lo` from `p` to `p+1`, the stop condition
   from `> hi` to `>= hi`, and the `p < D` comparison to `<=`. Each must make the
   equivalence test fail.
3. **bench-1 at doubling 3**, after #567 restores its readahead to the setting
   #567 finds best, or 8192 if none wins. A build of this change against `main`
   (1f956b40), 3 runs each, interleaved, each from an emptied page cache:
   sampler duration, `ChunkBinScanned`, and bytes and requests read (iostat).
   **Success:** `ChunkBinScanned` drops from about 30 million to the size of the
   anchor's bins, and the sample reads fewer bytes. Duration is reported either
   way.
4. **A negative result** is no change in duration or bytes read. That would mean
   the index scan is cheap next to the shard reads, and the gap #566 found comes
   from readahead (#567).

## Rollout and rollback

Always on, because the output is identical. Rolling back means running a build
without it. Nothing is stored, so there is nothing to migrate.

## Upstream portability

The change is local to `reserveSample` and uses only `swarm.Proximity`, the
reserve's existing `IterateChunksItems`, and the bin stored in each
`ChunkBinItem`. It applies to upstream v2.8.1 as is. It only matters there for
nodes using `reserve-capacity-doubling`, which upstream caps at 1. At doubling 1
the scan halves.

## Configuration

None. See Design.
