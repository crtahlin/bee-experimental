# A Pebble block cache sized above what the memtables reserve

Issue: [#555](https://github.com/crtahlin/wasp/issues/555)

## Revision of 2026-09-29

This spec was first merged in [#558](https://github.com/crtahlin/wasp/pull/558)
with a different design (a fixed 256 MiB default), a different explanation
("the filters and index do not fit in 32 MiB") and a success criterion that
failed. The measurements behind the change are reported on #555. In short:

- The first explanation was wrong. The metadata figure of 63 to 68 MiB came from
  the `IndexSize` table property. The table layouts give 36 to 41 MiB in total,
  and a reserve sample's lookups only need a small part of it (below).
- The success criterion ("every 256 MiB run beats every 32 MiB run", three runs
  per arm after a restart) failed. 32 MiB turned out to be fast after a restart
  and slow only later, so restarted runs mostly measured the fast state.
- The design and the criterion below were written **after** those results. That
  is stated here so that it is not mistaken for a prediction. The criterion was
  then tested in a separately pre-registered test (test 4 on #555), which it
  passed.

## Problem

`db-block-cache-capacity` defaults to 32 MiB (`cmd/bee/cmd/cmd.go:351`,
`pkg/storer/storer.go:267`), a value chosen for goleveldb. Since
[#185](https://github.com/crtahlin/wasp/issues/185) made Pebble the engine for a
new data directory, the same 32 MiB becomes Pebble's block cache
(`pebbleIndexStoreOptions`, `pkg/storer/storer.go:441`).

On a node holding a full neighbourhood, that cache sometimes stops holding
anything useful. A reserve sample then spends about half of its CPU re-reading
bloom filter blocks, allocating them on the Go heap (the build has no cgo),
checksumming and collecting them, and runs three to twenty times slower. On
`stake-1` this made samples take up to 24 minutes against a round window of
about 570 seconds, and the node missed both rounds it was selected for.

## Mechanism

From Pebble v1.1.5 source, confirmed by the measurements on #555.

1. **Memtables are charged against the block cache.** Each memtable reserves its
   full size from the cache (`db.go:2350`, `Cache.Reserve`, which "effectively
   shrinks the size of the cache by N bytes"). The reservation is split evenly
   over the cache's shards. A memtable that is being flushed, and one kept for
   recycling, keep their reservations. Memtables start at 256 KiB after a
   restart and double at each rotation up to `db-write-buffer-size`. With
   `MemTableStopWritesThreshold` at its default of 2, up to
   (2 + 1) × `db-write-buffer-size` can be reserved.
2. **A block that does not fit the usable size is silently not cached**
   (`internal/cache/clockpro.go`, `metaAdd`). With the reservation at or above
   the cache size, nothing is cached at all.
3. **A point lookup reads the filters of the upper levels only.** `Get` walks L0
   to L6 and checks the filter of each overlapping table above L6, but never an
   L6 filter (`table_cache.go:511`, `UseL6Filters` is false). The filter working
   set of a reserve sample is therefore the L0 to L5 tables overlapping
   `retrievalIdx/`: 0 to 13 tables and at most 0.8 MiB in any cache shard,
   across all measurements.

A sample is slow when, in some cache shard, the usable space
(cache minus reservation, divided by the shard count) is smaller than that
upper-level filter set. It is fast otherwise. Both sides change on their own:
the reservation grows as memtables double after a restart, and the upper-level
tables change with compaction and with writes (pullsync, local ingest). That is
why the same node was fast on some days and slow on others with the same
configuration.

**Evidence** (details and raw numbers on #555):

- Heap profiles: on a node slow at 32 MiB, 64.6 MB of memtable arenas were live.
  On a node fast at 32 MiB, 29.2 MiB were reserved and 2.05 MB of blocks were
  cached, about what the model allows.
- A causal test: bench-2 at a 32 MiB cache stayed fast for 1.7 h when only
  `db-write-buffer-size` was lowered from 64 MiB to 4 MiB.
- **Test 4 (pre-registered):** bench-2, with 800 MB of local ingest before every
  sample to grow the memtable to its full 64 MiB, identical in both arms,
  interleaved. **32 MiB: 6 of 6 slow** (178 to 241 s, `readFilter` 44 to 54% of
  CPU). **256 MiB: 0 of 6 slow** (40 to 47 s). Fisher's exact test, one-sided,
  p = 1/924, about 0.001. The model, applied to the live heap and MANIFEST
  before each sample, predicted 11 of the 12 runs. The miss was a slow run that
  it predicted fast, most likely because a flush created a new L0 table during
  the sample; that was not checked.

## Design

Size Pebble's block cache as **the usable amount plus the most that memtables
can reserve**:

```
pebble cache = usable + (MemTableStopWritesThreshold + 1) × db-write-buffer-size
usable       = db-block-cache-capacity, or 64 MiB when it is left at the shared
               default of 33554432
```

With the default 32 MiB write buffer, the default Pebble cache is therefore
64 MiB + 3 × 32 MiB = **160 MiB**. With a 64 MiB write buffer it is 256 MiB.

- **`db-block-cache-capacity` means usable cache on both engines.** With
  goleveldb the write buffer is separate from the block cache, and with this
  change the same is true for Pebble in effect. An operator who sets the option
  gets that much cache that memtables cannot take away.
- **Why 64 MiB usable by default:** the upper-level filter set measured so far is
  at most 0.8 MiB per shard. 64 MiB gives each shard well over ten times that,
  so compaction states with more upper-level tables still fit.
- **The shared default is replaced for Pebble only when it is left unchanged**,
  the pattern `pebbleIndexStoreOptions` already uses for
  `db-compaction-l0-trigger` (`pkg/storer/storer.go:453`). The one case it gets
  wrong is an operator who explicitly writes `db-block-cache-capacity: 33554432`:
  they get 64 MiB usable. The documentation says so and suggests 33554431 for a
  deliberately small cache.
- **goleveldb nodes are unchanged.**
- **Existing Pebble nodes get the new size on upgrade** if they never set the
  option.

The help text in `cmd/bee/cmd/cmd.go`, the four packaged `bee.yaml` files,
`docs/config-reference.yaml` and `docs/DIFFERENCES.md` are updated in the same
pull request, per rules 8 and 13.

### What raising and lowering it cost

- **Raising it** costs memory, but not all of it at once. Memtables live inside
  the reservation, so the cache's blocks and the memtables together stay near
  the Pebble cache size (160 MiB by default). On this build both are on the Go
  heap, and the collector lets the heap grow to about twice its live size, so
  the resident cost is up to about twice that. A node with little RAM loses that
  much page cache.
- **Lowering it** below the upper-level filter set, about 1 MiB per shard today,
  brings back the filter re-reads and slow samples.
- It costs other nodes nothing.

## Protocol impact

**None.** Local cache sizing. No value in `.github/protocol-freeze.lock` is
affected; nothing on the wire or on disk changes.

## Measurement

1. **The mechanism and the benefit of more room:** test 4 on #555 (above),
   already done.
2. **The implementation itself**, on bench-2 with a build of the pull request,
   using test 4's protocol (restart, 5 min settle, 800 MB of local ingest, one
   `/rchash` sample, unpin), 6 runs:
   - 3 runs with `db-block-cache-capacity` unset and `db-write-buffer-size`
     unset (defaults: 160 MiB Pebble cache);
   - 3 runs with `db-block-cache-capacity` unset and `db-write-buffer-size` at
     64 MiB (bench-2's value: 256 MiB Pebble cache).

   **Success:** all 6 runs under 150 s, and `readFilter` under 5% of CPU in each
   profile. **A negative result** is any run slow with filter thrash, which would
   mean the formula leaves too little room.
3. After the merge, the change is validated on a real node by the next selected
   round on stake-1 committing (`lastPlayedRound` advancing).

## Not in scope

- Why stake-1 remains about 5x slower than bench-2 after the fix. It has four
  first-generation EPYC cores against eight Zen 3 cores, and its kernel spends
  about five times more CPU per cold chunk read. Turning off the retbleed
  mitigation did not speed it up (#555). That is a host property.
- Building with cgo so that the cache and memtables leave the Go heap:
  [#560](https://github.com/crtahlin/wasp/issues/560).
- Scaling with `reserve-capacity-doubling`. A doubled reserve roughly doubles the
  upper-level filter set, which the 64 MiB usable default still covers by a wide
  margin; measuring it belongs to that experiment.
