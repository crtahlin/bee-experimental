# A Pebble block cache default that fits a full neighbourhood's index

Issue: [#555](https://github.com/crtahlin/wasp/issues/555)

## Problem

`db-block-cache-capacity` defaults to 32 MiB (`cmd/bee/cmd/cmd.go:351`,
`pkg/storer/storer.go:267`). The value was chosen for goleveldb. Since
[#185](https://github.com/crtahlin/wasp/issues/185) made Pebble the engine for a
new data directory, the same 32 MiB becomes Pebble's block cache
(`pebbleIndexStoreOptions`, `pkg/storer/storer.go:441`).

With that cache, a reserve sample on a node holding a full neighbourhood spends
about half of its CPU re-reading bloom filter blocks. Each lookup of a chunk's
retrieval index entry needs the table's filter block (one per table, about
120 to 400 KiB) and one 4 KiB partition of its two-level index. The filters and
index partitions do not stay in the cache, so they are read again, allocated on
the Go heap (this build has no cgo), checksummed and collected, for most of the
3.8 million lookups in a sample.

On `stake-1` this made the sample take 24 minutes against a round window of
about 570 seconds, and the node missed both rounds it was selected for.

## What is measured, and what is not

**Measured (issue #555, comments of 2026-09-28 and 2026-09-29).**

**Table: Pebble index metadata on three nodes holding a full radius-9 neighbourhood**

| Node | Tables | Keys | Bloom filters | Index blocks | Filters and index |
|---|---|---|---|---|---|
| stake-1 | 209 | 19.8 M | 23.6 MiB | 39.4 MiB | 63 MiB |
| bench-2 | 204 | 21.6 M | 25.7 MiB | 40.7 MiB | 66 MiB |
| bench-1 | 231 | 23.0 M | 27.5 MiB | 40.8 MiB | 68 MiB |

Read with `sstable.Reader` from Pebble v1.1.5, the version wasp uses, on the
live table files, read-only.

**Table: reserve sample duration against block cache size, bench-1, one run per size, 2026-09-28**

| Block cache | Cold | Warm | `readFilter` share of CPU |
|---|---|---|---|
| 32 MiB (default) | 171 s | 172 s | 37% |
| 64 MiB | 166 s | 161 s | 37% |
| 128 MiB | 61 s | 49 s | 8% |
| 256 MiB | 69 s | 50 s | not measured (profile window missed the run) |

bench-2 went from 246 s to 73 s cold with 1 GiB. stake-1 went from 24 min to
about 6.5 min with 256 MiB or 1 GiB. Overnight soaks on both held steady for
10 to 12 hours.

These are single runs per size. Rule 7 asks for three per condition, which is
what the measurement below provides.

**Not measured, and not claimed by this spec: why the same nodes were fast
earlier with the same 32 MiB cache.** stake-1 sampled in about 120 to 177 s on
2026-09-24, and bench-2 in about 45 s before 2026-09-22, both at 32 MiB with a
reserve of about the same size. One explanation fits (a working set close to the
cache's size, which works or thrashes depending on cache history), but it has
not been tested. Tests to settle it are running separately and will be reported
on #555. This spec does not depend on the answer: whatever let the default work
before, it does not work on these nodes now, and a larger cache fixes it now.

## Hypothesis

A Pebble block cache of at least twice the size of the filters plus index blocks
keeps them resident during a sample. At 63 to 68 MiB of metadata for a full
neighbourhood, 128 MiB is the smallest size that works on the three nodes, and
256 MiB leaves room for growth in the index. Above that, more cache buys
nothing measurable: 256 MiB and 1 GiB gave the same sample times.

If the hypothesis is wrong, interleaved runs at 32 and 256 MiB on the same node
will not differ beyond run-to-run noise.

## Design

When the storage engine is Pebble and `db-block-cache-capacity` is left at the
shared default (33554432), use **268435456 (256 MiB)** instead. An operator
value that differs from the shared default is used as given.

This is the pattern `pebbleIndexStoreOptions` already uses for
`db-compaction-l0-trigger` (`pkg/storer/storer.go:449`): the goleveldb-oriented
default is not allowed to override what Pebble needs, and an explicit operator
value still wins.

- **goleveldb nodes are unchanged.** [#12](https://github.com/crtahlin/wasp/issues/12)
  measured a small gain there (1.17x at 2 GiB), because the page cache already
  holds most of a goleveldb index. That result does not carry over to Pebble on a
  cgo-free build, where a page-cache hit still costs the allocation, the
  checksum and the collection.
- **Existing Pebble nodes get the new value on upgrade** if they never set the
  option. That is intended; it is what the change is for.
- **The one case the pattern gets wrong:** an operator who explicitly writes
  `db-block-cache-capacity: 33554432` on a Pebble node gets 256 MiB. The option's
  documentation says so and suggests 33554431 for a deliberately small cache.
  The same caveat exists for the L0 trigger today.

The help text in `cmd/bee/cmd/cmd.go`, `packaging/bee.yaml` (and the three
copies for Homebrew and Scoop), `docs/config-reference.yaml` and
`docs/DIFFERENCES.md` are updated in the same pull request, per rules 8 and 13.

### What raising and lowering it cost

- **Raising it** costs Go heap, about one to one: the cache is allocated in Go
  memory on this build. 256 MiB added about 0.6 GB to the resident size on
  stake-1 (0.89 GB with 256 MiB against about 0.32 GB before, including the
  heap's own overhead). A node with little RAM loses that much page cache.
- **Lowering it** below about twice the index metadata brings back the filter
  re-reads, and with them a reserve sample several times slower. On a node with
  a full neighbourhood that means below about 128 MiB.
- It costs other nodes nothing.

## Protocol impact

**None.** Local cache sizing. No value in `.github/protocol-freeze.lock` is
affected; nothing on the wire or on disk changes.

## Measurement

On bench-1, holding a full radius-9 neighbourhood of about 3.83 M chunks:

1. Interleave 32 MiB and 256 MiB, three runs each, in the order
   32, 256, 32, 256, 32, 256.
2. Each run: set the value, restart the node, wait 8 minutes, drop the page
   cache, run one `/rchash` sample with anchor1 set to the node's overlay.
3. Record the sampler's own `reserve sampler finished` line: duration, chunks
   iterated, chunk-load and hashing time. Normalise per chunk iterated.

**Success:** every 256 MiB run is faster than every 32 MiB run, by more than
the spread within either arm.

**A negative result** is overlapping ranges. That would mean the one-run
comparisons above were noise, and the default stays as it is.

After the merge, the change is validated on a real node by the next selected
round on stake-1 committing (`lastPlayedRound` advancing), which a sample that
finishes inside the window makes possible.

## Not in scope

- Why stake-1 remains about 5x slower than bench-2 after the fix. Measured on
  #555: its host has first-generation EPYC cores with the retbleed return thunk,
  where a cold chunk read costs about 90 us of kernel CPU against about 17 us on
  bench-2. That is a host property, not a Pebble one.
- Dropping the bottom-level bloom filter. That would shrink the filters by about
  70% and could make a smaller cache enough, but it changes missing-key lookups
  in pullsync and needs its own measurement.
- Scaling the default with `reserve-capacity-doubling`. A doubled reserve
  doubles the key count; whether 256 MiB still fits then is a question for that
  experiment, and the documentation says to raise the value with it.
