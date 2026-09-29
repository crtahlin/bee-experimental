# Results: a Pebble block cache sized above what the memtables reserve

Issue: [#555](https://github.com/crtahlin/wasp/issues/555). Spec: [spec.md](spec.md).
Raw numbers and the full history are in the issue's comments.

## Test 4: the pre-registered comparison (2026-09-29)

On bench-2 (8 cores, full radius-9 neighbourhood of about 3.83 M chunks, 64 MiB
write buffer). Before every sample, 800 MB of local ingest was written, which grew
the memtable to its full 64 MiB. The load was identical in both arms, and the
runs were interleaved. A model prediction was recorded from the live heap and
MANIFEST before each sample.

**Table: reserve sample after the write load, bench-2, by block cache size**

| Block cache | Runs | Sample durations | `readFilter` share of CPU | Slow (over 150 s) |
|---|---|---|---|---|
| 32 MiB | 6 | 217, 220, 203, 241, 235, 178 s | 44 to 54% | 6 of 6 |
| 256 MiB | 6 | 44, 40, 42, 47, 47, 46 s | 0.4 to 0.7% | 0 of 6 |

- **Fisher's exact test, one-sided: p = 1/924, about 0.001.**
- Memtable reservation at the time of the sample: 46 to 92 MiB. That left
  0 MiB usable per shard at 32 MiB, and about 48.6 MiB at 256 MiB.
- The L0 to L5 tables over `retrievalIdx/` numbered 0 to 13, needing at most
  0.8 MiB in any shard.
- **The model predicted 11 of 12 runs.** The miss was run 11 (32 MiB). It was
  predicted fast because no upper-level table overlapped the lookup keys at
  snapshot time, but it was slow. The likely cause is a flush that created an L0
  table during the sample; that was not checked.

## Earlier tests, and why they did not decide it

- **Test 1** (bench-1, 15 interleaved runs per arm, a restart before each run).
  32 MiB was slow in 3 of 15 runs, and 256 MiB in 1 of 15 (p about 0.3). A
  restart keeps the memtables small, so the slow state rarely occurred. Three of
  the four slow runs overlapped a sample on bench-2, which shares the host.
- **Test 2** (bench-2, the same protocol) was stopped for the same two reasons.
- **Test 3** (bench-2, soak, a sample qualified at a memtable of at least
  32 MiB). All 6 qualifying 32 MiB samples were fast. The qualifying condition
  came from an incomplete mechanism: a 32 MiB memtable still left 0.69 MiB usable
  per shard, more than the 0.40 MiB needed.
- **A causal check:** at a 32 MiB cache, lowering only `db-write-buffer-size`
  from 64 MiB to 4 MiB kept bench-2 fast for 1.7 h. The same node had been slow
  at 64 MiB.

## The implementation (spec, Measurement item 2)

On bench-2 with a build of this change (`0.1.5-18e5e5dc`), using test 4's
protocol (800 MB of local ingest before each sample), 6 runs alternating the two
configurations, 2026-09-29.

**Table: reserve sample after the write load, bench-2, with the change**

| Configuration | Pebble cache | Sample durations | `readFilter` | Memtables before the sample | Cached blocks before the sample |
|---|---|---|---|---|---|
| defaults | 160 MiB | 45.8, 44.0, 45.1 s | 0.57 to 0.61% | 64 MB | 87 to 97 MB |
| `db-write-buffer-size: 67108864` | 256 MiB | 45.7, 41.0, 46.3 s | 0.35 to 0.53% | 97 to 127 MB | 156 to 227 MB |

**All 6 runs were under 150 s, with `readFilter` under 5%. The success criterion
is met.** With the memtables at full size, the cache still held 87 to 227 MB of
blocks. Under the same load the previous 32 MiB default was slow in 6 of 6 runs
(test 4).
