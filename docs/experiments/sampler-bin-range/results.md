# Results: scan only the bins that can hold the anchor's neighbourhood

Issue: [#568](https://github.com/crtahlin/wasp/issues/568). Spec: [spec.md](spec.md).
Implementation: [#570](https://github.com/crtahlin/wasp/pull/570).

## Measurement (spec item 3)

bench-1 at `reserve-capacity-doubling: 3` (storage radius 6, committed depth 9,
about 30.7 million chunks in the reserve), 2026-10-01, 14:15 to 15:33 UTC.
- **Builds:** `main` at 1f956b40 against this change at 39a5e6c6.
- **Anchors:** two fixed anchors, passed to `/rchash`:
  - **worst case:** p = 6, equal to the storage radius, so the range is bin 6, holding 4 neighbourhoods;
  - **own neighbourhood:** p = 12, which is at least D, so the range is bins 9 to 31.
- **Runs:** 3 per build and anchor, interleaved. Each run began with a binary swap and a restart, then 3 minutes of settling, then an emptied page cache, then one sample.
- **Other conditions:** `read_ahead_kb` 8192 (#567 found no setting better). bench-2 was idle.
- `main` has no `ChunkBinScanned`. Its scan is the reserve size at or above the storage radius, about 30.7 million entries.

**Table: reserve sample on bench-1 at doubling 3, `main` against the bounded scan**

| Anchor | Build | Sample (s) | `TotalIterated` | `ChunkBinScanned` | Data read (GB, iostat) |
|---|---|---|---|---|---|
| p = 6 (bin 6) | `main` | 138.5, 78.6, 160.4 | 3,860,338 to 3,862,228 | (about 30.7 million) | 65.7, 64.4, 66.7 |
| p = 6 (bin 6) | change | 73.3, 76.6, 73.3 | 3,860,832 to 3,862,596 | 15,440,125 to 15,447,107 | 64.4, 63.9, 63.9 |
| p = 12 (own) | `main` | 74.0, 138.3, 82.8 | 3,855,246 to 3,857,512 | (about 30.7 million) | 40.8, 43.7, 45.4 |
| p = 12 (own) | change | 106.4, 61.7, 61.5 | 3,855,685 to 3,857,937 | 3,855,685 to 3,857,937 | 40.3, 38.4, 37.8 |

**Against the success criterion:**
- **`ChunkBinScanned`** equals the size of the anchor's bins: bin 6 for p = 6, and exactly `TotalIterated` for p = 12, where every entry walked is in the neighbourhood.
- **Bytes read:** the change read fewer bytes than `main` in each of the 3 runs, for both anchors. **The criterion is met.**
- **The byte saving is small:** 1 to 4 per cent for p = 6, and 1 to 17 per cent for p = 12.

**Duration** (reported, no criterion):
- **p = 6:** all 3 runs of the change (73 to 77 s) were faster than all 3 of `main` (79 to 160 s). The one-sided Mann-Whitney p is 0.05, the smallest possible with 3 runs against 3.
- **p = 12:** medians 62 s against 83 s, with overlap.
- **The spread under identical conditions is large,** 74 to 138 s. CPU profiles taken during the samples show why:
  - fast samples are bound by CPU: about 3.4 to 4.2 cores in use, about half of it BMT hashing;
  - slow samples wait on the disk, at about 1.1 cores.
  - So the spread comes from the shared host's storage, not from the code.

## Where a doubled node's sample spends its reads

A separate trace (`bpftrace` on `vfs_read` and on page-cache insertions, one cold sample each, `main` build) attributes the bytes bee requests:

**Table: bytes requested by bee during one sample, by file kind**

| Node | Sharky shards | Pebble tables | Read from disk in total |
|---|---|---|---|
| bench-2, doubling 0 | 15.06 GB (3.86 million reads) | 2.35 GB | 33.7 GB |
| bench-1, doubling 3, p = 12 | 15.04 GB | 10.61 GB | 44.7 GB |
| bench-1, doubling 3, p = 6 | 15.06 GB | 10.96 GB | 71.7 GB |

- **Sharky's share is the same at every doubling:** one read per sampled chunk.
- **Pebble's share grows about 4.5 times at doubling 3.** The full index walk is part of it, and this change removes it. Point lookups in an index 8 times larger are the likely rest.
- **The disk reads 2 to 2.7 times what bee requests.** Part of this is certain: a sharky slot is 4,201 bytes, larger than a 4 KB page and not aligned to pages, so every chunk read brings in two pages. That alone is about 31 GB for 3.86 million chunks.
- **Readahead is not the cause.** #567 found no readahead setting that changes sample time.

## Conclusion

The change does what the spec claims. The index walk shrinks to the anchor's bins (2 to 8 times fewer entries at doubling 3), the sample is unchanged, and fewer bytes are read. The time saving is smaller than the scan reduction suggests, because the sample on this hardware is bound by hashing CPU or by disk waits, and the index walk was a minor part of either.
