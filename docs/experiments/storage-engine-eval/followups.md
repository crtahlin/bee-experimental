# Re-checking the Pebble follow-ups

Issue: [#198](https://github.com/crtahlin/wasp/issues/198). Follows the
storage-engine comparison in this directory
([results.md](results.md), [#185](https://github.com/crtahlin/wasp/issues/185)).

**Outcome: of the three follow-ups, only the level-0 one was a real question, and it
is answered yes. Re-checking it on the real Pebble node surfaced a larger problem:
the reserve-sample read is bound by disk speed and page-cache warmth, not by level-0
depth or the engine, which means the read-axis verdict in
[#185](https://github.com/crtahlin/wasp/issues/185) is not soundly established. That
is filed separately as [#231](https://github.com/crtahlin/wasp/issues/231). The other
two follow-ups are not the questions to spend time on now, and the conditions that
would bring them back are recorded.** The issue itself asked for exactly this check,
warning that the obvious metric had already been the wrong one once.

## Follow-up 2: level 0 stays shallow over long uptime (confirmed)

The `db-compaction-l0-trigger: 4` setting that made the sample read fast in
[#185](https://github.com/crtahlin/wasp/issues/185) was verified right after a
restart. The question was whether level 0 stays shallow through long steady-state
operation. It does, and the read it protects is at parity with goleveldb in
operation (see the section below).

It does. bench-2 (Pebble, no doubling, `trigger: 4`) after about 21 hours of uptime,
reserve full and settled:

| Pebble level | files | size |
|---|---|---|
| 0 | 6 | 9.6 MB |
| 4 | 11 | 37.8 MB |
| 5 | 45 | 249 MB |
| 6 | 142 | 1.48 GB |

Level 0 held at 6 files across four snapshots over two minutes, under the stop-writes
threshold of 12, with 47 MB of compaction debt. Compaction is keeping up, not falling
behind. The fix holds beyond a fresh restart.

## The read: at parity in operation, once measured correctly

This section replaces an earlier, wrong conclusion in the same file that the read
axis showed Pebble much slower. The correction matters, so it is kept in place
rather than quietly removed.

What was measured first: the reserve sample driven through the `/rchash` endpoint
with the operating system page cache dropped before each run showed Pebble far
slower than goleveldb, first about 170 to 217 s against goleveldb's 39 s, then
about 120 s after the disk was equalised (bench-2's disk was moved from an emulated
SATA controller to virtio-blk to match bench-1; both then read at about 670 MB/s
with O_DIRECT and about 2 GB/s buffered). That looked like a roughly threefold
engine difference on the read.

It is not. bee's own reserve sampler, the metric the redistribution game actually
runs and records as `bee_localstore_reserve_sample_duration_seconds`, runs at
parity on both engines across real rounds over days:

| Engine (node) | bee's own sampler, full-reserve rounds |
|---|---|
| goleveldb (bench-1) | 38 to 49 s |
| Pebble (bench-2) | 40 to 48 s (latest 43.8 s) |

So Pebble and goleveldb sample in the same time in real operation, both far inside
the sampling window (stated here first as a 190 s commit window; the code allows
about 114 blocks, 570 s at 5 s blocks and about 228 s at 2 s blocks, see #540) ([#17](https://github.com/crtahlin/wasp/issues/17)).
The read-axis verdict in [#185](https://github.com/crtahlin/wasp/issues/185), and
Pebble as the default engine, stand.

Why the first measurement misled: `/rchash` with the page cache dropped forces a
fully cold read, which does not happen between rounds in real operation. goleveldb
is insensitive to that, its cold and warm sample are both about 39 s, so its
synthetic number matched reality and the method looked sound. Pebble is
cache-sensitive, so the forced-cold path showed 90 to 217 s while its real sampler
runs about 43 s. The lesson: measure the reserve sample from bee's own sampler
metric, not from `/rchash` with `drop_caches`, which is a synthetic worst case that
exaggerates the engine difference.

One real residual remains, small: Pebble's first sample right after a cold start,
before its caches warm, is slower, about 90 to 120 s, where goleveldb's is about
39 s. Both are within the commit window, so a node that restarts just before a round
still samples in time. The disk difference between the two bench nodes was real and
is worth fixing for any future raw-throughput comparison, but it did not move the
operational sampler, which runs warm and stays about 43 s on either disk.

## Follow-up 1: compaction nudge after a mass eviction (real, not a risk now)

Pebble reclaims freed disk slowly after a mass eviction. This is a real mechanism but
not a risk in normal operation. Mass evictions on a no-doubling node are gradual, from
batch expiry and radius steps, not sudden. bench-2 sits at 65% disk with 17 GB free,
so a slow background reclaim does not outpace to disk-full. It was not tested by
forcing an eviction, because that is invasive on a live mainnet node (rule 4). Revisit
only if a node is seen filling disk because reclamation falls behind evictions,
which needs evictions that are both large and frequent together with thin disk
headroom.

## Follow-up 3: write-stall tail under saturation (not a regime a live node reaches)

Pebble showed occasional multi-second commit stalls when the write path was
saturated. A live node does not reach that regime. The global inbound rate is capped
at 1000 chunks per second ([#32](https://github.com/crtahlin/wasp/issues/32)), a few
hundred KB/s of index writes. bench-2 over 21 hours flushed and compacted about
465 MB, an average near 6 KB/s to Pebble, orders of magnitude below saturation. The
stalls were a synthetic microbench force-feeding writes far above any network rate.
This is not a question a node on a real connection reaches, as the issue anticipated.

## Disposition

All three follow-ups are resolved. Level 0 stays shallow over long uptime; the
eviction nudge and the write-stall tail are not regimes a live node reaches. The
read-axis retest [#231](https://github.com/crtahlin/wasp/issues/231) is done: measured
from bee's own sampler the two engines are at parity, so [#185](https://github.com/crtahlin/wasp/issues/185)
stands and [#231](https://github.com/crtahlin/wasp/issues/231) is closed. Both
[#198](https://github.com/crtahlin/wasp/issues/198) and
[#231](https://github.com/crtahlin/wasp/issues/231) are closed.

Generated with help of AI.
