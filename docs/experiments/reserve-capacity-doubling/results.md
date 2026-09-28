# Results: reserve capacity doubling on a real node

Issue: [#17](https://github.com/crtahlin/wasp/issues/17). Spec:
[`spec.md`](spec.md). Enabling code: the configurable cap
[#62](https://github.com/crtahlin/wasp/issues/62) and the redistribution
eligibility threshold [#219](https://github.com/crtahlin/wasp/issues/219).

**Verdict: doubling works, and the redistribution sample does not grow with it.**
A doubled node fills its reserve, holds it, settles healthy at the correct
shallower radius, and produces its reserve sample in time. The sample iterates one
neighborhood, whose size is fixed regardless of doubling, so sample duration is
independent of the doubling factor. The practical ceiling is disk, not sample time.

## Setup

- bench-1, pebble engine, node built from `cec9b84d`. Mainnet, unstaked.
- Default index tuning: `db-compaction-l0-trigger` at its default, 64 MB block
  cache. This is the slower of the two pebble configurations measured in
  [#185](https://github.com/crtahlin/wasp/issues/185) (tuned to trigger 4 the same
  sample ran about 44 s; untuned about 245 s).
- Base reserve (`d = 0`) is `DefaultReserveCapacity` = 4,194,304 chunks, about
  3.8M within radius at network radius 9.
- Each doubling level was a clean fill: wipe the reserve and reserve state, keep
  the node identity, set `reserve-capacity-doubling`, refill from empty. Keeping the
  old reserve state does not work: the stale radius plus the new doubling makes the
  committed depth deeper than the network radius and the node stays unhealthy and
  does not fill. Doubling is a fresh-reserve setting.

## The reserve fills and settles correctly

| Doubling | Within-radius reserve | Multiple of base | Storage radius | Committed depth | Healthy |
|---|---|---|---|---|---|
| d=1 | ~7.55M | 2x | 8 | 9 | yes |
| d=2 | ~15.26M | 4x | 7 | 9 | yes |

The node adopts the network radius at start, then lowers its storage radius one
step at a time as it fills (radius only decreases when the reserve is under
capacity and sync has settled). It converges to storage radius = network radius
minus the doubling, so committed depth equals the network radius and the node is
healthy. d=1 settled at radius 8, d=2 at radius 7. During the fill the node passes
through a transient "unhealthy, storage radius discrepancy" phase while the radius
is still stepping down; that clears on convergence.

## The sample is per-neighborhood, and flat across doubling

The redistribution reserve sample is taken over one neighborhood at the committed
depth. A neighborhood's size does not change with doubling, so the sample is the
same size at every level: doubling makes the node cover 2^d neighborhoods, not make
each one larger.

`rchash` at the committed depth (9), three runs per level, with the sampler's own
iterated-chunk count:

| Doubling | Total reserve | Sample TotalIterated | Sample duration (3 runs) |
|---|---|---|---|
| d=1 | ~7.55M | ~3.81M | 167 / 168 s (one cached 37 s run) |
| d=2 | ~15.26M | ~3.81M | 119 / 169 / 115 s |

`TotalIterated` is ~3.81M at both levels and does not track the total reserve
(7.55M then 15.26M). The sample iterates one neighborhood, which is the same size
whether the total store is 2x or 4x. Sample duration is flat too: d=2 is if
anything a little faster than d=1, and the 115 to 170 s spread is compaction and
cache-warmth noise, not a doubling trend. There is no 2^d scaling and no measurable
penalty from the deeper index of the larger total store.

## What limits doubling

Not the sample. The sample fits the commit-phase window (152-block round, three
38-block phases, about 190 s at 5 s blocks) even at bench-1's untuned setting, and
tuned to trigger 4 it is about 44 s with wide headroom.

(Correction, 2026-09-28, #540: the round has phases of 38, 38 and 76 blocks, and
the sampling window runs from the start of the claim phase to the start of the
next reveal, about 114 blocks: 570 s at 5 s blocks, about 228 s at the 2 s blocks
Gnosis Chain is moving to. The conclusion stands.)

The limits are elsewhere and scale with the total reserve, not the per-neighborhood
sample:

- **Disk.** The store grows 2^d. bench-1's 492 GB holds up to about d=4 (~60M
  chunks, ~320 GB). This is the binding constraint on this hardware.
- **Fill time.** Fill is sync-bound and the reserve is 2^d larger, so each level
  takes proportionally longer to reach full.
- **Maintenance over the whole reserve.** The eviction and count-within-radius
  scans are over the full 2^d reserve; at high enough doubling these could contend
  with the sampler, but no such effect appeared at d=1 or d=2.

## Recommendation

Raising `--max-reserve-capacity-doubling` is safe from the redistribution
sample's point of view: the sample does not grow with doubling and stays inside the
round window. An operator should size the doubling to their disk, not to a fear of
sampling too slowly, and should tune `db-compaction-l0-trigger` to 4 for sample-time
headroom. d=1 and d=2 both filled, held, and sampled cleanly on real hardware; d=3
and d=4 are reachable on this disk and are expected to behave the same for the
sample, bounded by fill time and disk.

Generated with help of AI.
