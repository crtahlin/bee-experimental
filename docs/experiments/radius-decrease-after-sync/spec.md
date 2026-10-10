# Storer: lower the radius again only after the puller has synced at the current radius

Issue: #696. Companion: #695 (one peer blocks the puller's recalculation).

## Terms

- **Reserve worker check:** the `thresholdTicker` case of `reserveWorker` (`pkg/storer/reserve.go:290-320`), every `reserve-wakeup-duration` (15 minutes by default, `DefaultReserveWakeUpDuration`, `pkg/node/node.go:272`).
- **Sync rate:** `Syncer.SyncRate()` (`pkg/storer/reserve.go:39-43`), the puller's rate of historical chunks over `DefaultHistRateWindow`, 15 minutes (`pkg/puller/puller.go`).
- **Recalculation:** `onChange` in the puller (`pkg/puller/puller.go:245-283`), at start, on topology changes and every 5 minutes.
- **Acted on radius r:** a completed recalculation that read r and, in that run, got cursors from and started bins for **at least one peer with proximity order `po >= r`** (a neighbour, which is where the content within the radius comes from). A recalculation in which every neighbour's cursors request failed (#695), or with no neighbour at all, has **not** acted on r.

## Problem

The check lowers the radius by one when the reserve within the radius is below half capacity and `SyncRate() == 0` (`reserve.go:311`). A sync rate of 0 is read as "synced at this radius, and still not enough to fill half the reserve". But it is also 0 when the puller has not synced at the current radius at all: right after the previous decrease, until its next recalculation (up to 5 minutes), and for as long as a recalculation is blocked (#695) or completes without any neighbour (every cursors request timed out). The next check then lowers the radius a second step, one more than the neighbourhood needs, and the node pulls twice the content it is responsible for.

The same condition is on upstream `master` (cc5c4706e), `pkg/storer/reserve.go:176`.

### Measured (dense host, ten nodes switched from doubling 2 to 3 at once, 2026-10-09)

- **Node A:** radius 7 after the restart, 6 at minute 16, 5 at minute 31; its pull rate stayed 0 from minute 16 to minute 36, because its puller's first recalculation did not return for about 30 minutes (#695). It filled to capacity at radius 5. When its radius went back to 6 it held 33.57 M chunks, 28.55 M of them within the radius: 5.0 M chunks outside its responsibility. Hours later it was still pulling about 560 chunks/s and evicting about 517/s, one stale chunk per arriving chunk.
- **Node B, same start:** its pull rate was still slightly above 0 at the first check, so it dropped only once (minute 31); its puller acted at minute 37, before the next check. It settled at about 31.0 M chunks, all within the radius.
- In the earlier refill on the same host three nodes ended at radius 5; whether by the same sequence was not checked.

## Change

1. **The puller reports since when it has acted on the current radius.** It keeps, guarded by a mutex:
   - `actedRadius`, the radius of the recalculation that last acted on a radius (definition above);
   - `actedSince`, the completion time of the **first** recalculation that acted on `actedRadius`. It is set when a recalculation acts on a radius different from `actedRadius` (or the first time), and is **not** updated by later recalculations at the same radius, which run every 5 minutes and on every topology change. A recalculation that does not act leaves both unchanged.

   It exposes this through a new optional interface in `pkg/storer`:

   ```go
   // RadiusSyncReporter is implemented by a Syncer that can tell since when it
   // has been syncing at a storage radius.
   type RadiusSyncReporter interface {
       // ActedOnRadius returns the storage radius the puller has acted on and
       // since when. ok is false until it has acted on any radius.
       ActedOnRadius() (radius uint8, since time.Time, ok bool)
   }
   ```

   Optional, so the existing `Syncer` interface and its mocks are unchanged. The storer type-asserts the syncer once, in `StartReserveWorker`; a syncer without it keeps today's behaviour. The puller exports `bee_puller_acted_radius` as a gauge.
2. **A further decrease waits for the puller.** In the check, after the stall gate (`radiusDecreaseBlocked`, unchanged and still first) and when the existing condition holds, with a reporter present:
   - `ok == false`: deferred, reason `puller_not_started` (before the first acting recalculation: during warm-up, after a restart, or while every neighbour fails);
   - `radius != current radius`: deferred, reason `puller_not_at_radius` (the puller has not acted on the last decrease);
   - `time.Since(since) < settle`: deferred, reason `puller_settling`;
   - otherwise the decrease goes ahead as today.

   `settle` is one sync-rate window, 15 minutes: the rate is then 0 over a whole window in which the puller was syncing at this radius, which is what "synced at this radius" was meant to say. The storer cannot import the puller (the puller imports `storer`), so `settle` is a storer option, `RadiusDecreaseSettle`, set in `node.go` from `puller.DefaultHistRateWindow`. **Zero disables only the time window:** the `ok` and radius checks still apply whenever a reporter is present.
3. **Metric and log:** `bee_localstore_radius_decrease_deferred_total{reason="puller_not_started"|"puller_not_at_radius"|"puller_settling"}`, a counter, all labels created at start; a debug line with the current radius, the puller's acted radius and the age of `since`.
4. **No wire change** and no new user setting.

### Cost

A further decrease can happen at most once per settle window after the puller first acted on the radius, and only at a reserve worker check.

- **A step with content to fetch:** the puller syncs at the new radius from its first acting recalculation; the settle window runs while it fills, and the rate stays above 0 throughout, so today's rule would not have lowered the radius either. The added delay is near zero.
- **A step with nothing new to fetch** (the reserve is still under half after the previous step): the puller acts within one recalculation (up to 5 minutes; about 30 seconds more with #695), then the 15-minute settle window, then the next 15-minute check: about 30 minutes per such step, against 15 today.

### A fresh node

A fresh node starts at `max(networkR, MinimumStorageRadius)`, where `networkR` is the most common committed depth among its peers (`pkg/node/node.go:1486-1501`, `pkg/salud/salud.go:315-334`). It never steps down from far above: a node with `reserve-capacity-doubling` d walks down d steps from the network's committed depth (3 on the dense host). It fills at each radius before the next step, so the settle window overlaps the fill and the initial fill is not delayed measurably; the first step waits for the puller's first acting recalculation after warm-up.

### Stall gate (#583)

Unchanged and checked first: while the batch store is stale or just recovered, no decrease and no deferral counted.

## Tests

1. **Reproduction, `TestRadiusDoesNotDropTwiceWithoutSync`, portable to upstream.** A storer with a reserve below half capacity, a short `ReserveWakeUpDuration` (exists upstream), started with `StartReserveWorker` (exists upstream) and a test-local syncer whose `SyncRate` is 0 and which implements `ActedOnRadius` reporting `ok == false` (it never acts). No new storer option is set, so `settle` is zero and only the `ok` and radius checks apply. Over several checks the radius must not drop more than one step below the start radius; with the `ok` check it does not drop at all. On unmodified code (and on upstream, which ignores `ActedOnRadius`) it drops a step at every check and the test fails.
2. **Settles, then drops:** the syncer reports the current radius with `since` older than `settle` and rate 0: the radius drops one step at the next check.
3. **Settling:** reported radius current, `since` younger than `settle`: deferred, `reason="puller_settling"`.
4. **Not at radius:** reported radius above the current one: deferred, `reason="puller_not_at_radius"`.
5. **Not started:** `ok == false`: deferred, `reason="puller_not_started"`.
6. **No reporter:** a syncer without `ActedOnRadius` (the existing mock) keeps today's behaviour.
7. **Stall gate first:** with the batch store stale, no decrease and no deferral counted.
8. **Puller side:**
   - after an acting recalculation the puller reports that radius and a fresh `since`;
   - **a later recalculation at the same radius does not move `since`**;
   - a recalculation at a new radius sets a new `since`;
   - a recalculation in which every neighbour's `GetCursors` fails, or with no neighbour, does not act: before any acting run `ok` is false, afterwards the previous values stay;
   - with a `GetCursors` stub that blocks until released, the reported radius does not change while the run is in progress (kills "recorded before `recalcPeers`").

**Mutation checks** (each must make a test fail): reporter check removed (test 1); `ok` ignored (tests 1, 5); `>=` instead of `==` on the radius (test 4); settle window ignored (test 3); `since` updated on every recalculation (test 8); acting counted without a neighbour (test 8); recorded before `recalcPeers` (test 8); deferral not counted (tests 3 to 5).

Race detector on `pkg/storer` and `pkg/puller`; check the exit status directly.

## Upstream

Expected to affect upstream: the condition is unchanged on `master`. Check: run test 1 on an unmodified `upstream/master` worktree; if the radius drops a step per check, add `affects-upstream` to #696 with what was checked, and a row in `docs/UPSTREAM.md`.

## Measurement

On the dense host (ten nodes, by role), with the build carrying this change and #695, in the next switch from doubling 2 to 3 on all ten nodes at once:

- per node, the radius timeline and `bee_puller_acted_radius` (pass: no node goes below the radius its neighbours hold, 6 in the earlier runs; on 2026-10-09 one node reached 5, in the run before three);
- out-of-radius chunks at the end, `reserveSize - reserveSizeWithinRadius` (pass: under 1 % of capacity, against 5.0 M on one node);
- `radius_decrease_deferred_total` by reason, and the time each node needed to reach its final radius (the cost above).

Generated with help of AI.
