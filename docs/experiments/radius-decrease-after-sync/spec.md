# Storer: lower the radius again only after the puller has synced at the current radius

Issue: #696. Companion: #695 (one peer blocks the puller's recalculation).

## Terms

- **Reserve worker check:** the `thresholdTicker` case of `reserveWorker` (`pkg/storer/reserve.go:290-320`), every `reserve-wakeup-duration` (15 minutes by default, `DefaultReserveWakeUpDuration`, `pkg/node/node.go:272`).
- **Sync rate:** `Syncer.SyncRate()` (`pkg/storer/reserve.go:39-43`), the puller's rate of historical chunks over `DefaultHistRateWindow`, 15 minutes (`pkg/puller/puller.go`).
- **Acting on a radius:** the puller acts on radius r when a recalculation (`onChange`) reads r and finishes `recalcPeers` with it; only then are the bins r opens being synced.

## Problem

The check lowers the radius by one when the reserve within the radius is below half capacity and `SyncRate() == 0` (`reserve.go:311`). A sync rate of 0 is read as "synced at this radius, and still not enough to fill half the reserve". But it is also 0 when the puller has not synced at the current radius at all: right after the previous decrease, until its next recalculation (up to 5 minutes), and for as long as a recalculation is blocked (#695). The next check then lowers the radius a second step, one more than the neighbourhood needs, and the node pulls twice the content it is responsible for.

The same condition is on upstream `master` (cc5c4706e), `pkg/storer/reserve.go:176`.

### Measured (dense host, ten nodes switched from doubling 2 to 3 at once, 2026-10-09)

- **Node A:** radius 7 after the restart, 6 at minute 16, 5 at minute 31; its pull rate stayed 0 from minute 16 to minute 36, because its puller's first recalculation did not return for about 30 minutes (#695). It filled to capacity at radius 5. When its radius went back to 6 it held 33.57 M chunks, 28.55 M of them within the radius: 5.0 M chunks outside its responsibility. Hours later it was still pulling about 560 chunks/s and evicting about 517/s, one stale chunk per arriving chunk.
- **Node B, same start:** its pull rate was still slightly above 0 at the first check, so it dropped only once (minute 31); its puller acted at minute 37, before the next check. It settled at about 31.0 M chunks, all within the radius.
- In the earlier refill on the same host three nodes ended at radius 5; whether by the same sequence was not checked.

## Change

1. **The puller reports what it has acted on.** The puller records, after each recalculation, the radius it used and when it finished. It exposes this through a new optional interface in `pkg/storer`:

   ```go
   // RadiusSyncReporter is implemented by a Syncer that can tell which storage
   // radius it last acted on.
   type RadiusSyncReporter interface {
       // LastRecalc returns the storage radius used by the most recent completed
       // recalculation and when it completed. ok is false before the first one.
       LastRecalc() (radius uint8, at time.Time, ok bool)
   }
   ```

   Optional, so the existing `Syncer` interface and its mocks are unchanged and a syncer without it keeps today's behaviour.
2. **A further decrease waits for the puller.** In the check, after the stall gate (`radiusDecreaseBlocked`, unchanged and still first) and when the existing condition holds, a syncer that implements `RadiusSyncReporter` must also report `ok`, `radius == current radius`, and `time.Since(at) >= settle`. Otherwise the decrease is deferred to a later check.
   - `settle` is one sync-rate window, 15 minutes: the rate is then 0 over a whole window in which the puller was syncing at this radius, which is what "synced at this radius" was meant to say. The storer cannot import the puller (the puller imports `storer`), so `settle` is a storer option, `RadiusDecreaseSettle`, set in `node.go` from `puller.DefaultHistRateWindow`; zero keeps today's behaviour, which is what tests and other callers get.
3. **Metric and log:** `bee_localstore_radius_decrease_deferred_total{reason="puller_not_at_radius"|"puller_settling"}`, a counter, both labels created at start; a debug line with the current radius, the puller's reported radius and its age.
4. **No wire change** and no new user setting.

### Cost

A decrease can now happen at most once per settle window after the puller has acted on the radius, and only at a reserve worker check. After a decrease the puller acts within one recalculation (up to 5 minutes; about 30 seconds more with #695), the settle window adds 15 minutes, and the next check comes at a 15-minute boundary: each further step takes about 30 minutes, against 15 today. A node whose radius must fall several steps reaches it more slowly; while it syncs at a radius its rate is above 0 and today's rule would not have lowered the radius either.

### A fresh node

A fresh node starts at the radius its peers report (`waitNetworkRFunc`), its puller acts on it in the first recalculation after warm-up, and while it fills, its rate is above 0, as today. Only the steps after a filled, still under-half reserve are slower, as above. The initial fill itself is not delayed.

### Stall gate (#583)

Unchanged and checked first: while the batch store is stale or just recovered, no decrease, whatever the puller reports.

## Tests

1. **Reproduction, `TestRadiusDoesNotDropTwiceWithoutSync`.** A storer with a reserve below half capacity, a short `ReserveWakeUpDuration`, and a test-local syncer whose `SyncRate` is 0 and which never acts on a radius (it implements `LastRecalc` reporting the start radius; upstream ignores it, so the test is portable). Over several checks, the radius may drop at most one step below the reported radius. On unmodified code it drops a step at every check and the test fails.
2. **Settles, then drops:** the syncer reports the current radius with `at` older than `settle` and rate 0: the radius drops one step at the next check.
3. **Settling:** reported radius current but `at` younger than `settle`: deferred, `reason="puller_settling"`.
4. **Not at radius:** reported radius above the current one (the puller has not acted on the last decrease): deferred, `reason="puller_not_at_radius"`.
5. **No reporter:** a syncer without `LastRecalc` (the existing mock) keeps today's behaviour; and `RadiusDecreaseSettle` zero keeps today's behaviour.
6. **Stall gate first:** with the batch store stale, no decrease and no deferral counted.
7. **Puller side:** after a recalculation the puller reports the radius it used and a fresh time; before the first one `ok` is false.

**Mutation checks** (each must make a test fail): reporter check removed; `>=` instead of `==` on the radius; settle window ignored; deferral not counted; puller records the radius before `recalcPeers` instead of after.

Race detector on `pkg/storer` and `pkg/puller`; check the exit status directly.

## Upstream

Expected to affect upstream: the condition is unchanged on `master`. Check: run test 1 on an unmodified `upstream/master` worktree (the syncer stub and the storer API it uses exist there); if the radius drops a step per check, add `affects-upstream` to #696 with what was checked, and a row in `docs/UPSTREAM.md`.

## Measurement

On the dense host (ten nodes, by role), with the build carrying this change and #695, in the next switch from doubling 2 to 3 on all ten nodes at once:

- per node, the radius timeline (pass: no node goes below the radius its neighbours hold, 6 in the earlier runs; on 2026-10-09 one node reached 5, in the run before three);
- out-of-radius chunks at the end, `reserveSize - reserveSizeWithinRadius` (pass: under 1 % of capacity, against 5.0 M on one node);
- `radius_decrease_deferred_total` by reason, and the time each node needed to reach its final radius (the cost above).

Generated with help of AI.
