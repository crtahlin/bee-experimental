# Storage lottery: sit out a round while evicting, and pause eviction during a sample

Issue: #649. Related: #623 (paced eviction, where the pause hooks in), #583 (the postage-stall lottery gate, the pattern reused here), #651 (default eviction rate), #650 (rate measurement), #540 (2-second blocks, a shorter sample budget).

## Terms

- **Eviction run:** one call of `unreserve` (radius increase, `pkg/storer/reserve.go:502`) or `evictExpiredBatches` (batch expiry, `:323`) by the reserve worker (`:235`). Both are the only callers of `evictBatch` (`:381`), and both build their per-round hooks with `evictionHooks` (`pkg/storer/evictionpace.go:219`). `EvictBatch` (`reserve.go:412`) only records an expired batch and triggers the worker.
- **Round:** one pass of `EvictBatchBin` (`pkg/storer/internal/reserve/reserve.go`): at most `EvictionRound` (1,000) chunks read and deleted under the batch lock, then `hooks.After` under the lock, the lock released, then `hooks.Pay`.
- **Sample:** `reserveSample` (`pkg/storer/sample.go:185`), which sets `samplingInProgress` for its whole run (`:196-197`); `IsSampling` reads it (`reserve.go:606-610`). The storage incentives agent takes a sample in `handleSample` (`pkg/storageincentives/agent.go:432`); the `/rchash` debug endpoint takes one too.
- **Evicting:** an eviction run has been in progress for at least `evictingMinAge` (60 s, compiled in). Defined in section 1.

## Problem

Measured on the ten-node bench host (8 cores), all nodes switched to half capacity at once on the paced-eviction build at the default rate 0 (no limit), about 2,600 chunks/s evicted per node:

| Sample on an observer node | Duration | Chunks that failed to load |
|---|---|---|
| Before the eviction | 2 min 59 s | not recorded |
| During the eviction | did not finish within 25 minutes | - |
| During the eviction | 14 min 58 s | 5 |
| During the eviction | 9 min 27 s | 14 |
| During the eviction | 8 min 27 s | 1 |
| After the eviction | 1 min 37 s to 2 min 12 s | 4 to 7 |

The sample budget is about 570 s at 5-second blocks and about 228 s after #540. Every sample during the eviction missed it. Failed loads occur after the eviction too, so they are not attributed to it (correction posted on the issue).

Today's gates before a sample (`handleSample`, `agent.go:436-470`): frozen, not selected, not fully synced, unhealthy, insufficient funds. A refill (radius decrease) already sits out, because the node pulls and so is not fully synced. An eviction does not: the node pulls almost nothing while it evicts.

## What eviction does to a sample (reasoned from code, not verified)

- **Radius-increase eviction** deletes chunks in bins below the storage radius: `unreserve` calls `evictBatch(..., radius, ...)` with the current radius as the upper bin (`reserve.go:560`) and raises the radius only after the bins below it are empty (`:584-588`). The sample walks bins from `max(committedDepth or p, storageRadius)` up (`sampleBins`, `sample.go:651-658`). So an eviction that runs entirely before or after a sample does not change the sampled set.
- **During** a sample it can: if `unreserve` raises the radius from r to r+1 while a sample that started at radius r walks bin r, the eviction then deletes chunks in bin r under the running sample, which skips them (`LocateFailed`, `ChunkLoadFailed`). That sample can differ from its neighbours'.
- **Batch-expiry eviction** deletes chunks of expired batches in all bins, but the sample already excludes batches below the minimum balance (`batchesBelowValue`, `sample.go:218`), so those chunks are excluded whether or not they are deleted yet.

So the measured harm is **time**: a sample during a long eviction misses its deadline, and a late sample produces no commit. That outcome equals a skip, but only after the sample has competed with eviction and with serving for CPU and disk. The content hazard is the radius step during a running sample. The operator decided (2026-10-09, issue comment) that a node should not play while evicting, on the ground that neighbours evict at different paces during a network-wide event; this spec implements that decision, with the pause covering the content hazard.

## Hypothesis

1. Sitting out a round when the node has been evicting for at least a minute turns a sample that would miss its deadline into a cheap skip, and frees the CPU and disk for eviction and serving.
2. Pausing eviction between rounds while any sample runs keeps the reserve stable under the sample, so a sample that started before an eviction, or a short eviction, finishes on the same set it started on and in about its usual time on a host where this node is the only one evicting.
3. On a host with many nodes evicting at once, the pause of one node does not shorten its sample much, because the other nodes keep evicting; that case needs the rate cap (#651). Measured with #650.

## Design

### 1. Evicting: what it means and how the agent learns it

- `DB` gains `evictionStarted atomic.Int64` (Unix nanoseconds, 0 = no run). The reserve worker sets it before calling `unreserve` or `evictExpiredBatches` and clears it when the call returns, on every path (a `defer` in a small wrapper). Only the reserve worker evicts, so one value is enough.
- `func (db *DB) EvictingFor() time.Duration` returns 0 when no run is in progress, else the time since the run started. It is added to `RadiusChecker` (`pkg/storer/storer.go:168-177`), next to `IsSampling`, so the agent (whose store is `storer.Reserve`, which embeds `ReserveStore` and so `RadiusChecker`) can call it. Mocks (`pkg/storer/mock/mockreserve.go`, `mockstorer.go`) gain it with a setter for tests.
- **Evicting** = `EvictingFor() >= evictingMinAge`, with `evictingMinAge = time.Minute`.
  - **Why a minimum age:** a node at capacity evicts in short top-ups: an arrival that pushes the reserve over capacity triggers `unreserve`, which evicts at least `ReserveMinEvictCount` (1,000, `pkg/node/node.go:272`). At 500 chunks/s that takes about 2 s; at rate 0 much less. Counting those would skip rounds for no reason (a node at capacity receiving about 30 chunks/s would be "evicting" for several percent of the time at 500 chunks/s). A run older than a minute is a large eviction: a radius increase or a large expiry.
  - **Why not "reserve above capacity":** the paced mode raises its rate to the arrival rate, so the excess over capacity is not a measure of how much work remains, and the batch-expiry path runs while the reserve can be below capacity.
- A short run that is younger than a minute when the node is selected does not stop the node from playing; the pause (section 3) keeps it out of the sample.

### 2. The gate in `handleSample`

After the selection check and before `IsFullySynced` (`agent.go:454`):

```go
if d := a.store.EvictingFor(); d >= evictingMinAge {
	a.logger.Info("skipping round because node is evicting", "round", round, "evicting_for", d)
	a.metrics.SkippedWhileEvicting.Inc()
	return false, nil
}
```

- Evaluated live at the start of the sample phase, not cached per phase like `IsFullySynced` (`agent.go:229`): an eviction can start or end within a phase, and the call is a single atomic load.
- `evictingMinAge` lives in `pkg/storer` (exported) so the agent and the storer share one value.
- `SetLastSelectedRound` and `NeighborhoodSelected` are already recorded before the gate, as for the other skips, so a skipped selection stays visible in `/redistributionstate` (`lastSelectedRound` ahead of `lastPlayedRound`).
- **Not** added to the `isFullySynced` function in `pkg/node/node.go:1698-1704`, unlike the postage-stall gate (#583): that would report a node as not fully synced during every large eviction, mislead `/redistributionstate.isFullySynced` and the sync-time measurements, and hide the reason. A separate check with its own log line and metric names the reason.

### 3. The pause between rounds

`evictionHooks` (`evictionpace.go:219`) wraps `Pay` for both the paced and the unpaced mode:

```go
pay := h.Pay
h.Pay = func(n int) error {
	if err := db.waitWhileSampling(ctx, expiry); err != nil {
		return err
	}
	return pay(n)
}
```

- `waitWhileSampling` returns at once when no sample runs. Otherwise it polls `IsSampling` every 250 ms (as the puller does, `pkg/puller/puller.go:419-428`) and returns `ErrDBQuit` on `db.quit`, `errEvictionExpiry` on `expiry` and `ctx.Err()` on the context, checked in that order as `stop()` does. It counts the paused time.
- `Pay` runs with the batch lock **released** (`internal/reserve` hook contract), so a paused eviction blocks no upload, retrieval or put of that batch. The 1 ms yield between rounds (`EvictionYield`) is unchanged and independent.
- The pause comes before the paced `wait`, so a paused eviction does not hold a limiter reservation. With burst = one round, the limiter cannot save up tokens during the pause, so eviction resumes at the configured rate, not with a spike.
- A shutdown during the pause stops the eviction exactly as a shutdown between rounds does today (#407 rules: `ErrDBQuit`, no warning). A batch expiry ends an `unreserve` as today; for `evictExpiredBatches` (`expiry == nil`) a nil channel never fires, as today.
- Every sample pauses eviction, including `/rchash`. Bench scripts that call `/rchash` during an eviction measurement will see the eviction pause; the measurement plan accounts for it.
- The reserve overfills during a pause by the arrival rate times the sample duration (minutes at most). The pause cannot be infinite: a sample ends or fails, and `samplingInProgress` is cleared on every return (`sample.go:196-197`).

### 4. Metrics and logs

| Name | Type | Meaning |
|---|---|---|
| `bee_storageincentives_skipped_while_evicting` | counter | selected rounds skipped by the gate |
| `bee_localstore_eviction_paused_seconds_total` | counter | time eviction spent waiting for a sample |
| `bee_localstore_eviction_running_seconds` | gauge | `EvictingFor()` in seconds, 0 when no run |

- Info `skipping round because node is evicting` with `round` and `evicting_for`, alongside the existing skip lines.
- Debug `reserve eviction paused while a sample runs` when a pause starts, and Debug with the paused duration when it ends; the counter carries the total.
- No new `/redistributionstate` field: the skip shows as `lastSelectedRound` ahead of `lastPlayedRound`, the log line and the counter. Adding a field is an API change this issue does not need.

### 5. Interactions

- **Radius-decrease skip (#583/#633):** unrelated; it blocks a radius decrease while the batch store is stale. A radius increase while stale still evicts, and this gate then also applies.
- **Postage-stall gate (#583):** both can close; the first one checked logs. Order in `handleSample`: frozen, selected, evicting, fully synced (which includes the postage gate), healthy, funds.
- **Sampler singleflight** (`reserveSampleAndHash`, `agent.go:495`): unchanged; the pause follows `samplingInProgress`, which is set inside `reserveSample` whoever called it.
- **Puller pause (#23):** unchanged; during a sample neither pull-sync nor eviction runs.

## Cost to a staked node

Each selection that falls inside a large eviction is skipped: one round's reward chance. At the proposed default of 500 chunks/s (#651) a half-capacity eviction of about 14 M chunks lasts about 8 hours, about 38 rounds of 152 blocks at 5 s. At committed depth 9 a neighbourhood is selected about once per 512 rounds, so a node misses a selection in about 7 % of such evictions (estimate). Without the gate, the measured samples would have missed the deadline anyway, so on a dense host the gate costs nothing it would have earned; on a host where this node evicts alone, a sample with eviction paused might have finished in time, and that round is lost. The minimum age keeps routine top-ups from costing anything. Label: `stake-risk`.

## Protocol impact

None. No message, stream, or contract interaction changes (rule 6). The commit, reveal and claim transactions are the same; a skipped round sends none, as today's skips.

## Configuration

None. `evictingMinAge` (1 minute) and the 250 ms poll are compiled in. Rule 8: a setting can follow if measurement shows the minute is wrong.

## Measurement

**Unit tests** (`pkg/storer`, `pkg/storageincentives`):

1. `EvictingFor` is 0 before a run, grows during `unreserve` and `evictExpiredBatches`, and is 0 after each returns, including on an error and on `ErrDBQuit`.
2. Gate: with the mock store reporting `EvictingFor() = 2 min`, a selected `handleSample` returns `false`, logs the skip, increments the counter and does not call `ReserveSample`; at 30 s it samples; at 0 it samples.
3. Gate order: frozen and not-selected still return first; an evicting node that is also not fully synced is counted as evicting.
4. Pause: with `IsSampling` true, an eviction with several rounds deletes the first round, then waits; the evicted count stays flat while sampling is true and resumes when it turns false. Paused time is counted. Both paced and unpaced hooks.
5. Pause exits: `db.quit` during the pause returns `ErrDBQuit` and logs no warning; an expiry during the pause ends `unreserve` with `errEvictionExpiry`; a cancelled context returns its error.
6. Pause and the lock: during the pause a `Put` of the same batch completes (the batch lock is free).
7. No pause when no sample runs: a full eviction with `IsSampling` false takes no longer than today (within the 1 ms yield per round).

Tests use `synctest` for time where the package already does.

**Mutation checks** (each must make a test fail): gate removed; gate compares with `>` 0 instead of the minimum age; `evictionStarted` not cleared on an error path; pause placed in `After` (under the lock) instead of `Pay`; pause ignores `db.quit`; pause skipped in the unpaced hooks.

**Real nodes** (role names only): during the windowed runs of #650 on the ten-node host, take samples on two observer nodes and check that the observer's evicted counter stays flat during its sample, that `eviction_paused_seconds_total` grows by about the sample duration, and the sample duration with this node paused but its nine neighbours evicting. A bench node cannot be made to be selected, so the gate itself is verified by unit tests, as for #583. On the staked node, the next eviction (if any) should show `skipped_while_evicting` only for selections that fall inside it.

## Rollout and rollback

Ships with the next build to the ten-node host and later the staked node, after #651. Rollback is the previous build; nothing is stored.

## Upstream portability

The sample and eviction paths are unchanged upstream apart from wasp's paced eviction (#623), which this builds on, so the pause does not port without it; the gate ports on its own (one method on the storer, one check in `handleSample`). No `affects-upstream` label: this is policy for operating a dense host, not a defect in upstream code.
