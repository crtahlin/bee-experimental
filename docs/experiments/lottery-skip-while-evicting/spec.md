# Storage lottery: sit out a round while evicting, and pause eviction during a sample

Issue: #649. Related: #623 (paced eviction, where the pause hooks in), #583 (the postage-stall lottery gate, the pattern reused here), #651 (default eviction rate), #650 (rate measurement), #540 (2-second blocks, a shorter sample budget).

## Terms

- **Eviction run:** one call of `unreserve` (radius increase, `pkg/storer/reserve.go:502`) or `evictExpiredBatches` (batch expiry, `:323`) by the reserve worker (`:235`). Both are the only callers of `evictBatch` (`:381`), and both build their per-round hooks with `evictionHooks` (`pkg/storer/evictionpace.go:219`). `EvictBatch` (`reserve.go:412`) only records an expired batch and triggers the worker.
- **Round:** one pass of `EvictBatchBin` (`pkg/storer/internal/reserve/reserve.go`): at most `EvictionRound` (1,000) chunks read and deleted under the batch lock, then `hooks.After` under the lock, the lock released, then `hooks.Pay`.
- **Sample:** `reserveSample` (`pkg/storer/sample.go:185`), which sets `samplingInProgress` for its whole run (`:196-197`); `IsSampling` reads it (`reserve.go:606-610`). The storage incentives agent takes a sample in `handleSample` (`pkg/storageincentives/agent.go:432`); the `/rchash` debug endpoint takes one too.
- **Eviction episode:** a stretch of eviction work that may span several reserve-worker runs. Defined in section 1.
- **Evicting:** an eviction episode has been open for at least `evictingMinAge` (60 s, compiled in), counting active eviction time only. Defined in section 1.

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

- **Radius-increase eviction** deletes chunks in bins below the storage radius: `unreserve` calls `evictBatch(..., radius, ...)` with the current radius as the upper bin (`reserve.go:560`) and raises the radius after the bins below it are empty enough (`:584-588`). The sample walks bins from `max(committedDepth or p, storageRadius)` up (`sampleBins`, `sample.go:651-658`).
- **On a default node this collides with a running sample.** `maxAllowedDoubling = 1` (`pkg/node/node.go:274`), and with no doubling the committed depth equals the storage radius, so the sample starts at bin r, the storage radius. When `unreserve` raises the radius from r to r+1 during that sample, its next rounds delete chunks in bin r, which the sample is walking. The sample skips them (`LocateFailed`, `ChunkLoadFailed`) and can differ from its neighbours'. On a doubled node (committed depth above the radius) the same step deletes outside the sampled range. So an eviction that runs entirely before or after a sample does not change the sampled set, but a radius step **during** a sample does, on the default configuration.
- **Batch-expiry eviction** deletes chunks of expired batches in all bins. `batchstore.cleanup` evicts the batch and then deletes it from the batch store (`pkg/postage/batchstore/store.go:328-336`), so a sample rejects those chunks through the failed stamp load (`sample.go:474`) or `validStamp` (`:490`), not through `batchesBelowValue`. They do not change the sample result, but each costs the sample a read.

So there are two harms: **time** (a sample during a long eviction misses its deadline; measured) and **content** (a radius step during a sample, on a default node; reasoned). The operator decided (2026-10-09, issue comment) that a node should not play while evicting, since neighbours evict at different paces during a network-wide event. The gate implements that decision; the pause and the barrier (section 3) remove the content hazard for a sample that is already running.

## Hypothesis

1. Sitting out a round when the node has been evicting for at least a minute turns a sample that would miss its deadline into a cheap skip, and frees the CPU and disk for eviction and serving.
2. With the pause and the barrier, no eviction round and no radius step runs while a sample runs, so a sample sees the reserve it started on. On a host where this node is the only one evicting, the sample also takes about its usual time.
3. On a host with many nodes evicting at once, one node's pause does not shorten its own sample much, because the other nodes keep evicting; that case needs the rate cap (#651). Measured with #650.

## Design

### 1. Evicting: episodes, and how the agent learns it

**Why per-run state is not enough.** `unreserve` returns on a batch expiry (`reserve.go:550-552`, `:567-569`) and when it reaches its target (`:578-580`); the worker then starts again on the next trigger (`:274-288`). A flag set and cleared per run would restart the clock on every interruption, so a long eviction interrupted by expiries more often than once a minute would never count, and each restart would open the gate for a minute.

**Episode.** `DB` keeps an eviction episode state, owned by the reserve worker (the only goroutine that evicts) and published through atomics for readers:

- **Opens** when an eviction run (`unreserve` or `evictExpiredBatches`) starts and no episode is open.
- **Stays open** across runs: a run that ends while work remains (`EvictionTarget() > 0`, or an `expiredBatchItem` still recorded) does not close it.
- **Closes** when a run ends with `EvictionTarget() == 0` and no expired batch pending, or when no run has been active for `episodeIdleGrace` (10 s, compiled in). The grace covers the gap between a run that ended for an expiry and the next run the worker starts.
- **Active time** is the sum of time spent inside runs of the episode, **excluding** time paused for a sample (section 3). Pausing must not turn a short top-up into "evicting": a 2-second top-up paused by a 5-minute sample stays at 2 s.

`func (db *DB) EvictingFor() time.Duration` returns the active time of the open episode, or 0. It is added to `storer.Reserve` (`pkg/storer/storer.go:142-148`), the agent's store type, not to `RadiusChecker`, which also serves the puller, salud and the API. Mocks gain it with a setter.

**Evicting** = `EvictingFor() >= evictingMinAge`, with `evictingMinAge = time.Minute`.
- **Why a minimum age:** a node at capacity evicts in short top-ups: an arrival that pushes the reserve over capacity triggers `unreserve`, which evicts at least `ReserveMinEvictCount` (1,000, `pkg/node/node.go:272`), about 2 s at 500 chunks/s. Counting those would skip rounds for no reason. An episode with a minute of active eviction is a large eviction: a radius increase or a large expiry.
- **Why not "reserve above capacity":** the paced mode raises its rate to the arrival rate, so the excess over capacity does not measure remaining work, and the batch-expiry path runs while the reserve can be below capacity.

A short episode that is younger than a minute when the node is selected does not stop it from playing; the pause and the barrier keep it out of the sample.

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

### 3. No eviction while a sample runs: pause, barrier and radius step

**Sampling counter.** `samplingInProgress` becomes a counter (`atomic.Int32`, incremented at the start of `reserveSample`, decremented on every return; `IsSampling` is `> 0`). Today it is a boolean (`storer.go:787`, `sample.go:196-197`), so when two samples overlap (an `/rchash` with another anchor while the agent samples) the first to finish clears it while the other still runs. The puller's pause (`puller.go:420`) benefits from the same change.

**Barrier.** `DB` gains `evictionMu sync.Mutex`, used only for this:
- Eviction holds it around each `evictRound` (read and delete of one round) and around `radius++; SetRadius` in `unreserve` (`reserve.go:584-588`). It is **never** held across `Pay`, the pause or the rate-limiter wait.
- Under the lock, before the round or the radius step, eviction checks `IsSampling()`. If a sample runs, it releases the lock and pauses (below), then re-checks.
- `reserveSample` increments the counter, then locks and unlocks `evictionMu` once, then reads `StorageRadius()` (`sample.go:243`). The lock-unlock waits for at most one round (1,000 chunks) or one radius step already in progress; after it, every later round and step sees the counter and waits.
- This cannot deadlock: the sample takes the lock once and holds nothing else while waiting, and eviction never waits for the sample while holding it.

The check before each round covers the first round of the next `evictBatch` after a radius step, which today runs before any `Pay` (`Pay` follows only a round that deleted something), and a sample that starts between a `Pay` and the next round.

The round check needs a hook into `EvictBatchBin`: `EvictionHooks` gains `Before func() (release func(), err error)`, called before each round outside the batch lock. The storer's `Before` takes `evictionMu`, pauses while sampling, and returns the unlock; `EvictBatchBin` calls `Before` before taking the batch lock and `release` after releasing it, so the order is fixed: `evictionMu` first, the batch lock second, released in reverse. `Pay` runs after `release`.

**Pause.** `waitWhileSampling(ctx, expiry)` polls `IsSampling` every 250 ms (as the puller does, `puller.go:419-428`) and returns `ErrDBQuit` on `db.quit`, `errEvictionExpiry` on `expiry` and `ctx.Err()` on the context, in that order, as `stop()` does. It counts paused time and excludes it from the episode's active time.

- **Bounds:** the agent's sample context ends at the next reveal phase (`agent.go:157`), about 570 s at 5-second blocks. A `/rchash` sample is bounded only by its HTTP request (`pkg/api/rchash.go:125`). A sample ends or fails and the counter is decremented on every return.
- **Cap:** a single pause is capped at `maxEvictionPause` (15 minutes, compiled in). When it is reached, eviction logs a Warning (`reserve eviction resumed after waiting 15m for a sample`) and continues; the barrier then lets rounds run during that sample. This keeps a stuck or very slow sample from holding the reserve over capacity indefinitely.
- **Overfill:** pull-sync is paused while a sample runs (#23), so only push-sync arrivals add to the reserve during a pause: minutes of uploads at most.
- **Locks:** the pause runs with the batch lock and `evictionMu` released, so uploads, retrievals and puts of that batch proceed. The 1 ms yield between rounds (`EvictionYield`) is unchanged.
- **Paced mode:** the pause runs before the limiter wait, so no reservation is held during it. The limiter refills during a pause up to one burst (`evictionpace.go:81`, burst = one round), so the first round after a pause runs without waiting, then the configured rate applies. The worker step-down compares deletion rate with wall-clock time (`:169`); paused time is excluded from that measurement, so a pause is not counted as spare capacity.
- A shutdown during the pause stops the eviction as a shutdown between rounds does today (#407: `ErrDBQuit`, no warning). A batch expiry ends an `unreserve` as today (it returns nil on `errEvictionExpiry`, `reserve.go:567-569`); for `evictExpiredBatches` (`expiry == nil`) a nil channel never fires, as today.
- Every sample pauses eviction, including `/rchash`; bench scripts that call `/rchash` during an eviction measurement see the eviction pause, and the measurement plan accounts for it.

### 4. Metrics and logs

| Name | Type | Meaning |
|---|---|---|
| `bee_storageincentives_skipped_while_evicting` | counter | selected rounds skipped by the gate |
| `bee_localstore_eviction_paused_seconds_total` | counter | time eviction spent waiting for a sample |
| `bee_localstore_eviction_pause_capped_total` | counter | pauses that reached `maxEvictionPause` |
| `bee_localstore_eviction_running_seconds` | gauge (`GaugeFunc` reading `EvictingFor()`, so it is never stale) | active time of the open eviction episode, 0 when none |

- Info `skipping round because node is evicting` with `round` and `evicting_for`, alongside the existing skip lines.
- Debug `reserve eviction paused while a sample runs` when a pause starts, and Debug with the paused duration when it ends; the counter carries the total.
- No new `/redistributionstate` field: the skip shows as `lastSelectedRound` ahead of `lastPlayedRound`, the log line and the counter. Adding a field is an API change this issue does not need.

### 5. Interactions

- **Radius-decrease skip (#583/#633):** unrelated; it blocks a radius decrease while the batch store is stale. A radius increase while stale still evicts, and this gate then also applies.
- **Postage-stall gate (#583):** both can close; the first one checked logs. Order in `handleSample`: frozen, selected, evicting, fully synced (which includes the postage gate), healthy, funds.
- **Sampler singleflight** (`reserveSampleAndHash`, `agent.go:495`): unchanged; the pause follows `samplingInProgress`, which is set inside `reserveSample` whoever called it.
- **Puller pause (#23):** unchanged; during a sample neither pull-sync nor eviction runs.

## Cost to a staked node

Each selection that falls inside a large eviction is skipped: one round's reward chance. At the proposed default of 500 chunks/s (#651) a half-capacity eviction on a node with `reserve-capacity-doubling: 3` (capacity 4,194,304 x 8, about 33.5 M chunks, so about 14 to 17 M evicted when it halves) lasts about 8 hours, about 38 rounds of 152 blocks at 5 s. At committed depth 9 a neighbourhood is selected about once per 512 rounds, so a node misses a selection in about 7 % of such evictions (estimate). Without the gate, the measured samples would have missed the deadline anyway, so on a dense host the gate costs nothing it would have earned; on a host where this node evicts alone, a sample with eviction paused might have finished in time, and that round is lost. The minimum age keeps routine top-ups from costing anything. Label: `stake-risk`.

## Protocol impact

None. No message, stream, or contract interaction changes (rule 6). The commit, reveal and claim transactions are the same; a skipped round sends none, as today's skips.

## Configuration

None. `evictingMinAge` (1 minute) and the 250 ms poll are compiled in. Rule 8: a setting can follow if measurement shows the minute is wrong.

## Measurement

**Unit tests** (`pkg/storer`, `pkg/storageincentives`):

1. Episode: `EvictingFor` is 0 before any run, grows during `unreserve` and `evictExpiredBatches`, and is 0 after the work is done, including after an error and `ErrDBQuit`.
2. Episode across interruptions: an `unreserve` interrupted by a batch expiry, then restarted, keeps one episode and its active time keeps growing; the episode closes after the idle grace when no run follows.
3. Episode and pauses: a top-up paused for longer than a minute by a sample keeps an active time under a minute.
4. Gate: with the mock store reporting `EvictingFor() = 2 min`, a selected `handleSample` returns `false`, logs the skip, increments the counter and does not call `ReserveSample`; at 30 s and at 0 it samples.
5. Gate order: frozen and not-selected still return first; an evicting node that is also not fully synced is counted as evicting.
6. Round check: with the sampling counter above 0, no round runs, including the first round of a new `evictBatch`; the evicted count stays flat and resumes after the sample.
7. Radius step: with a sample running, `unreserve` does not call `SetRadius` until it ends.
8. Barrier: a sample that starts while a round is in progress waits for that round only; the round after it does not run until the sample ends (hook to hold a round open).
9. Overlapping samples: two samples overlap; after the first ends, eviction stays paused until the second ends.
10. Pause exits: `db.quit` during the pause returns `ErrDBQuit` and logs no warning; an expiry during a pause in `unreserve` ends it, which then returns nil; a cancelled context returns its error.
11. Pause cap: a sample longer than `maxEvictionPause` (shortened in the test) lets eviction resume with the Warning and the counter.
12. Locks: during the pause a `Put` of the same batch completes.
13. No sample: a full eviction takes no longer than today (within the 1 ms yield per round).

Tests use `synctest` for time where the package already does.

**Mutation checks** (each must make a test fail): gate removed; gate compares with `> 0` instead of the minimum age; episode closed at the end of every run; paused time counted as active time; round check removed (pause only in `Pay`); sampling check before `SetRadius` removed; `reserveSample` skips the lock-unlock; sampling counter turned back into a boolean; pause cap removed; pause ignores `db.quit`; pause skipped in the unpaced hooks.

**Real nodes** (role names only): during the windowed runs of #650 on the ten-node host, take samples on two observer nodes and check that the observer's evicted counter and storage radius stay flat during its sample, that `eviction_paused_seconds_total` grows by about the sample duration, and the sample duration with this node paused but its nine neighbours evicting. A bench node cannot be made to be selected, so the gate itself is verified by unit tests, as for #583. On the staked node, the next eviction (if any) should show `skipped_while_evicting` only for selections that fall inside it.

## Rollout and rollback

Ships with the next build to the ten-node host and later the staked node, after #651. Rollback is the previous build; nothing is stored.

## Upstream portability

The sample and eviction paths are unchanged upstream apart from wasp's paced eviction (#623), which this builds on, so the pause does not port without it; the gate ports on its own (one method on the storer, one check in `handleSample`); the barrier and the radius-step check could port with a simpler pause. No `affects-upstream` label: this is policy for operating a dense host, not a defect in upstream code.
