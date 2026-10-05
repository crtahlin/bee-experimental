# Lower the storage radius when historical sync is finished

Issue: [#588](https://github.com/crtahlin/wasp/issues/588).
Type: optimization, with two robustness fixes.

## Terms

- **Storage radius:** the proximity order (number of leading address bits shared with the node) from which a node stores chunks. Lowering it by one doubles the range the node stores.
- **Committed depth:** storage radius plus `reserve-capacity-doubling`. A **doubled node** has a doubling above 0 and covers 2^doubling neighbourhoods.
- **Historical sync:** pulling a peer's chunks up to the cursor that the peer reported when syncing with it started. **Live sync** pulls what arrives after that cursor.
- **Fill:** the hours after a new node starts, while it pulls its reserve to capacity, lowering its radius step by step.
- **Salud:** the health service (`pkg/salud`). The **storage-incentives agent** (`pkg/storageincentives`) plays the redistribution rounds.

## Problem

A node lowers its storage radius in one place only: `reserveWorker` in `pkg/storer/reserve.go`. This happens on a ticker, every `reserve-wakeup-duration` (15 minutes), when all of these hold:

```go
count < threshold(db.reserve.Capacity()) && db.syncer.SyncRate() == 0 && radius > db.reserveOptions.minimumRadius
```

`SyncRate()` is the puller's count of historical chunks it actually stored. It uses a 15-minute moving window (`DefaultHistRateWindow`) that also weighs the previous window (`pkg/rate`). It is exactly 0 only after 15 to 30 minutes in which no historical chunk was stored.

That makes the rule slow and unreliable:

- **Slow.** After the sync for a radius is finished, the node waits 15 to 30 minutes for the rate to reach 0. It then waits up to 15 more minutes for the ticker. A node with doubling 3 fills in three steps, from radius 9 to 6. On sw-1, such nodes spent roughly 90 to 110 minutes of a 6-hour fill waiting (#588).
- **Rare historical chunks can stop it.** One historical chunk stored every 15 to 30 minutes keeps the rate above 0, and the radius is then never lowered. New or rejoining neighbours could cause this. This is reasoned from the code. On our full nodes such chunks are rare: 33 in 30 hours on one, 15 in 3 days on another.
- **A rate of 0 does not mean finished.** The rate is also 0 when:
  - there are no peers;
  - the puller is paused while a sample runs;
  - a worker walks a range the node already holds and stores nothing;
  - a peer keeps failing in the #573 backoff.

  Upstream's original rule also required peers to be present; that check was dropped in 2023.

**Why it matters.** When a node's radius stays too high, Salud marks the node itself unhealthy, if its neighbourhood agrees with the network but its committed depth does not (`pkg/salud/salud.go`). The storage-incentives agent then skips every round it is selected for. The node also stores about half of what it should for each step it is too high.

**Running the ticker more often would cost too much.** Each tick scans the whole reserve index: measured at 3.24 s per 4.07 million chunks in #28, so roughly 24 s at 30 million chunks (extrapolated). A 1-minute interval kept bench-1 at 0.43 cores when idle, against 0.07 at the default (#566).

**The puller is not a source of delay.** A radius change reaches it at once. `Reserve.SetRadius` calls Kademlia's `SetStorageRadius`, which signals the topology channel that the puller's `manage` loop waits on (see the correction on #588).

## Hypothesis

Lower the radius as soon as the puller has finished the historical sync for the current radius, instead of waiting for a rate to fall to 0 and for a ticker. This removes nearly all of the wait. Fill time per node with doubling 3 should drop from about 6 hours to about 4.5. It also fixes the two unreliable cases:
- occasional historical chunks no longer stop lowering, because the rule waits for the work to be complete, not for a pause;
- having no peers, or having work still pending, no longer counts as finished.

## Design

### 1. The puller tracks historical work per radius (`pkg/puller`)

**Which work counts.** At radius `r`, a peer's bins are in scope as follows:

| Peer's proximity order (PO) | Bins in scope |
|---|---|
| PO ≥ r (a neighbour) | every bin ≥ r |
| r − 2 ≤ PO < r (`maxPODelta` band) | only the bin equal to its PO |
| PO < r − 2 | none; the peer is not synced |

**States of each in-scope peer and bin:**

| State | Meaning | Counts as |
|---|---|---|
| finished | The historical worker exited at `start > cursor`, or the cursor was 0, so there was nothing to sync. | done |
| running | A historical worker is running, including while it waits during a sample. | pending |
| not started | The peer has no usable cursors. `GetCursors` failed, or it returned the wrong number of bins (`errCursorsLength`). | pending |
| stalled | The worker has failed with no progress for longer than 10 minutes in a row (#573 backoff). Or the peer has been "not started" for longer than 10 minutes. | excluded, and logged once at info level |
| aborted | The worker exited without finishing: a `nextPeerInterval` error, `top == MaxUint64`, or an `addPeerInterval` failure. The bin stays marked as syncing because its live worker keeps running, so it is not restarted. | excluded, and logged once at info level |
| gone | The worker exited on `ErrPeerNotFound`, or the peer disconnected or was stopped. | removed |

A stalled worker that makes progress again is counted as running again.

**Where the counts change.** The counts are atomic, so readers never take `syncPeersMtx`. That lock is held during `recalcPeers`, which makes network calls.

- **Running count:**
  - Incremented in `syncPeerBin`, synchronously, in the `cursor > 0` branch, before `safe.Go`. That code runs under `peer.mtx` and `syncPeersMtx`.
  - Decremented by a `defer` registered after `defer peer.wg.Done()`. Deferred calls run in reverse order, so the decrement happens before `Done`. As a result, when `disconnectPeer` finishes waiting for a peer's workers, they have all decremented.
  - A worker that becomes stalled decrements once and sets a local flag. If it makes progress again, it increments and clears the flag. Its final decrement runs only while it is counted. The count therefore cannot go negative or stay positive after the worker exits.
- **On a radius increase,** `cancelBin` does not wait for workers. Cancelled workers decrement when they exit. This only keeps the count pending a little longer, which is safe.
- **Not-started count and neighbour count:** recomputed under the lock at the end of `onChange`, after `recalcPeers` returns, and stored atomically.
  - The not-started count includes only in-scope peers whose cursors are missing or wrong and that are not yet stalled.
  - The neighbour count includes only neighbours (PO ≥ r) whose in-scope bins have all finished cleanly. That guarantees at least one complete source was synced, not merely that a peer exists.

**The radius is published last.** The puller stores the radius its counts belong to atomically, at the end of `onChange`, after `recalcPeers` has returned. Every increment for that radius has therefore already happened. Until the first publication, the stored value is a sentinel meaning "none", because radius 0 is valid.

On a radius decrease, `onChange` waits for every old worker to exit, through `disconnectPeer` and `wg.Wait`, before the new workers start. The running count is then not reset. If it is not 0 at that point, that is logged at warning level as a bug, instead of being hidden by a reset.

**`HistoricalSyncDone(radius uint8) bool`** returns true when:
1. the published radius equals `radius`;
2. the running count is 0;
3. the not-started count is 0;
4. the neighbour count is at least 1.

**`HistoricalSyncChanged() <-chan struct{}`** is a channel with a buffer of 1, sent to without blocking whenever `HistoricalSyncDone` may have become true:
- the running count reaches 0;
- the radius is published;
- a worker or peer becomes stalled;
- the neighbour count rises above 0.

The receiver re-reads the state, so a duplicate signal does no harm, and none of the cases that matter is missed.

The comment "Number of active historical syncing jobs" on the storer's `Syncer` interface describes `SyncRate()` wrongly, and is corrected.

### 2. The reserve worker lowers the radius on that state (`pkg/storer/reserve.go`)

**One function, `checkRadius(trigger)`, holds the decision.** It runs the cheap conditions first, then the scan, then the final condition:

1. **Cheap conditions, first:**
   - `radius > minimumRadius`;
   - the hold times have passed;
   - `!db.IsSampling()`;
   - `syncer.HistoricalSyncDone(radius)`.

   If any fails, it stops before scanning.
2. **Scan, only when needed.** For a signal-triggered check, it scans only if the last measured count within radius (`reserveSizeWithinRadius`) was below the threshold. That is the situation a signal is for. The ticker always scans, as today, because the scan also removes chunks of batches that no longer exist.
3. **Final condition:** `count < threshold(capacity)`, then lower by one.

**Hold times, kept in memory:**
- at least 2 minutes after a decrease;
- at least 15 minutes after an increase made by `unreserve` while the node runs.

They limit how often a cycle of lowering, going over capacity, raising and pulling again can repeat. Today's thresholds of 50 and 100 per cent leave little margin, because lowering the radius roughly doubles the count. The holds do not prevent such a cycle, so the churn metric below watches for it. After a restart, both holds start from the start time.

**Triggers:**
- **The ticker,** every `reserve-wakeup-duration` as today, as the fallback.
- **`HistoricalSyncChanged`.**
- **`evictExpiredBatches` finishing,** because the reserve got smaller. The cheap conditions run first, so frequent on-chain expiries cost no scan unless the state also says "done".

**Retry with one timer.** A signal-triggered check that is refused by the 2-minute rate limit, a hold or a running sample is not dropped. One timer is set for the earliest time it can pass. While a sample runs, the timer polls `IsSampling` every 30 seconds. Sampling runs only for rounds the node is selected for, and lasts minutes, so the timer never waits for long.

**Unchanged:**
- the 50 per cent threshold;
- the raise path in `unreserve`;
- `SyncRate()`, which stays as a metric and in the playing eligibility check (`syncedWithinThreshold` in `pkg/node`).

**Expected cost:**
- **While a node fills:** a few extra scans per radius step.
- **On a full, idle node:** neighbour reconnects make the running count go up and back to 0. Each such signal costs only the cheap conditions plus, at most, a scan when the last count was below the threshold. That does not happen on a full node, so it costs no scan.

### 3. Metrics

- `bee_localstore_radius_check_total{trigger, result}` (the storer's metrics subsystem is `localstore`):
  - `trigger` is one of `ticker`, `sync_done`, `expiry`;
  - `result` is one of `lowered`, `above_threshold`, `sync_pending`, `held`, `postponed_sampling`, `minimum`, `skipped_scan`.
- `bee_puller_historical_pending`: the running and not-started counts.
- `bee_puller_historical_excluded`: the stalled and aborted counts.

## Protocol impact

None. Only the timing of this node's own radius changes is affected. Messages, streams and protocol versions are unchanged, and `.github/protocol-freeze.lock` is untouched.

## Configuration

None. These are constants:
- the 10-minute stall bound;
- the hold times;
- the 2-minute rate limit;
- the 30-second sampling poll.

Following rule 8, each becomes a setting only if a measurement shows it matters.

## Measurement

### Unit tests

The timed tests use `testing/synctest`, as `pkg/storer/internal/events` already does.

**Puller:**
1. Not done while a worker runs, including one that stores nothing.
2. Done when every in-scope worker has finished.
3. Not done with no neighbour, or with only band peers.
4. Not done when every neighbour is stalled.
5. Not done for a radius other than the published one.
6. Not done during `recalcPeers`, before the radius is published.
7. A stalled worker stops counting after the bound, and counts again after it makes progress.
8. A peer whose `GetCursors` keeps failing is excluded after the bound. The pullsync mock needs a cursor-error option for this.
9. A peer with the wrong number of cursors is counted as not started.
10. Each abort exit (`nextPeerInterval` error, `MaxUint64`, `addPeerInterval` failure) is excluded. `ErrPeerNotFound` is removed.
11. A bin cancelled by a radius increase decrements when its worker exits.
12. The running count never goes negative under `-race`, across disconnects, cancels and radius changes.
13. Every signal case in section 1 sends a signal.

**Reserve worker:**
1. Historical chunks still arriving through the rate path every few seconds do not stop lowering once the work is done. This fails on today's rule.
2. With no peers, the radius is not lowered. This fails on today's rule.
3. A `sync_done` signal lowers the radius without waiting for the ticker.
4. A second signal within 2 minutes does not run a second scan, and the timer runs the check later.
5. A signal during a hold or a sample is retried by the timer, not dropped.
6. A signal-triggered check does not scan when the last count was above the threshold.
7. The hold after a raise prevents an immediate lower.

**Existing tests.** The reserve tests that build the worker with `NewMockRateReporter(0)` expect a decrease when the rate is 0. About 16 calls in `pkg/storer/*_test.go` move to a mock that reports `HistoricalSyncDone`. `pkg/puller/mock` gains both new methods.

**Mutation checks.** Each of these is removed in turn, and each removal must fail one of the tests with a message:
- the neighbour guard;
- the radius match;
- the publication order;
- the running count;
- the not-started count;
- each signal case;
- the hold;
- the rate limit;
- the sampling postponement;
- the scan skip.

### On sw-1

sw-1 is a test machine with no stake, so the sampling postponement is not exercised there; the unit tests cover it.

Runs, three in total:
- Two new nodes with doubling 3 fill at the same time, one per disk: one runs `main`, one runs the change. They use different target prefixes, so they do not sync from each other.
- The build that runs on each disk is swapped between runs.
- The test nodes are removed between runs.
- Metrics are read every 60 seconds.

**Recorded per radius step:** the time from the last historical chunk to the radius decrease in the log. The last chunk is when `bee_puller_synced_chunks{type="historical"}` stops rising. Expected: 15 to 45 minutes on `main`, under 3 minutes with the change, at 60-second resolution.

**Recorded per fill:**
- the total fill time;
- the radius checks and their results, from the new metric;
- any radius increase after a decrease (churn);
- the final count within radius, compared with the count reported by fully synced peers in the same neighbourhood. Each node is compared within its own neighbourhood, since different prefixes hold different amounts.

**Success:** all three runs show the shorter step wait, with no churn, and each node ends within the range of its neighbours' counts.

**A negative result:** the wait does not shrink, or churn appears. The ledger then records it, and the change is reverted.

## Rollout and rollback

The change is always on. Rolling back means running a build without it. Nothing new is stored.

## Upstream portability

The rule is unmodified in `upstream/v2.8.2` and on master. Upstream master shortened the rate window to 10 minutes (#5630, not yet released). That changes `DefaultHistRateWindow` in `puller.go` and a line in `pullsync.go`, but not the rule in `reserve.go`, so the risk of a conflict at the next sync is small. The issue does not carry `affects-upstream`: the unreliable cases are reasoned from the code, not reproduced. A node observed stuck above the network radius would justify the label.
