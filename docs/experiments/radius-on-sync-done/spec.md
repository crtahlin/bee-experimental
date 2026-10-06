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
| stalled | The worker has made no progress for longer than 10 minutes, for any reason. It may be failing in the #573 backoff, or blocked in `Sync` waiting for a peer that no longer holds the requested range: the serving side waits without a time limit for the first chunk. Or the bin has been "not started" for longer than 10 minutes. Evaluated when the state is read, from the time of the last progress or the first-seen time. | excluded, and logged once at info level |
| aborted | The worker exited without finishing: a `nextPeerInterval` error, `top == MaxUint64`, or an `addPeerInterval` failure. The bin stays marked as syncing because its live worker keeps running, so it is not restarted. | excluded, and logged once at info level |
| gone | The worker exited on `ErrPeerNotFound`, or the peer disconnected or was stopped. | removed |

A stalled worker that makes progress again is counted as running again.

**Where the state is kept.** Each in-scope peer and bin has one entry in a map owned by the puller. The map has its own small mutex, `histMtx`, with these rules:
- The lock order is `syncPeersMtx`, then `peer.mtx`, then `histMtx`.
- `histMtx` is a leaf lock: nothing else is acquired while it is held. It is never held across a wait or a network call, and the signal is sent without blocking.
- Workers take only `histMtx`, never `peer.mtx`. `stop()` waits for workers while `peer.mtx` is held, so a worker that took `peer.mtx` would deadlock.
- Code at the end of `onChange` that reads `peer.cursors` takes `peer.mtx` before `histMtx`, never the reverse.

The four places that take `histMtx` are then free of deadlock:
- workers, which take `histMtx` only;
- `syncPeerBin`, under `peer.mtx`;
- the end of `onChange`, under `syncPeersMtx`, after `recalcPeers` has waited for its `syncPeer` calls;
- `HistoricalSyncDone`, which takes `histMtx` only and is called from the reserve worker.

**Who updates which entry:**
- There is one entry per in-scope peer and bin.
- `syncPeerBin` sets the entry to running when it starts a historical worker, or to finished when the cursor is 0. The entry carries a token, a generation number that is new for each worker.
- The historical worker updates its own entry:
  - finished, on the `start > cursor` exit;
  - aborted, on the abort exits;
  - stalled, after 10 minutes of failures with no progress;
  - running again, after progress.

  Only the historical worker does this. The live worker shares the same closure, so the update is guarded by `isHistorical`.
- **A worker updates its entry only if the entry still exists and carries the worker's own token.** A worker cancelled by a radius increase can exit after its entry was removed, and must not recreate it.
- A worker that exits because its context ended writes nothing.
- The worker records the time of its last progress in the entry. Progress means a call that advanced the interval.
- `disconnectPeer` deletes all of the peer's entries after `stop()` returns, when no worker of that peer can write any more. A radius decrease disconnects every peer, so no entry from the old radius survives. This holds even when re-fetching a peer's cursors then fails.
- At the end of `onChange`, under `histMtx`:
  - entries of bins no longer in scope are removed;
  - each in-scope bin of a peer with missing or wrong cursors gets a not-started entry with its first-seen time.

Each radius step starts its stall timers again. A peer that keeps failing therefore adds up to 10 minutes to every step, not only to the first.

**The radius is published last, and recalculation is visible.** A "recalculating" flag is set at the start of `onChange` and cleared at its end. The flag and the published radius are both set and read under `histMtx`, so a reader never sees a peer half-removed. The puller publishes the radius its map belongs to at the end of `onChange`, after `recalcPeers` has returned. Until the first publication, the published value is a sentinel meaning "none", because radius 0 is valid. While the flag is set, for example while a new neighbour's `GetCursors` call is in flight, the state is not done.

**`HistoricalSyncDone(radius uint8) bool`** returns false while recalculating, or when the published radius differs. Otherwise it walks the in-scope bins of every current peer under `histMtx`, from `radius` to the highest bin for a neighbour and the PO bin for a band peer. A missing entry counts as not finished. It returns true when:
1. no in-scope bin is running or not started, after turning entries older than the bound into stalled;
2. at least one neighbour (PO >= radius) has every in-scope bin finished cleanly.

Stalled and aborted entries do not block it and do not count as a finished neighbour. Reading the map costs one pass over about 170 peers times up to 26 bins, about 4,400 entries. That is microseconds, and it runs only when the reserve worker asks.

**`HistoricalSyncChanged() <-chan struct{}`** is a channel with a buffer of 1, sent to without blocking:
- when an entry changes to finished, stalled or aborted;
- when an entry is removed;
- when the published radius changes;
- when `onChange` changes the set of not-started entries;
- when the recalculating flag is cleared, if any of the events above happened while it was set, or if `HistoricalSyncDone` was read while it was set. A completion during a recalculation, or one signalled before it but read during it, would otherwise be read as "not done" and then lost.

A plain `onChange` with no such event sends no signal. The receiver re-reads the state, so a duplicate signal does no harm.

**A bin left after `ErrPeerNotFound`.** It keeps its cancel function in `binCancelFuncs`. If the peer reconnects before `onChange` removes it, the bin is not restarted. Today's code behaves the same way. That neighbour then simply does not count as finished, which is safe.

The comment "Number of active historical syncing jobs" on the storer's `Syncer` interface describes `SyncRate()` wrongly, and is corrected.

### 2. The reserve worker lowers the radius on that state (`pkg/storer/reserve.go`)

**One function, `checkRadius(trigger)`, holds the decision.**

- **The ticker** first scans unconditionally, with `reserveWakeupScan` as today: the combined pass when a sweep is due, the count-only pass otherwise. That keeps batch reconciliation, the `ReserveSizeWithinRadius` metric and `/status` current. It then evaluates the rule below.
- **A signal or an expiry** evaluates the cheap conditions first, and stops if any fails:
  - `radius > minimumRadius`;
  - the hold times have passed;
  - `!db.IsSampling()`;
  - `syncer.HistoricalSyncDone(radius)`.

  It then decides the count as follows:
  - **Without a scan:** if `reserve.Size()`, which is current and at least the count within radius, is below the threshold, the count is below it too, and no scan is needed.
  - **Otherwise it scans,** using the count-only pass (`countChunksWithinRadius`), when one of these holds:
    - the stored count belongs to a different radius;
    - the stored count is below the threshold;
    - the trigger is an expiry, because the reserve just got smaller.
  - **Otherwise it stops.** This is the common case on a full node after a neighbour reconnects.

  Scans started by a signal or an expiry are limited to one every 2 minutes.
- **The rule:** `count < threshold(capacity)` and the conditions above, then the radius is lowered by one.

**The stored count becomes a field of `DB`,** together with the radius it was measured at. Today `reserveSizeWithinRadius` is a package-level variable, which parallel tests with several `DB`s would share.

**Fallback to today's rule.** On each ticker check, the reserve worker also evaluates today's condition, with the peer guard it lost in 2023: `count < threshold && SyncRate() == 0`, and at least one peer at or above the radius (`HasNeighbour`). Without that guard, the fallback would lower the radius of a node with no peers after an hour, because its rate is 0 only because nothing is synced. If that condition held, but the new rule did not lower the radius, on 4 ticker checks in a row (about an hour), the radius is lowered by the old rule. Then:
- it logs a warning, once for each run of 4 such checks;
- it counts the event as `result="fallback"`;
- the run count resets when the radius changes, or when a check finds the old condition false.

The fallback respects the hold after a raise and is postponed while a sample runs, like every other check.

This covers every case in which the new rule refuses but today's rule would lower the radius while a neighbour is connected, whatever the cause. In the worst case, the change therefore lowers the radius about one hour later than today would. It never stays higher forever when today's rule would not.

During a fallback, today's rule brings back its own weakness: a rate of 0 during a sampling pause, or with peers in backoff. That is acceptable, because the fallback only runs when the new rule has been refusing for an hour. A new node whose sync takes more than an hour does not reach the fallback, because its rate is above 0.

**Hold times, kept in memory:**
- 2 minutes after a decrease, and from start;
- 15 minutes after an increase made by `unreserve`.

There is no 15-minute hold from start, so a new node can make its first step as soon as its sync is done. The holds limit how often a cycle of lowering, going over capacity, raising and pulling again can repeat. Today's thresholds of 50 and 100 per cent leave little margin, because lowering the radius roughly doubles the count. The holds do not prevent such a cycle, so the churn metric watches for it.

**Triggers:**
- the ticker, every `reserve-wakeup-duration` as today;
- `HistoricalSyncChanged`;
- `evictExpiredBatches` finishing.

**Retry with one timer.** A signal-triggered check refused by the scan limit, a hold or a running sample is not dropped. One timer is armed for the earliest time it can pass. While a sample runs, the timer polls `IsSampling` every 30 seconds. While the timer is not armed, its channel is nil. A successful ticker check does not cancel a pending retry, because a second check does no harm.

**Unchanged:**
- the 50 per cent threshold;
- the raise path in `unreserve`;
- `SyncRate()`, which stays as a metric and in the playing eligibility check (`syncedWithinThreshold` in `pkg/node`).

**Expected cost:**
- **While a node fills:** a few extra scans per radius step.
- **On a full, idle node:** a neighbour that reconnects starts and finishes its historical workers, which sends signals. Each signal costs a map read and the size check. A full node's size is above the threshold and its stored count belongs to the current radius, so no scan runs.

### 3. Metrics

- `bee_localstore_radius_check_total{trigger, result}` (the storer's metrics subsystem is `localstore`):
  - `trigger` is one of `ticker`, `sync_done`, `expiry`;
  - `result` is one of `lowered`, `above_threshold`, `sync_pending`, `held`, `postponed_sampling`, `minimum`, `skipped_scan`, `fallback`.
- `bee_puller_historical_bins{state}`: in-scope peer and bin entries by state (`running`, `not_started`, `finished`, `stalled`, `aborted`).

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
7. A worker with no progress, failing or blocked, is treated as stalled after the bound, and counts again after it makes progress.
8. A peer whose `GetCursors` keeps failing is excluded after the bound. The pullsync mock needs a cursor-error option for this.
9. A peer with the wrong number of cursors is counted as not started.
10. Each abort exit (`nextPeerInterval` error, `MaxUint64`, `addPeerInterval` failure) is excluded. `ErrPeerNotFound` is removed.
11. An entry removed after its bin was cancelled is not recreated by the exiting worker, whether it exits as finished, aborted or cancelled.
11b. After a radius decrease where re-fetching a peer's cursors fails, no finished entry from the old radius remains.
11c. A completion that happens while `onChange` is recalculating is signalled when the flag is cleared.
12. A neighbour's last bin finishing between two `onChange` calls makes the state done at once, without waiting for the next recalculation.
13. A new neighbour connecting at an unchanged radius makes the state not done while its `GetCursors` call is in flight.
14. The map has no entry left for a peer after it disconnects, across disconnects, cancels and radius changes.
15. Every signal case in section 1 sends a signal, and an `onChange` that changes nothing sends none.

**Reserve worker:**
1. Historical chunks still arriving through the rate path every few seconds do not stop lowering once the work is done. This fails on today's rule.
2. With no peers, the radius is not lowered. This fails on today's rule.
3. A `sync_done` signal lowers the radius without waiting for the ticker.
4. A second signal within 2 minutes does not run a second scan, and the timer runs the check later.
5. A signal during a hold or a sample is retried by the timer, not dropped.
6. A signal-triggered check does not scan when the last count was above the threshold.
7. The hold after a raise prevents an immediate lower.
8. An expiry trigger scans even when the stored count was above the threshold.
9. The fallback lowers the radius after 4 ticker checks in a row in which the old condition held and the new rule refused, for example with a worker blocked waiting on a peer. Its count resets on a radius change.
10. The ticker scans even when a cheap condition fails.

**Existing tests.** The 17 `NewMockRateReporter` calls in `pkg/storer/*_test.go` (13 in `reserve_test.go`, 2 in `compact_test.go`, 2 in `recover_test.go`) move to a mock that also reports `HistoricalSyncDone`. The call that uses rate 1 and expects no decrease (`reserve_test.go`) gets a mock that reports "not done". `pkg/puller/mock` gains both new methods.

**Mutation checks.** Each of these is removed in turn, and each removal must fail one of the tests with a message:
- the neighbour guard;
- the radius match;
- the publication order;
- the recalculating flag;
- the signal when the flag is cleared;
- the entry token;
- the removal in `disconnectPeer`;
- the running state;
- the not-started state;
- the fallback;
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

**Recorded per radius step:** the time from the last historical chunk to the radius decrease in the log. The last chunk is when `bee_puller_synced_chunks{type="historical"}` stops rising. Expected: 15 to 45 minutes on `main`, under 3 minutes with the change, at 60-second resolution, except where a stalled peer adds its 10-minute bound.

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
