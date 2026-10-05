# Lower the storage radius when historical sync is finished

Issue: [#588](https://github.com/crtahlin/wasp/issues/588).
Type: optimization, with two robustness fixes.

## Problem

A node lowers its storage radius, which doubles the range it stores, in one place only: `reserveWorker` in `pkg/storer/reserve.go`. It does this on a ticker, every `reserve-wakeup-duration` (15 minutes), when all three of these hold:

```go
count < threshold(db.reserve.Capacity()) && db.syncer.SyncRate() == 0 && radius > db.reserveOptions.minimumRadius
```

`SyncRate()` is the puller's count of **historical** chunks it actually stored. A historical chunk is one pulled from a peer's past range (up to the cursor read when syncing with that peer started), as opposed to a chunk that arrives live. The count is a moving window of 15 minutes (`DefaultHistRateWindow`) that also weighs the previous window (`pkg/rate`). It is exactly 0 only after 15 to 30 minutes in which no historical chunk was stored.

That makes the rule slow and fragile:

- **Slow.** After the sync for a radius is finished, the node waits 15 to 30 minutes for the rate to reach 0, then up to 15 minutes for the ticker. A doubled node fills in three steps, from radius 9 to 6. On sw-1, new doubled nodes spent roughly 90 to 110 minutes of a 6-hour fill waiting (#588).
- **A trickle blocks it.** One historical chunk every 15 to 30 minutes, for example from new or rejoining neighbours, keeps the rate above 0 and stops lowering indefinitely. This is reasoned from the code. On our full nodes it is rare: 33 historical chunks in 30 hours on one, 15 in 3 days on another.
- **Zero does not mean finished.** The rate is also 0 when:
  - there are no peers;
  - the puller is paused while a sample runs;
  - a worker walks a range the node already holds, so it stores nothing;
  - a peer keeps failing in the #573 backoff.

  Upstream's original rule also required peers to be present. That guard was dropped in 2023.

A radius that stays too high matters. Salud marks the node itself unhealthy when the neighbourhood agrees with the network but the node's committed depth does not (`pkg/salud/salud.go`), and the storage-incentives agent then skips every round. The node also stores about half of what it should for each step it is too high.

Running the ticker more often is not the answer. Each tick does a full index scan, measured at 3.24 s per 4.07 million chunks in #28, so roughly 24 s at 30 million chunks (extrapolated). A 1-minute interval kept bench-1 at 0.43 cores when idle, against 0.07 at the default (#566).

The puller is not a source of delay. A radius change reaches it at once: `Reserve.SetRadius` calls Kademlia's `SetStorageRadius`, which signals the topology channel that the puller's `manage` loop waits on (correction on #588).

## Hypothesis

Lower the radius as soon as the puller has finished the historical sync for the current radius, instead of waiting for a rate to decay and for a ticker. This removes nearly all of the wait. Fill time per doubled node should drop from about 6 hours to about 4.5. It also makes the rule correct in the two fragile cases:
- a trickle no longer blocks it, because the node waits for completion, not for silence;
- no peers, or work still pending, no longer counts as finished.

## Design

### 1. The puller knows when historical sync for a radius is finished (`pkg/puller`)

**What is tracked.** For each peer and bin that the puller syncs at the current radius, the historical work is in one of five states:

| State | Meaning | Counts as |
|---|---|---|
| finished | the historical worker exited at `start > cursor`, or there was nothing to sync (cursor 0) | done |
| running | a historical worker is running, including while it waits during a sample | pending |
| not started | the peer has no cursors yet, because `GetCursors` failed; it is retried at the next recalculation | pending |
| stalled | the worker has had consecutive failures with no progress for longer than **10 minutes** (#573 backoff), or the peer has been "not started" for longer than 10 minutes | excluded, logged once at info level |
| gone | the peer disconnected or was stopped | removed |

A stalled worker that makes progress again counts as running.

**`HistoricalSyncDone(radius uint8) bool`** returns true when all three hold:
1. the puller's current radius equals `radius`, so the state reflects that radius and not the previous one;
2. at least one connected peer has a proximity order at or above `radius` (restores the old peer guard);
3. no peer or bin is pending.

**It must not block or take `syncPeersMtx`.** That lock is held during `recalcPeers`, which makes network calls, and `disconnectPeer` waits for workers while holding it. So the state is kept as counters, updated at the points where the state changes and read atomically:
- a pending count;
- a neighbourhood peer count;
- the radius the counters belong to.

**Notification.** `HistoricalSyncChanged() <-chan struct{}` is a channel with a buffer of 1, sent to without blocking whenever the pending count reaches 0. The receiver re-reads `HistoricalSyncDone`, so a lost or duplicate signal does no harm.

**Radius change.** When the radius goes down, `onChange` disconnects every peer and starts again, which already happens today. The counters are reset for the new radius before the workers start, so the state is never "done" for a radius whose sync has not begun.

The comment on the storer's `Syncer` interface, "Number of active historical syncing jobs", describes `SyncRate()` wrongly. It is corrected.

### 2. The reserve worker lowers the radius on that signal (`pkg/storer/reserve.go`)

**New condition:**

```go
count < threshold(capacity) && syncer.HistoricalSyncDone(radius) && radius > minimumRadius && heldLongEnough(now)
```

**Hold time.** `heldLongEnough` requires at least 2 minutes since the last decrease, and at least 15 minutes since the last increase made by `unreserve`. This damps a cycle of lowering, going over capacity, raising and pulling again. The times are kept in memory; after a restart the hold starts from the start time.

**What runs the check.** The decision is moved from the ticker branch into one function, `checkRadius`, and it runs when:
- the ticker fires, every `reserve-wakeup-duration` as today, as the fallback;
- `HistoricalSyncChanged` signals;
- `evictExpiredBatches` finishes, because the reserve got smaller.

Checks started by a signal are limited to one every 2 minutes. A signal that arrives sooner sets a timer for the remaining time, instead of being dropped. A check is postponed while `db.IsSampling()`, because a full index scan during a sample competes for the disk. The ticker check is postponed the same way.

**Unchanged:**
- the 50 per cent threshold;
- the raise path in `unreserve`;
- `SyncRate()`, which stays as a metric and in the playing eligibility check (`syncedWithinThreshold` in `pkg/node`).

**Expected cost.** A few extra index scans per radius step while a node fills. On a full, idle node, about none: the pending count does not change, so no signal fires.

### 3. Metrics

- `bee_storer_radius_check_total{trigger, result}`, where:
  - `trigger` is one of `ticker`, `sync_done`, `expiry`;
  - `result` is one of `lowered`, `above_threshold`, `sync_pending`, `held`, `postponed_sampling`, `minimum`.
- `bee_puller_historical_pending`, the pending count.

## Protocol impact

None. Only when this node changes its own radius is affected. Messages, streams and protocol versions are unchanged, and `.github/protocol-freeze.lock` is untouched.

## Configuration

None. The 10-minute stall bound, the hold times and the 2-minute limit are constants. Following rule 8, each would become a setting only if a measurement shows it matters.

## Measurement

### Unit tests

In `pkg/puller` and `pkg/storer`, using the existing puller and pullsync mocks:

**The done state:**
1. Not done while a historical worker runs, even when it stores nothing.
2. Done when every worker in scope has finished.
3. Not done with no neighbourhood peer.
4. Not done for a radius other than the puller's current one.
5. A peer that keeps failing stops counting after the bound (with the bound shortened through `export_test.go`).
6. A peer whose `GetCursors` keeps failing stops counting after the bound.
7. A worker that makes progress again counts once more.

**The reserve worker:**
1. A historical trickle, one stored chunk every few seconds through the old rate path, does **not** prevent lowering once the work is done. This fails on today's rule.
2. With no peers, the radius is **not** lowered. This fails on today's rule, which lowers when the rate is 0.
3. A `sync_done` signal lowers the radius without waiting for the ticker.
4. A second signal within 2 minutes does not run a second scan, but the timer runs it later.
5. A check during sampling is postponed.
6. The hold after a raise prevents an immediate lower.

**Mutation checks.** Each condition is removed in turn, and each removal must fail one of these tests with a message:
- the peer guard;
- the radius match;
- the pending count;
- the hold;
- the rate limit;
- the sampling postponement.

### On sw-1

sw-1 is a test machine with no stake. New doubled nodes are filled, one per disk at a time: one runs `main` and one runs the change, at the same time, with different target prefixes. This is repeated three times, and the test nodes are removed between runs.

**Recorded per radius step:**
- the time from the last historical chunk (`bee_puller_synced_chunks{type="historical"}` stops rising) to the radius decrease in the log. Expected: 15 to 45 minutes on `main`, under 3 minutes with the change.

**Recorded per fill:**
- total fill time;
- the number of radius checks, from the new metric;
- any radius increase after a decrease (churn);
- the final reserve size, which must be within 1 per cent of the `main` node's.

**Success:** all three runs show the shorter step wait, there is no churn, and the reserve sizes match.

**A negative result:** the wait does not shrink, or the change causes churn. The ledger then records it, and the change is reverted.

## Rollout and rollback

The change is always on. Rolling back means running a build without it. Nothing new is stored.

## Upstream portability

The rule is unmodified in `upstream/v2.8.2` and on master. Upstream master shortened the rate window to 10 minutes (#5630, not yet released), which touches the same lines. That needs care at the next upstream sync. The issue does not carry `affects-upstream`: the fragile cases are reasoned, not reproduced. A node observed stuck above the network radius would justify the label.
