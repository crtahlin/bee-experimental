# A stop during a long eviction or during startup must leave the store clean

Issues: [#634](https://github.com/crtahlin/wasp/issues/634) (a shutdown during a long eviction panics `pebble: closed` and leaves the store dirty) and [#635](https://github.com/crtahlin/wasp/issues/635) (a stop during startup leaves the store dirty). Related: [#399](https://github.com/crtahlin/wasp/issues/399) (closing the store under a live scan), [#428](https://github.com/crtahlin/wasp/issues/428) (Close waits for the store's close), [#623](https://github.com/crtahlin/wasp/issues/623) (paced eviction in rounds).

One spec for both because they have the same consequence and the same measurement: the sharky `.DIRTY` marker stays, and the next start runs a full recovery, measured at **2 h 17 min** on bench-1 (about 142 GB). They are fixed in different places (the storer's `Close`, and the start command), and either part can ship alone.

## Terms

- **Dirty marker:** `.DIRTY` in the sharky directory. `sharkyRecovery` creates it when the storer opens and removes it on a clean close (`pkg/storer/recover.go:37-40`). If it is present at start, the node validates every chunk before it serves anything.
- **Drain window:** how long `DB.Close` waits for background workers before it closes the store anyway: 5 s, or `ShutdownTimeout` if longer (`pkg/storer/storer.go:1109-1115`).
- **Round:** one unit of eviction, at most 1,000 items, read and deleted under the batch lock (`pkg/storer/internal/reserve/reserve.go:390-432`, `:449`).

## Problem

### #634: Close closes the store under a running round (reproduced on current `main`)

Paced eviction (#623) checks for a shutdown between rounds (`pkg/storer/evictionpace.go:254-297`) and between batches (`pkg/storer/reserve.go:341-345`), so the crash path of the original report, one iteration over a whole batch, is gone. But `Close` still closes the store when the drain window ends, whether or not a round is running (`storer.go:1117-1141`). A round that runs longer than the drain window reads a closed store.

**Reproduced (scratch test, not committed, 2026-10-10):** a disk storer (pebble), 200 chunks of one batch, the reserve worker evicting it, and a test-only stall inside the round's iteration that holds the round past the drain:

```
Close returned after 5.02s: storer closed, but bg goroutines were still running after 5s
panic: pebble: closed
github.com/cockroachdb/pebble.(*DB).newIter
pkg/storage/pebblestore.(*Store).Iterate
pkg/storer/internal/transaction.(*indexTrx).Iterate
```

How likely is a round longer than 5 s on a real node? Not measured. On the dense host, with ten nodes evicting at once and disk-wait pressure up to 78 %, a node deleted about 2,600 to 3,150 chunks per second, so a round of 1,000 took about 0.3 to 0.4 s. A round of 5 s needs under 200 deletes per second: a slower disk, a host under heavier load, or a pebble write stall. Rare, but a crash then costs hours.

Upstream (`v2.8.2` and `master`) has the same bounded drain (`storer.go:639`) and evicts a whole batch in one iteration, so the window there is much wider (reasoned from code; not reproduced on an upstream build).

### #635: a stop before the node is built exits without closing the store (from code)

The start command builds the node in the background and handles a stop as follows (`cmd/bee/cmd/start.go`):
1. A SIGINT or SIGTERM cancels the context (`:72-80`).
2. `start` returns on `ctx.Done()` while the node is still being built (`:100-110`).
3. `stop` returns at once because no node was stored yet (`:123-132`, `if val == nil { return }`).
4. `RunE` returns nil (`:184-197`), so the process exits with status 0.

The build goroutine is abandoned. If `storer.New` had already opened the store (`pkg/node/node.go:1195`), the dirty marker exists and the index store is never closed; the next start runs a full recovery. The bench-1 observation fits this path (no "node shutdown" line, exit status 0, then 2 h 17 min of recovery), but which start-up step the stop hit is a hypothesis: the marker is written only after the index store has opened (`recover.go:38-40`), so the stop came after the write-ahead-log replay, not during it as the issue first assumed. Candidates are the reserve and cache counts, the migrations and `rebuildHeldCount` (see the worst case below).

`NewBee` itself closes what it opened on an error (`node.go:566-573`, a deferred `b.Shutdown()`), but only if the process is still running when it returns. Upstream has the same structure (`start.go:115-116`).

## Hypothesis

1. With a close gate in the storer (design 1), a stop during an eviction never panics, closes the store, and leaves no dirty marker, however long a round takes, as long as each admitted unit of work returns.
2. With `stop` waiting for the build when nobody else received it (design 2) and the start-up scans honouring cancellation (design 3), a stop at any point during startup ends within the service manager's stop timeout with the store closed by `NewBee`'s own cleanup, and no dirty marker unless sharky recovery itself was interrupted (which must leave the marker).

## Design

### 1. A close gate in the storer (#634)

Instead of closing the store under live work after the drain, `Close` stops admitting new store work and waits only for work already admitted.

- **Admission counter, not a lock.** A `closing` flag (atomic) and an `inside` counter (atomic) with a channel `Close` waits on. To enter, a caller increments `inside`, then re-checks `closing`; if set, it decrements and returns `ErrDBQuit`. On leaving it decrements, and the last one out while `closing` is set signals `Close`. A `sync.RWMutex` is not used: store calls nest inside `Iterate` callbacks (`IsChunkPinnedInCollection` → `Has`, `internal/reserve/reserve.go:480-490`), and a pending writer would block the nested reader and deadlock. A `sync.WaitGroup` is not used either, because its `Add` must not race its `Wait`.
- **What is admitted is a unit of work, not a single call:**
  - a **transaction** (`Storage.Run` and `NewTransaction`) is admitted when it starts and leaves at `done()`, after its `Commit`. `Commit` releases sharky slots (`pkg/storer/internal/transaction/transaction.go:191-199`); refusing a `Release` would leak the slot for good (it is saved as used, `pkg/sharky/shard.go:158-161`, and no recovery runs after a clean close), and a `Release` after sharky has closed blocks forever (`shard.go:198-204`). Admitting the whole transaction avoids both;
  - a **top-level read** outside a transaction (`IndexStore().Get/Has/Iterate`, `ChunkStore().Get`) is admitted for the duration of the call;
  - a nested call inside an admitted unit re-enters (the counter allows it; a nested call made after `closing` is set still returns `ErrDBQuit` from its re-check, which the enclosing unit returns).
- **Order in `Close`, kept from #399 and #428:** close `quit`, run the drains as today, **then** set `closing` (after the drain, so workers still get their safe-point chance to stop first), then wait until `inside` is zero, then `dbCloser.Close()` (closed at `storer.go:1155` today).
- **Iterations stop at the next callback:** an `Iterate` running when `closing` is set returns `ErrDBQuit` from its next callback. This is not bounded in time: the engine can step over many tombstones before it calls back (for example after a large eviction). The worst case is part of the bound below.
- **Where:** the gate wraps the index store and sharky inside `storer.New` (the `DB`'s own store), not inside `initStore`, which the offline tools share (`validate.go`, `compact.go`). Sharky is created at `storer.go:610` and handed to the transaction layer as a concrete type (`transaction.go:67`), so the gate goes into the transaction layer's entry points rather than a wrapper type. The index-store wrapper must pass through `Level0Files`, `DB()` and the metrics collector, or the write-stall warning and the pebble metrics disappear; a test checks it.
- **Open failures:** the index store is closed when `sharkyRecovery` or `sharky.New` fails (`storer.go:604-607, 615-617`), which today leaves it open.
- **No new setting.**

Cost: two atomic operations per admitted unit. To check in the implementation with a benchmark of the reserve put path and `Iterate` (a regression above about 2 % is a finding).

### 2. Stop waits for the build, only if nobody else will (#635)

- `start` already reads `respC` (`cmd/bee/cmd/start.go:100-106`), and the channel holds one value (`:221-227`). So `stop` waits for the build **only if `start` did not receive the result**: one owner receives from `respC` (a small helper that records whether the result was taken and stores it), and `stop` waits only when it was not. After a build error with no signal, `start` has the result, `stop` returns at once, and #490's exit status 78 for a configuration error is unchanged.
- When `stop` does wait: the context is already cancelled, so `NewBee` returns, either with an error (its deferred `Shutdown` closes the store, `pkg/node/node.go:566-573`) or with a node (then `stop` calls `Shutdown` on it, as for a running node).
- **Exit status:** unchanged for a stop during startup (0), and an info line "stopped during startup; closing the store".
- **Recovery interrupted:** sharky recovery returns on the cancelled context (`recover.go:30`) and the marker stays, which is correct.

### 3. The worst-case wait, and what bounds it

Neither part is safe unless the stop finishes before the service manager kills the process.

- **systemd** (`packaging/bee.service`) sets no `TimeoutStopSec`, so the default 90 s applies, then SIGKILL. systemd sends one SIGTERM; the "second signal" escape exists only at a terminal. **Windows** service mode has its own stop timeout and no second signal either.
- **Part 1 worst case:** the drain (5 s) plus the longest admitted unit: a commit during a pebble write stall, or an iteration stepping over tombstones to its next callback. Neither is bounded by code; measured values come from the bench run below.
- **Part 2 worst case:** the start-up steps that do not honour cancellation run to the end: the full reserve and cache `Count`s (`internal/reserve/reserve.go:88`, `internal/cache/cache.go:53`), the migrations (`reserveRepair.go:249-254`) and `rebuildHeldCount` (`held.go:291`). On a reserve of about 31 M chunks these are likely to take minutes (hypothesis, measured below). Postage catch-up already returns on cancellation (`node.go:1304`).

**Two ways to bound it (operator decision, because option B changes the packaged unit):**
- **A. Make those scans honour cancellation** (preferred, in this change): the counts, `rebuildHeldCount` and the migration loops check the context between items and return; a cancelled count leaves its value unset, which is fine because the node is stopping; a cancelled migration must be resumable (each migration step is already idempotent or must be made so; to verify per migration in the implementation). Then part 2's wait is bounded by one item plus the in-flight transaction.
- **B. Raise `TimeoutStopSec`** in `packaging/bee.service` (for example to 10 minutes) as a safety net on top of A. It changes the packaged unit: a node that hangs on stop then takes up to 10 minutes to be killed. Not done unless the operator approves.

The spec implements A and proposes B as an option.

### Out of scope

- Avoiding recovery when nothing was written since the marker was created. A separate change.

## Tests

1. **Round outlasts the drain (#634), the reproduction, committed:** a disk storer for each engine (pebble and leveldb), the reserve worker evicting one batch, a test-only stall (a package variable in `internal/reserve`, nil in production) holding a round past a shortened drain window. Assert: no panic, `Close` returns, the dirty marker is gone, and a reopen does not run recovery. On unmodified code it panics with `pebble: closed`.
2. **Long iteration stops at close:** an `Iterate` with a blocking callback; after `Close` sets `closing` and the callback is released, the iteration returns `ErrDBQuit` at the next callback and `Close` completes.
3. **Nested calls do not deadlock:** a callback that calls `Has` (as the pinning check does) while `Close` is waiting: the nested call returns `ErrDBQuit`, the iteration ends, `Close` completes.
4. **A transaction is admitted until done:** a transaction that has written and is about to commit when `closing` is set completes its commit, and its sharky releases run before sharky closes; afterwards a reopen shows no leaked slot (the free-slot count matches the chunks).
5. **Calls after close:** top-level `Get`, `Has`, `Iterate`, a transaction start and a sharky read after `Close` return `ErrDBQuit`, with no panic or block.
6. **Gate under concurrency:** many goroutines running transactions and reads while `Close` runs, under `-race`; every call returns success or `ErrDBQuit`, and `Close` returns.
7. **Wrapper passes through:** `Level0Files`, `DB()` and the metrics collector behave as without the gate.
8. **Stop during build (#635):** the start/stop logic factored into a function taking the build as a parameter. Cases: a build that blocks until its context ends, then returns an error after closing a fake closer: `stop` waits and the closer was called. A build that returns a node as the stop arrives: `Shutdown` is called. **A build that fails before any signal:** `start` takes the error, `stop` returns at once, and the command returns the error (exit 78 for a configuration error, #490).
9. **Cancellable start-up scans:** the reserve count, the cache count and `rebuildHeldCount` return promptly on a cancelled context; each migration, cancelled midway and run again, gives the same result as uninterrupted.
10. **Recovery interrupted:** `sharkyRecovery` with a cancelled context returns and leaves the marker.

**Mutation checks** (each must make a test fail): gate removed (test 1); a lock instead of the counter (test 3 deadlocks); transactions admitted per call instead of until done (test 4); calls after close not refused (test 5); passthrough of `Level0Files` dropped (test 7); `stop` waiting when `start` already took the result (test 8 hangs); a node returned at the stop not shut down (test 8); a start-up scan ignoring cancellation (test 9).

## Measurement

**Bench-1** (single node, pebble, about 31 M chunks), three runs each:
- `systemctl stop` during an expired-batch eviction and during an `unreserve` eviction (at rate 0 and at the default 500): no panic in the journal, clean exit, next start without "sharky .DIRTY file exists";
- `systemctl stop` at 5, 20 and 60 s after a start (during the replay, during the start-up counts and migrations, after readiness) and **during postage catch-up**: next start without recovery;
- the time from each stop to the exit, against the 90 s systemd default.

Pass: no recovery after any of these stops, and every stop exits within 90 s. If any stop exceeds 90 s even with option A, that is the case for option B, put to the operator with the measured time. Today's result for comparison: one stop during eviction and one stop during startup each cost 2 h 17 min.

## Protocol impact

None. No message changes; rule 6 does not apply.

## Rollout and rollback

Both parts are internal and need no setting. Rollback is a revert of the merge commit.

## Upstream portability

The same defects exist upstream (same bounded drain, `storer.go:639`; same `stop` path, `start.go:115-116`). Test 1 needs only the storer and a stall hook, and test 8 needs the start logic factored out; whether to add `affects-upstream` is decided after test 1 is run on an upstream tree (with the hook added there), per rule 11.

Generated with help of AI.
