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

The build goroutine is abandoned. If `storer.New` had already opened the store (`pkg/node/node.go:1195`), the dirty marker exists and the index store is never closed; the next start runs a full recovery. This matches the bench-1 observation: a stop about 20 s after a start, no "node shutdown" line, exit status 0, then 2 h 17 min of recovery.

`NewBee` itself closes what it opened on an error (`node.go:566-573`, a deferred `b.Shutdown()`), but only if the process is still running when it returns. Upstream has the same structure (`start.go:115-116`).

## Hypothesis

1. With a close gate in the storer (design 1), a stop during an eviction never panics, closes the store, and leaves no dirty marker, however long a round takes, as long as each single store call returns.
2. With `stop` waiting for the build (design 2), a stop at any point during startup ends with the store closed by `NewBee`'s own cleanup, and no dirty marker unless sharky recovery itself was interrupted (which must leave the marker, as recovery is incomplete).

## Design

### 1. A close gate in the storer (#634)

Instead of closing the store under live work after the drain, `Close` stops new store work and waits only for the calls already inside the store:

- A **gate** in front of the storer's index store and sharky (one `sync.RWMutex` plus a `closing` flag). Every store call (`Get`, `Has`, `Put`, `Delete`, `Iterate`, transaction commit, sharky read and write) takes the read side for the duration of that call. `Close` sets `closing`, waits the drain window as today, then takes the write side, which waits only for calls in flight, and closes the store.
- **Calls after `closing` is set** return `ErrDBQuit` at once and do not reach the store.
- **Iterations stop at the next item:** an `Iterate` that is running when `closing` is set returns `ErrDBQuit` from its next callback, so a long scan (a reserve sample, a sweep) cannot hold the write side for minutes. The wait for the write side is then bounded by one item, not one round.
- The eviction round, the reserve worker and every caller already treat `ErrDBQuit` as a shutdown (`reserve.go:268`, `:284`, `:295`); the round's deletes return it and the worker exits.
- **Where:** a thin wrapper at the point where the storer builds its `storage.BatchStore` and sharky (`initStore`, `storer.go:318`), so `pkg/storage` and the engines do not change.
- **Close keeps its drain window and its existing order** (#399, #428): the gate is an additional step between the drains and `dbCloser.Close()`, not a replacement. The drain still gives workers their chance to stop at a safe point; the gate makes a worker that did not stop harmless.
- **No new setting.**

Cost: one uncontended read lock per store call. To check in the implementation with a benchmark of `Put` and `Iterate` (a regression above about 2 % on the reserve put path is a finding).

### 2. Stop waits for the build (#635)

- In `stop`, when no node is stored yet, wait for `respC`: the context is already cancelled, so `NewBee` returns, either with an error (its deferred `Shutdown` closes the store) or with a node (then `stop` calls `Shutdown` on it, as for a running node).
- The wait keeps the existing escape: a second signal ends the wait at once (as for `Shutdown` today, `start.go:144-149`). An operator or a service manager's stop timeout still ends a stuck build.
- **Cancellation inside the build:** the slow steps of `storer.New` must return on the cancelled context: sharky recovery already takes `ctx` (`recover.go:30`); the index store's write-ahead-log replay happens inside the engine's open and cannot be interrupted, so a stop during the replay waits for it (seconds to a minute on a large store, measured in the measurement step). The spec does not try to interrupt the replay.
- **Recovery interrupted:** if the stop arrives during sharky recovery, recovery returns on the cancelled context and the marker stays: correct, the next start must finish the recovery.
- **Exit status:** unchanged for a stop during startup (0, the operator asked for it), and a log line "stopped during startup; closing the store" so the journal shows it.
- Windows service mode uses the same `stop` (`start.go:163-175`) and gets the same behaviour.

### Out of scope

- Avoiding recovery when nothing was written since the marker was created (the marker is written at open, before any write). Noted in #635; a separate change, because deciding "nothing written" safely needs its own design.

## Tests

1. **Round outlasts the drain (#634), the reproduction above, committed:** a disk storer for each engine (pebble and leveldb), the reserve worker evicting one batch, a test-only stall (a package variable in `internal/reserve`, nil in production) holding a round past a shortened drain window. Assert: no panic, `Close` returns, the dirty marker is gone, and a reopen of the same directory does not run recovery. On unmodified code it panics with `pebble: closed`.
2. **Long iteration stops at close:** an `Iterate` over many items with a callback that blocks on a channel; `Close` sets `closing`; after release the iteration returns `ErrDBQuit` at the next item and `Close` completes.
3. **Calls after close:** `Get`, `Put`, `Has`, `Iterate` and a sharky read after `Close` return `ErrDBQuit`, with no panic.
4. **Gate under concurrency:** many goroutines calling the store while `Close` runs, under `-race`; afterwards every call returned either success or `ErrDBQuit`.
5. **Stop during build (#635):** the start/stop logic factored into a function that takes the build as a parameter (so the command is testable without a node). A build that blocks until its context ends and then returns an error after closing a fake closer: `stop` waits for it and the closer was called. A build that returns a node just as the stop arrives: `Shutdown` is called on it. A second signal ends the wait.
6. **Recovery interrupted:** `sharkyRecovery` with a cancelled context returns and leaves the marker.

**Mutation checks** (each must make a test fail): gate removed (test 1 panics); `closing` not checked in `Iterate` callbacks (test 2 does not finish within its deadline); calls after close not refused (test 3); `stop` returning when no node is stored (test 5); a node returned at the stop not shut down (test 5).

## Measurement

**Bench-1** (single node, pebble, about 31 M chunks), three runs each:
- `systemctl stop` during an expired-batch eviction and during an `unreserve` eviction (both at rate 0 and at the default 500): no panic in the journal, exit clean, next start without "sharky .DIRTY file exists".
- `systemctl stop` at 5, 20 and 60 s after a start (during the index replay, during reserve warm-up, after readiness): next start without recovery, and the time from the stop to the exit.

Pass: no recovery after any of these stops, and no stop takes longer than the replay itself plus 10 s. Today's result for comparison: one stop during eviction and one stop during startup each cost 2 h 17 min.

## Protocol impact

None. No message changes; rule 6 does not apply.

## Rollout and rollback

Both parts are internal and need no setting. Rollback is a revert of the merge commit.

## Upstream portability

The same defects exist upstream (same bounded drain, `storer.go:639`; same `stop` path, `start.go:115-116`). Test 1 needs only the storer and a stall hook, and test 5 needs the start logic factored out; whether to add `affects-upstream` is decided after test 1 is run on an upstream tree (with the hook added there), per rule 11.

Generated with help of AI.
