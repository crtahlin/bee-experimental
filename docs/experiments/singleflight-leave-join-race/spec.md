# Singleflight: fix the race when a caller leaves while another joins

Issues: #647, and #676, which this change makes unnecessary. Found while implementing #640 (#644) and confirmed in its review; #676 came from the review of #671 (#641).

## Terms

- **Singleflight:** `resenje.org/singleflight`, a duplicate-call suppression library. Callers of `Group.Do` with the same key share one running function; each caller waits in `wait` until the function finishes or its own context ends. Wasp pins v0.4.0 (`go.mod`), as upstream `master` does.
- **Leave:** a caller whose context ends returns from `wait` while the shared call goes on for other callers.
- **Join:** a caller that calls `Do` with a key whose call is already running.

## Problem

In v0.4.0, a caller leaving `wait` reads `c.shared` after releasing the group lock (`singleflight.go:86`, `return v, c.shared, err`), while a joining caller writes `c.shared = true` under that lock (`:43`). When a third caller keeps the call open, the two accesses are not ordered, and the race detector reports a data race. It concerns one bool and does not change any result, but CI runs the packages under `-race`. The race is broader than "a caller leaves": any return from `wait`, whether the call finished or the caller's context ended, races with a caller that joins a call still in the map.

**Where it shows in wasp.** Pull-sync's `collectAddrs` shares a call per (bin, start) across all requesting peers (`pkg/pullsync/pullsync.go`, `intervalsSF.Do`). A request that leaves (ended by #640 or #641, or its peer disconnects) while another peer's request for the same (bin, start) joins and a third keeps the call open hits this race. #644 avoids it only for requests that a new request ended itself: the new request waits until they have left before it joins. #676 proposes the same wait for entries `register` skipped because they were already ending. The case between different peers is not covered by either and existed before #644.

Other users of the library in wasp: retrieval (`pkg/retrieval/retrieval.go`), the storage-incentives sample (`pkg/storageincentives/agent.go`) and the transaction cache (`pkg/transaction/wrapped/cache/cache.go`). The same race applies to them: retrieval shares one flight per chunk between callers with different deadlines (a forwarded request's handler times out after `RetrieveChunkTimeout`, 30 s), and the storage-incentives sample shares one flight per anchor and depth between the agent and `/rchash` callers. Only the transaction cache reads `shared`, for its `SharedLoads` metric (`cache.go:85`).

### Reproduced (2026-10-09)

A standalone test against the library alone: one caller keeps a call open, a second joins and then its context is cancelled, while a third joins. Run 200 times under `go test -race` with Go 1.26.4:

| Library version | Exit status | Data race reports |
|---|---|---|
| v0.4.0 | 1 | 1, between `singleflight.go:86` and `:43` (`:47` in the stack) |
| v0.4.1 | 0 | 0 |

## Dependency rule and approval

`AGENTS.md` (Guidelines) forbids adding, removing or updating `go.mod` dependencies unless the task explicitly requires it or the person asking explicitly requests the change. The operator explicitly approved this dependency change on 2026-10-10: upgrade `resenje.org/singleflight` to v0.4.1. The operator also allowed the latest release; v0.4.1 is chosen for the reason in option 2.

## Options considered

1. **Upgrade the library to v0.4.1. Chosen.** v0.4.1 changes only this: `wait` copies `c.shared` into a local while it still holds the lock and returns the local. Nothing else changes.
2. Upgrade to v0.4.3, the newest. Rejected: v0.4.2 recovers a panic in the shared function and re-panics it in the waiting callers, and v0.4.3 changes `Forget`. In v0.4.0 and v0.4.1 a panic in the function crashes the node, which is the behaviour the rest of the node relies on; from v0.4.2 a panic with no caller left waiting is recovered and dropped silently, so a real fault would leave no trace. That is the reason to stay on v0.4.1. Nothing in wasp calls `Forget`.
3. Vendor or fork a fixed copy under `pkg/`, or replace singleflight in pull-sync with a local implementation. Rejected: a released upstream fix exists.
4. Keep v0.4.0 and avoid the pattern in each caller, as #644 did for its own case and #676 proposes for another. Rejected: it cannot cover callers from different peers, and it would have to be repeated in every package that uses the library.

## Change

1. `go get resenje.org/singleflight@v0.4.1`, updating `go.mod` and `go.sum`. No other dependency changes; check with `go mod tidy`, and that `go.mod` gains no new requirement.
2. **Pull-sync's wait from #644 stays in this change, with an accurate comment.** The comment today says the wait avoids the library race; that reason goes away with v0.4.1, and the wait was never ordering the subscription release either: the subscription is released by `defer unsub()` inside the singleflight goroutine (`pkg/pullsync/pullsync.go`, `collectAddrs`), which is cancelled asynchronously after the last caller leaves, and an entry's `done` does not wait for it. What the wait actually does: when the ended request was the only caller of the collection for that key, it makes the new request start a fresh `SubscribeBin` instead of joining the collection still being cancelled. When other callers keep the collection open, the new request joins it as before. The new comment says exactly that. Whether to drop the wait is a separate issue, #693, with a test that counts subscriptions (`SubscribeBinCalls()`) for a replaced same-start request with and without the wait.
3. `docs/DIFFERENCES.md`: in the #640 row (line 59) replace "because joining while a caller leaves is a data race inside the singleflight package" with the accurate description of what the wait does (above); add a row for the library upgrade.
4. **#676 is not implemented:** with the library fixed, waiting for skipped entries that are already ending only delays the new request. #676 is closed as not planned when this merges, with a comment pointing here.

No change to the wire or to settings. One metric can change: the transaction cache's `SharedLoads` (`cache.go:85`). v0.4.1 takes the flag under the lock, so it counts only joins made before the caller returned; it can count fewer shared loads than v0.4.0, not more.

## Tests

1. **The library reproduction (deterministic enough to rely on).** The standalone test above: under `-race`, `-count=200`, v0.4.0 reports the race and v0.4.1 does not. It is added to the repository as a test in a small test-only package (or as a test next to the pull-sync tests) so CI keeps it.
2. **A pull-sync reproduction between different peers (probabilistic).** Peer A's request for (bin b, start s) waits; peer B's request for (b, s) joins the shared collection; a third peer C's request for (b, s) joins as A's request leaves (its requester goes away, #641). There must be no `synctest.Wait()` between the leave and the join: with one, the race never shows. On v0.4.0 it reproduced in 11 of 100 single runs, so it runs with `-count=100` (about 99 % chance of catching the race); on v0.4.1 it was clean in 30 of 30. It is stated in the test as probabilistic.
3. The existing tests of the four packages that use the library pass under `-race`: pullsync, retrieval, storageincentives and transaction/wrapped/cache.

**Mutation check:** pin v0.4.0 again; test 1 must fail under `-race`, and test 2 should fail in nearly every `-count=100` run.

## Upstream

Upstream `master` pins the same v0.4.0. Upstream has neither the #640 nor the #641 leave path, so the pull-sync test above cannot run there meaningfully; a clean run would prove nothing. The path upstream can reach is retrieval: one flight per chunk (`pkg/retrieval/retrieval.go`, `singleflight.Do` with `flightRoute`) shared by callers with different deadlines, the forwarding handler's context timing out after `RetrieveChunkTimeout` (30 s) while another request for the same chunk joins. A retrieval test settles the `affects-upstream` label (rule 11): one caller keeps the flight open, a second joins and is ended with a cancelled context (not by waiting for `RetrieveChunkTimeout`), and a third joins as it leaves, all under `-race`. All three callers use the same kind of key: the origin suffix (`retrieval.go:147`) separates the flights of origin and forwarded requests, so mixing them would not share a flight.

**Result (2026-10-10):** the test (`pkg/retrieval/singleflight_race_test.go`, 100 iterations per run) reports the race at `singleflight.go:43` against `:86` on unmodified upstream `master` (cc5c4706e) in 3 of 3 runs, and is clean in 3 of 3 runs on the same tree with v0.4.1. #647 therefore carries `affects-upstream`, and `docs/UPSTREAM.md` has a row.

## Measurement

None on a node: the change is a dependency upgrade that removes a race report and changes no behaviour apart from the `SharedLoads` note above. CI under `-race` is the check.
