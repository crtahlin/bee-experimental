# Singleflight: fix the race when a caller leaves while another joins

Issues: #647, and #676, which this change makes unnecessary. Found while implementing #640 (#644) and confirmed in its review; #676 came from the review of #671 (#641).

## Terms

- **Singleflight:** `resenje.org/singleflight`, a duplicate-call suppression library. Callers of `Group.Do` with the same key share one running function; each caller waits in `wait` until the function finishes or its own context ends. Wasp pins v0.4.0 (`go.mod`), as upstream `master` does.
- **Leave:** a caller whose context ends returns from `wait` while the shared call goes on for other callers.
- **Join:** a caller that calls `Do` with a key whose call is already running.

## Problem

In v0.4.0, a caller leaving `wait` reads `c.shared` after releasing the group lock (`singleflight.go:86`, `return v, c.shared, err`), while a joining caller writes `c.shared = true` under that lock (`:43`). When a third caller keeps the call open, the two accesses are not ordered, and the race detector reports a data race. It concerns one bool and does not change any result, but CI runs the packages under `-race`.

**Where it shows in wasp.** Pull-sync's `collectAddrs` shares a call per (bin, start) across all requesting peers (`pkg/pullsync/pullsync.go`, `intervalsSF.Do`). A request that leaves (ended by #640 or #641, or its peer disconnects) while another peer's request for the same (bin, start) joins and a third keeps the call open hits this race. #644 avoids it only for requests that a new request ended itself: the new request waits until they have left before it joins. #676 proposes the same wait for entries `register` skipped because they were already ending. The case between different peers is not covered by either and existed before #644.

Other users of the library in wasp: retrieval (`pkg/retrieval/retrieval.go`), the storage-incentives sample (`pkg/storageincentives/agent.go`) and the transaction cache (`pkg/transaction/wrapped/cache/cache.go`). The same race applies to them whenever a caller leaves while another joins.

### Reproduced (2026-10-09)

A standalone test against the library alone: one caller keeps a call open, a second joins and then its context is cancelled, while a third joins. Run 200 times under `go test -race` with Go 1.26.4:

| Library version | Exit status | Data race reports |
|---|---|---|
| v0.4.0 | 1 | 1, between `singleflight.go:86` and `:43` (`:47` in the stack) |
| v0.4.1 | 0 | 0 |

## Options considered

1. **Upgrade the library to v0.4.1. Chosen.** v0.4.1 changes only this: `wait` copies `c.shared` into a local while it still holds the lock and returns the local. Nothing else changes.
2. Upgrade to v0.4.3, the newest. Rejected for now: v0.4.2 also recovers a panic in the shared function and re-panics it in the waiting callers, and v0.4.3 changes `Forget`. The panic change alters behaviour: in v0.4.0 a panic in the function crashes the node; from v0.4.2 a panic with no caller left waiting is recovered and dropped silently. That is a separate decision. Nothing in wasp calls `Forget`.
3. Vendor or fork a fixed copy under `pkg/`, or replace singleflight in pull-sync with a local implementation. Rejected: a released upstream fix exists.
4. Keep v0.4.0 and avoid the pattern in each caller, as #644 did for its own case and #676 proposes for another. Rejected: it cannot cover callers from different peers, and it would have to be repeated in every package that uses the library.

## Change

1. `go get resenje.org/singleflight@v0.4.1`, updating `go.mod` and `go.sum`. No other dependency changes; check with `go mod tidy`, and that `go.mod` gains no new requirement.
2. Pull-sync keeps the wait added in #644: it still makes an ended request release its subscription before the replacing request subscribes, and it is cheap. Its comment is reworded: the reason is no longer a race in the library.
3. `docs/DIFFERENCES.md`, the #640 row: replace "because joining while a caller leaves is a data race inside the singleflight package" with the remaining reason, and add a row for the library upgrade.
4. **#676 is not implemented:** with the library fixed, waiting for skipped entries that are already ending only delays the new request. #676 is closed as not planned when this merges, with a comment pointing here.

No change to the wire, to settings or to metrics.

## Tests

1. **A pull-sync reproduction between different peers.** Peer A's request for (bin b, start s) waits; peer B's request for (b, s) joins the shared collection; a third peer C's request for (b, s) joins as A's requester goes away (#641) or A's request is replaced. Run under `-race` with `-count=20`. It must report the race on v0.4.0 and pass on v0.4.1. This test also runs on an upstream `master` worktree: if it reports the race there, #647 gets the `affects-upstream` label (rule 11), and `docs/UPSTREAM.md` gets a row.
2. The existing tests of the four packages that use the library pass under `-race`: pullsync, retrieval, storageincentives and transaction/wrapped/cache.

**Mutation check:** pin v0.4.0 again; test 1 must fail under `-race`.

## Upstream

Upstream `master` pins the same v0.4.0 and uses the library the same way in pull-sync and retrieval. Whether the race is reachable in unmodified upstream pull-sync is checked by test 1 on the upstream tree (above). Upgrading the dependency would fix it there as well.

## Measurement

None on a node: the change is a dependency upgrade that removes a race report and changes no behaviour. CI under `-race` is the check.
