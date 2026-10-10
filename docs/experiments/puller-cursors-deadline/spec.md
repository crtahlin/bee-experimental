# Puller: a deadline on the cursors request, so one peer cannot block the recalculation

Issue: #695. Companion: #696 (the radius drops a second step while the puller has not acted on the current radius).

## Terms

- **Recalculation:** `onChange` in `manage` (`pkg/puller/puller.go:245-283`). It runs at start, on every topology change and every `recalcPeersDur` (5 minutes, `DefaultRecalcPeersDur`). It reads the storage radius, drops peers that are gone, resets intervals after a radius decrease, and calls `recalcPeers`. wasp's `bee_puller_on_change_runs` counts its starts (`:246`); upstream has no such counter.
- **Cursors request:** `GetCursors` (`pkg/pullsync/pullsync.go`), the stream `cursors` on which a peer answers with its last BinID per bin and its epoch. The puller calls it once per new sync peer, in `syncPeer` (`puller.go:336`), before it can start any bin. The remote `cursorHandler` takes no lock and answers from `ReserveLastBinIDs`.

## Problem

`recalcPeers` (`puller.go:315-329`) starts `syncPeer` for every peer in its own goroutine and then waits for all of them (`wg.Wait()`, `:328`), holding `syncPeersMtx` for the whole recalculation. `syncPeer` holds the peer's `mtx` and calls `GetCursors(ctx, ...)` with the puller's own context, which has no deadline. `NewStream` bounds only its header exchange (10 s); the `Syn` write and the `Ack` read are bounded by nothing but that context. One peer that answers slowly or never therefore holds up the whole recalculation, and while it runs the puller reacts to nothing, including a change of the storage radius: the bins a radius decrease opens are not synced until the next recalculation, and the ticker drops the ticks it misses.

The same code is on upstream `master` (cc5c4706e): `recalcPeers` waits at `pkg/puller/puller.go:253` and `syncPeer` calls `GetCursors(ctx, ...)` without a deadline at `:261`.

### Measured (dense host, ten nodes restarted at once, 2026-10-09)

On two nodes `bee_puller_on_change_runs` stayed at 1 from the end of warm-up for about 30 minutes, then moved to 2 on both within a minute of each other. Neither node synced the bins opened by its radius decrease until that second run; one of them (radius 7 to 6 at minute 16) pulled nothing until minute 37 and dropped a second step meanwhile (#696). That two nodes unblocked together suggests one peer shared by both (hypothesis).

### What can block the first run

The stuck run was the **first** one after the restart. There `prevRadius` is 0 and `p.syncPeers` is empty, so neither the radius-decrease branch (`disconnectPeer` for every peer, `resetIntervals`) nor the removal of departed peers runs. What runs, and can block, is:

1. `EachConnectedPeerRev` on the topology;
2. per peer, in parallel, `GetCursors` (no deadline);
3. per peer, under the puller-wide `intervalMtx`: `getPeerEpoch`, `resetPeerIntervals` (32 statestore deletes) and `setPeerEpoch`. Every sync worker also takes `intervalMtx` on each iteration (`nextPeerInterval`, `addPeerInterval`), so on a host with saturated disk I/O (which the dense host had during the restart) these statestore writes can form a convoy.

**That a silent `GetCursors` is the cause is a hypothesis.** Path 3 is the alternative. This spec fixes path 2 and adds the diagnostic that tells them apart (Change 4): if the dump of a long run shows goroutines in `intervalMtx` or the statestore rather than in `GetCursors`, the cause is path 3 and gets its own issue.

## Change

1. **A deadline on the cursors request.** In `syncPeer`, `GetCursors` gets a context derived from the recalculation's with a timeout `cursorsTimeout`, 30 seconds. The deadline covers the stream open, the negotiation, the write and the read, because all of them take that context. A correct peer answers in milliseconds plus a round trip; 30 seconds leaves room for a busy peer and a slow link.
2. **On a timeout or error** `syncPeer` returns as today, `peer.cursors` stays nil, and the next recalculation asks again. No backoff: the next ask is at least one recalculation later. A peer that never answers costs one stream and 30 seconds of the run per recalculation, which happens every 5 minutes, on topology changes, and after every radius decrease (a decrease re-creates all sync peers, so every peer is asked again).
3. **`recalcPeers` keeps waiting for all peers.** The wait under `syncPeersMtx` keeps a `syncPeer` from running after a later recalculation removed that peer: `disconnectPeer` stops the peer's workers and deletes it under the same mutex, and a `syncPeer` still running afterwards would start bins on a removed object, which after a radius decrease means duplicate workers for the same peer and bins. With the deadline and parallel calls, the run is bounded at about `cursorsTimeout`. Only the run itself (and tests) take `syncPeersMtx`.
4. **Visibility:**
   - `bee_puller_on_change_started_timestamp_seconds`, a gauge with the start time of the run in progress (0 when none), so a run that is still blocked is visible as `now − start`;
   - `bee_puller_on_change_duration_seconds`, a histogram of completed runs (buckets 0.1 s to 3,600 s);
   - `bee_puller_cursor_requests_failed_total{reason="timeout"|"error"}`, both labels created at start. A timeout is `cctx.Err() == context.DeadlineExceeded` on the request's own context; when the puller's context has ended (shutdown) the failure is counted as neither;
   - an **info** line naming the peer (overlay) whose cursors request timed out, with the timeout, at most once per peer per 10 minutes;
   - **a goroutine dump** when a run has been in progress for more than 2 minutes, once per run, so a blocked run shows where it waits (path 2 or path 3 above). The check runs in a separate watcher goroutine started with the puller, which reads the start gauge's value every 10 seconds, because `onChange` itself is the code that is blocked. The dump is the pprof goroutine profile with `debug=1` (stacks grouped by count), written to a file in the node's data directory (`puller-blocked-<unix time>.txt`, at most 3 kept), and the warning line gives its path; the log does not carry the dump itself.
5. **No wire change** (rule 6) and no setting: `cursorsTimeout` and the 2-minute dump threshold are constants, like `recalcPeersDur`. Tests lower them through package variables.

## Tests

1. **Reproduction, `TestRecalcNotBlockedBySilentPeer`, portable to upstream.** It uses only what upstream has: the kademlia mock with `WithEachPeerRevCalls` and `Trigger()` to drive recalculations, and a test-local `pullsync.Interface` stub. The stub's `GetCursors` blocks until its context ends for peer A and answers at once for peer B, and counts its calls per peer with atomics. The test triggers a recalculation, waits for A's first `GetCursors`, then triggers further recalculations, and asserts **lock-free** on **peer A's** call count: B gets its cursors in the first run and is never asked again, so only A's count shows whether a later recalculation ran. On fixed code A's count reaches **at least 2** within a generous deadline, polling; on unmodified code it stays **exactly 1**, because the first run never returns. No assertion takes `syncPeersMtx`: on unmodified code the first run holds it forever, so a test that took it would hang instead of failing. The timeout is set through a per-tree shim in the test's own file: on wasp a package variable, on the upstream tree (where none exists) the shim is absent and the test fails on its deadline with "peer A asked 1 time, want at least 2", which is the reproduction.
2. **`pullsync` level, `TestGetCursorsDeadline`:** with `streamtest` and a cursors handler that never answers, `GetCursors` with a context carrying a deadline returns at that deadline with `context.DeadlineExceeded`. This is what kills the mutation "deadline applied only to the stream open", which no puller-level test can, since those stub `pullsync`.
3. **Timeout counted and logged:** one recalculation with peer A silent gives `cursor_requests_failed_total{reason="timeout"}` 1, `{reason="error"}` 0, and one info line naming A; an error answer counts as `error`; a shutdown during the request counts as neither.
4. **Retry:** when A starts answering, the next recalculation gets its cursors and starts its bins.
5. **Gauge and histogram:** the start gauge is set while a run is in progress and 0 after; one histogram observation per completed run.
6. **Dump:** with the threshold lowered, a blocked run writes one profile file and one warning naming its path, not one per watcher tick; at most 3 files are kept.
7. **Radius change acted on:** with peer A silent, a radius decrease is followed by syncing the newly opened bins from B, checked by polling until it eventually holds, within a generous deadline (at least one trigger plus `cursorsTimeout`).

Real-time tests use periods in tens of milliseconds, assert "at least" counts, and poll with generous deadlines; they do not use `synctest`, because on unmodified code every goroutine is durably blocked and `synctest` would panic instead of failing.

**Mutation checks** (each must make a test fail): deadline removed (tests 1, 7); deadline applied only to the stream open (test 2); timeout counted as `error` (test 3); shutdown counted as a failure (test 3); failed request not counted or not logged (test 3); start gauge not cleared (test 5); dump written on every watcher tick (test 6); dump check moved into `onChange` (test 6, which blocks `onChange`).

Race detector on `pkg/puller` and `pkg/pullsync`; check the exit status directly.

## Upstream

Expected to affect upstream: the same code is on `master` at the lines above. Check: run test 1 on an unmodified `upstream/master` worktree (cc5c4706e or newer); if peer A's `GetCursors` count stays at exactly 1 there ("peer A asked 1 time, want at least 2"), add `affects-upstream` to #695 with what was checked, and a row in `docs/UPSTREAM.md`.

## Measurement

On the dense host (ten nodes, by role), with the build carrying this change and #696:

- Restart all ten nodes at once, twice, a day apart.
- Per node, for 2 hours after each restart:
  - the largest `now − bee_puller_on_change_started_timestamp_seconds` while a run is in progress, sampled every 15 s (pass: under 60 s, against about 30 minutes on two nodes on 2026-10-09);
  - `cursor_requests_failed_total` by reason, and the peers named in the timeout lines (is one peer shared across nodes?);
  - any goroutine profile file: where the blocked run waited.
- If runs stay long with no timeouts and the dump shows path 3, the hypothesis is refuted for that case and path 3 gets its own issue.

Generated with help of AI.
