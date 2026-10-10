# Puller: a deadline on the cursors request, so one peer cannot block the recalculation

Issue: #695. Companion: #696 (the radius drops a second step while the puller has not acted on the current radius).

## Terms

- **Recalculation:** `onChange` in `manage` (`pkg/puller/puller.go:245-292`). It runs at start, on every topology change and every `recalcPeersDur` (5 minutes, `DefaultRecalcPeersDur`). It reads the storage radius, drops peers that are gone, resets intervals after a radius decrease, and calls `recalcPeers`. `bee_puller_on_change_runs` counts its starts (`:246`).
- **Cursors request:** `GetCursors` (`pkg/pullsync/pullsync.go`), the stream `cursors` on which a peer answers with its last BinID per bin and its epoch. The puller calls it once per new sync peer, in `syncPeer` (`puller.go:336`), before it can start any bin.

## Problem

`recalcPeers` (`puller.go:315-329`) starts `syncPeer` for every peer in its own goroutine and then waits for all of them (`wg.Wait()`, `:328`), holding `syncPeersMtx` for the whole recalculation. `syncPeer` holds the peer's `mtx` and calls `GetCursors(ctx, ...)` with the puller's own context, which has no deadline. `GetCursors` opens a stream, writes the `Syn` and reads the `Ack` with that context: nothing bounds the stream open, the protocol negotiation or the read. One peer that answers slowly or never therefore holds up the whole recalculation, and while it runs the puller reacts to nothing, including a change of the storage radius: the bins a radius decrease opens are not synced until the next recalculation, and the ticker drops the ticks it misses.

The same code is on upstream `master` (cc5c4706e): `recalcPeers` waits at `pkg/puller/puller.go:253` and `syncPeer` calls `GetCursors(ctx, ...)` without a deadline at `:261`.

### Measured (dense host, ten nodes restarted at once, 2026-10-09)

On two nodes `bee_puller_on_change_runs` stayed at 1 from the end of warm-up for about 30 minutes, then moved to 2 on both within a minute of each other. Neither node synced the bins opened by its radius decrease until that second run; one of them (radius 7 to 6 at minute 16) pulled nothing until minute 37 and dropped a second step meanwhile (#696).

Which peer held the run up is not known: the cursors request logs at debug level only. **That a single slow `GetCursors` is the cause is a hypothesis** consistent with the counter and with the code above. The measurement below confirms or refutes it; if recalculations stay long with no cursor timeouts, the next suspect inside `onChange` is `disconnectPeer`, which waits for a peer's workers (`syncPeer.stop`, `peer.wg.Wait()`).

## Change

1. **A deadline on the cursors request.** In `syncPeer`, the call becomes `GetCursors` with a context derived from the recalculation's with a timeout `cursorsTimeout`, 30 seconds. The `cursors` handler answers at once from `ReserveLastBinIDs` (one index read per bin), so a correct peer answers in milliseconds plus a round trip; 30 seconds leaves room for a busy peer and a slow link. The deadline covers the stream open, negotiation, write and read, because all of them take that context.
2. **On a timeout or error** `syncPeer` returns as today ("sync peer failed", debug), `peer.cursors` stays nil, and the next recalculation asks again. No backoff is added: the next ask is at least one recalculation later (up to 5 minutes), and a peer that never answers costs one stream and 30 seconds per recalculation.
3. **`recalcPeers` keeps waiting for all peers.** The wait under `syncPeersMtx` is what keeps a `syncPeer` from running after the next recalculation removed that peer (`disconnectPeer` stops the peer's workers and deletes it under the same mutex; a `syncPeer` still running afterwards could start bins on a removed peer that nothing will stop). With the deadline, the wait is bounded by about `cursorsTimeout`, since the `syncPeer` calls run in parallel. Removing the wait would need a "stopped" flag on `syncPeer`, checked under `peer.mtx`, and is not needed to fix the measured fault.
4. **Metrics:**
   - `bee_puller_cursor_requests_failed_total{reason="timeout"|"error"}`, a counter; both labels created at start so the series export 0.
   - `bee_puller_on_change_duration_seconds`, a histogram of how long each recalculation took (buckets 0.1 s to 3,600 s), so a blocked recalculation is visible directly instead of by a counter that stops moving.
5. **No wire change** (rule 6) and no setting: `cursorsTimeout` is a constant, like `recalcPeersDur`. A test can lower it through a package variable.

## Tests

1. **Reproduction, `TestRecalcNotBlockedBySilentPeer`.** A test-local `pullsync.Interface` stub (so the test runs unchanged on upstream) whose `GetCursors` for peer A blocks until its context ends and answers at once for peer B; a topology mock with A and B; a short `RecalcPeersDur`. With `cursorsTimeout` lowered, the test asserts that `on_change_runs` reaches at least 3 within a few recalculation periods and that peer B's bins are synced. On unmodified code the first recalculation never returns and the test fails with the counter at 1. Real time, with the periods in tens of milliseconds and a generous overall deadline; not `synctest`, because on unmodified code every goroutine is durably blocked and `synctest` would panic instead of failing.
2. **Timeout counted:** after one recalculation with peer A silent, `cursor_requests_failed_total{reason="timeout"}` is 1 and `{reason="error"}` 0; an error answer counts as `error`.
3. **Retry:** when peer A starts answering, the next recalculation gets its cursors and starts its bins.
4. **Duration histogram:** one observation per recalculation.
5. **Radius change acted on:** with peer A silent, a radius decrease is followed by syncing the newly opened bins from peer B within one recalculation period plus `cursorsTimeout`.

**Mutation checks** (each must make a test fail): deadline removed; deadline applied only to the stream open (a context dropped before the read); timeout counted as `error`; failed request not counted; duration not observed.

Race detector on the puller package; check the exit status directly.

## Upstream

Expected to affect upstream: the same code is on `master` at the lines above. Check: run test 1 (it uses only a local stub and the existing topology mock) on an unmodified `upstream/master` worktree; if it fails there with the counter at 1, add `affects-upstream` to #695 with what was checked, and a row in `docs/UPSTREAM.md`.

## Measurement

On the dense host (ten nodes, by role), with the build carrying this change and #696:

- Restart all ten nodes at once, twice, a day apart.
- Per node, for 2 hours after each restart: the gaps between increments of `bee_puller_on_change_runs` (pass: no gap longer than 6 minutes, against about 30 minutes on two nodes on 2026-10-09), the `on_change_duration` maximum (pass: under 60 seconds), and `cursor_requests_failed_total` by reason.
- If gaps stay long with no timeouts counted, the hypothesis above is refuted and the issue stays open with that result.

Generated with help of AI.
