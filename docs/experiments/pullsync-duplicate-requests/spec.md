# Pull-sync: end an older waiting request when the same peer asks again

Issue: #640. Companion: #641 (notice a reset by the requester). Backstop: #638 (stream limits per peer). The cap these requests fill: #636.

## Terms

- **Request:** one inbound pull-sync stream (`pullsync/1.4.0`, stream `pullsync`). The requester sends a `Get` with a bin and a start BinID. The server answers with an `Offer`, reads a `Want`, then sends the wanted chunks.
- **Waiting request:** a request whose handler is still building its offer, in `makeOffer`/`collectAddrs` (`pkg/pullsync/pullsync.go:478`, `:502`). `collectAddrs` waits with no time limit for the first chunk at or above the start, then up to `pageTimeout` for more (`:503-:556`).
- **Live request** and **historical request:** the puller runs two workers per peer and bin (`pkg/puller/puller.go:430-543`). The historical worker asks from the next missing interval up to the peer's cursor. The live worker asks from cursor + 1 upward. A live request in a quiet bin waits a long time; a historical request almost never waits, because chunks at or above its start already exist.

## Problem

The server never ends a waiting request whose requester has given up:

- the handler context ends only when the peer disconnects or the node stops (`pkg/p2p/libp2p/libp2p.go:1018`: `context.WithCancel(s.ctx)`, cancelled through the peer registry on disconnect);
- `collectAddrs` does not read from the stream while it waits, so a reset or close by the requester is not seen (#641).

When a requester restarts its sync with us and asks again, its old request stays open next to the new one. Abandoned live requests in high bins can wait for hours. Each holds one inbound stream, and the node-wide inbound stream cap is 5,014. At the cap, every new stream from every peer is refused, including retrieval, push-sync, pricing and hive (#636).

### Measured (goroutine dumps, 2026-10-09)

On two full nodes on different hosts, one neighbour each restarts its whole sync with the node about every 5 minutes (gaps of 4 to 7 minutes). The node rates both neighbours as not reachable.

| Node | Waiting requests | Distinct (bin, start) among them | Largest repeat of one (bin, start) | Batch every ~5 min |
|---|---|---|---|---|
| Node A (depth 6) | 378 of 388 inbound pull-sync | 26 | 30 | 10 requests, bins 22 to 31 |
| Node B (depth 9) | 414 | 25 | 29 | 10 requests, bins 22 to 31 |

- On node A, the neighbour's inbound stream count rose by about 115 an hour after a restart: 124 after 1 h, 239 after 2 h, 264 at the first named log line, and 802 inbound in total about 6 h later.
- Requests for lower bins arrive in the same batches but complete within minutes, because chunks keep landing there. Only the quiet high bins accumulate.
- Before the restarts, one peer held about 4,500 of 4,830 inbound streams on node B. On seven of ten nodes on a third host, one peer each held 700 to 1,100; the request pattern was not checked there.

Why those neighbours restart every 5 minutes is not known yet. It matches the puller's peer recalculation interval (`--pullsync-recalc-interval`, default 5 minutes); a measurement with a node that cannot be dialled is running. This spec is about the server side, which holds the abandoned requests whatever the cause.

## Hypothesis

A correct requester never has two waiting requests for the same bin with the same start: per peer and bin it runs one live worker and at most one historical worker, each with one request in flight (`pkg/puller/puller.go:478`), and their starts never meet (historical ≤ cursor < live). A second waiting request from the same peer, for the same bin and start, therefore means the first was abandoned. Ending the first frees its stream and loses nothing.

With that rule, node A's neighbour would hold at most one waiting request per (bin, start) instead of 30, roughly 26 instead of 281 at the time of the dump.

## Design

### 1. Track waiting requests per peer and bin

In the handler (`pkg/pullsync/pullsync.go:139`), after the `Get` is read (`:170`) and before `makeOffer` (`:180`):

- derive a cancellable context for this request: `context.WithCancelCause(ctx)`;
- register the request under the key (peer overlay, bin), with its start and its cancel function;
- unregister it as soon as `makeOffer` returns, whatever the result. Only the waiting phase is tracked. A request that is reading the `Want` or sending chunks is never ended by this rule.

The registry is a map from (overlay, bin) to a short list of entries, guarded by one mutex. An entry is removed by identity, so a request that was already replaced does not remove its replacement. Its size is bounded by the number of open streams.

### 2. Which older request is ended

When a new request registers under (peer, bin):

1. **Same start:** every waiting entry with the same start is ended. This is the observed pattern: all 378 waiting requests on node A fell into 26 (bin, start) pairs.
2. **More than two:** if, after step 1, more than two requests would be waiting for this (peer, bin), the oldest are ended until two remain, counting the new one. A correct requester has at most two (one live, one historical), so the cap never ends a request it still wants. This bounds a requester whose start moves between restarts (for example a historical range that was evicted and now waits), which step 1 alone would not catch.

The new request always proceeds. Ending is done after the new entry is registered, so the shared collection (section 3) keeps a waiter throughout.

Keying on (peer, bin) without the start would end a live request when the historical worker for the same bin asks, and the reverse. Each ended request makes the worker retry after a backoff (#573), so the two workers would keep ending each other. Keying on the start as well avoids that; the cap of two covers the rest.

### 3. Interaction with the shared collection (singleflight)

`collectAddrs` runs inside `s.intervalsSF.Do(ctx, sfKey(bin, start), ...)` (`:503`), a `resenje.org/singleflight` group (v0.4.0) shared by all peers. Its `Do` gives the collection function a context that is cancelled only when **every** caller's context is done (it keeps a counter per key and cancels at zero). Cancelling one caller makes only that caller return with its context error; the others keep waiting for the same result. So:

- ending an old request from peer X does not affect a request from peer Y with the same (bin, start);
- ending the old request after registering the new one from the same peer keeps the counter above zero, so the collection is not restarted.

### 4. What the ended request does

Its context is cancelled with the cause `errRequestReplaced`. `collectAddrs` returns the context error, `makeOffer` and the handler return it wrapped, and the existing deferred `stream.Reset()` (`:149-:155`) resets the stream. The libp2p wrapper logs the error at debug level only. It is not a `DisconnectError` or a `BlockPeerError`, so the peer is not disconnected or blocklisted.

The requester has already abandoned that stream; it was reset on its side when its context ended. Upstream and older wasp requesters see nothing. A requester that, against expectation, still waited on the ended stream would see a reset and an error from `Sync`; its worker retries after a backoff (#573), so nothing is lost.

### 5. Metrics

- `bee_pullsync_requests_replaced_total{reason}`: requests ended by this rule, with `reason` = `same_start` or `over_cap`.
- `bee_pullsync_waiting_requests`: requests currently waiting for their first chunk, from the registry size.

### 6. Documentation

`docs/DIFFERENCES.md` records the rule and the two metrics.

## Protocol impact

None. No message, field, stream name, protocol ID or reset code changes. The requester sees a stream reset, which it already handles. `make protocol-freeze` must report the wire surface unchanged.

## Configuration

None. The rule ends only requests a correct requester has abandoned, so it has no trade-off that needs a setting (rule 8). The cap of two follows from the puller's design, not from tuning.

## Measurement

**Tests** (`package pullsync_test`, with the existing stream recorder and a real `Syncer`):

1. **Reproduction:** one peer sends a `Get` for (bin b, start s) in an empty bin, then a second identical `Get`. On today's code both stay waiting; with the change the first ends with `errRequestReplaced` and the second keeps waiting. The test has its own deadline, so a regression fails with a message rather than a hang.
2. Two **different peers** with the same (bin, start): both keep waiting.
3. The **same peer** with **different bins**: both keep waiting.
4. A waiting request whose chunk then arrives completes normally: offer, want, delivery.
5. **Cap:** the same peer and bin, three different starts, none completing: the oldest ends with reason `over_cap`, two remain.
6. **Shared collection:** two peers wait on the same (bin, start); the first peer asks again; when a chunk arrives, the second peer's request and the first peer's new request both receive it.
7. Metrics: the counter rises by reason, and the gauge goes back to zero after all requests end.

Mutation checks: register with the start dropped from the comparison (tests 2, 3 or 5 must fail as they apply); end the new request instead of the old one (test 1); skip unregistering (test 7); skip the cap (test 5). Each must fail a test.

**Real nodes:** the build goes to sw-1 node 1 first, then all sw-1 nodes before the next fill cycle.

- Before (measured above): `bee_libp2p_inbound_streams_per_peer_max` at 281 for one neighbour, rising about 115 an hour; 30 repeats of one (bin, start) in a goroutine dump.
- After: in a goroutine dump after at least 2 hours, no (peer, bin, start) repeats; `bee_libp2p_inbound_streams_per_peer_max` stays below 64 for that neighbour; `bee_pullsync_requests_replaced_total{reason="same_start"}` rises by about 10 every 5 minutes while that neighbour keeps restarting.
- During the next fill cycle on sw-1: total inbound streams per node stay far below the cap (today 115 to 2,144 after about 8 hours, rising).
- A negative result: repeats still present in the dump, or the per-peer maximum still rising steadily for the same neighbour.

## Rollout and rollback

Ships in the next build with no setting. Rollback is the previous build; the waiting requests then accumulate again.

## Upstream portability

The handler and `collectAddrs` path is unchanged from `v2.8.2` up to upstream `master` (checked 2026-10-09; the only change is `pageTimeout`, 1 s to 250 ms). The change is self-contained in `pkg/pullsync` and ports as is. The `affects-upstream` label waits for test 1 to pass against unmodified upstream code, which the implementation PR will report.

## To check during implementation

- That `context.Cause` reaches the handler's error, so the debug log names the reason (`go.mod` is at Go 1.26, so `WithCancelCause` is available).
- That the test stream recorder lets two requests from one peer wait at once; if not, test 1 uses two goroutines on one `Syncer` with a protocol-level client.
