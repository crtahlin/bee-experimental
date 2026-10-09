# Pull-sync: end an older waiting request when the same peer asks again

Issue: #640. Companion: #641 (notice a reset by the requester). Backstop: #638 (stream limits per peer). The cap these requests fill: #636. Related, out of scope here: #643 (the requested bin is not checked against `swarm.MaxBins`).

## Terms

- **Request:** one inbound pull-sync stream (`pullsync/1.4.0`, stream `pullsync`). The requester sends a `Get` with a bin and a start BinID. The server answers with an `Offer`, reads a `Want`, then sends the wanted chunks.
- **Waiting request:** a request whose handler is inside `makeOffer` (`pkg/pullsync/pullsync.go:180`), which calls `collectAddrs` (`:478`, `:502`). `collectAddrs` waits with no time limit for the first chunk at or above the start, then up to `pageTimeout` for more (`:503-:556`).
- **Live request** and **historical request:** the puller runs two workers per peer and bin (`pkg/puller/puller.go:430-543`). The historical worker asks from the next missing interval up to the peer's cursor and exits once its start passes the cursor (`:456-458`). The live worker asks from cursor + 1 and only moves up (`:540`). A live request in a quiet bin waits a long time; a historical request almost never waits, because chunks at or above its start already exist.

## Problem

The server never ends a waiting request whose requester has given up:

- the handler context ends only when the peer disconnects or the node stops (`pkg/p2p/libp2p/libp2p.go:1018`: `context.WithCancel(s.ctx)`, cancelled through the peer registry on disconnect);
- `collectAddrs` does not read from the stream while it waits, so a reset or close by the requester is not seen (#641).

When a requester restarts its sync with us and asks again, its old request stays open next to the new one. Abandoned live requests in high bins can wait for hours. Each holds one inbound stream, and the node-wide inbound stream cap is 5,014. At the cap, every new stream from every peer is refused, including retrieval, push-sync, pricing and hive (#636).

### Measured (goroutine dumps, 2026-10-09)

On two full nodes on different hosts, one neighbour each restarts its whole sync with the node about every 5 minutes (gaps of 4 to 7 minutes). The node rates both neighbours as not reachable.

| Node | Waiting requests | Distinct (bin, start) among them | Largest repeat of one (bin, start) | Batch every ~5 min |
|---|---|---|---|---|
| Node A (depth 6) | 378 of 388 inbound pull-sync | 26, in 26 different bins (one start per bin) | 30 | 10 requests, bins 22 to 31 |
| Node B (depth 9) | 414 | 25 (bins per start not tabulated) | 29 | 10 requests, bins 22 to 31 |

- On node A, the neighbour's inbound stream count rose by about 115 an hour after a restart: 124 after 1 h, 239 after 2 h, 264 at the first named log line, and 802 inbound in total about 6 h later.
- Requests for lower bins arrive in the same batches but mostly complete within minutes, because chunks keep landing there. Only the quiet high bins accumulate.
- Before the restarts, one peer held about 4,500 of 4,830 inbound streams on node B. On seven of ten nodes on a third host, one peer each held 700 to 1,100; the request pattern was not checked there.

### Why those neighbours restart (lead, not verified)

The puller's 5-minute recalculation does not restart a bin that is still syncing (`isBinSyncing`, `pkg/puller/puller.go:385`, `:398`). Workers restart only through `disconnectPeer`, which runs when the peer is missing from `EachConnectedPeerRev` (`:285-298`), when the storage radius decreases (`:262-267`), or when the peer's epoch changes (`:347-349`). Hypothesis: the requester's topology drops our node from its connected set at a recalculation while the libp2p connection stays up. A measurement with a node that cannot be dialled is running. This spec is about the server side, which holds the abandoned requests whatever the cause.

## Hypothesis

A second waiting request from the same peer for the same bin and start means the first was abandoned. **Reasoned from the puller code, not verified against these neighbours:** per peer and bin a wasp or upstream requester runs one live worker and at most one historical worker, each with one request in flight (`pkg/puller/puller.go:478`); their starts never meet (historical ≤ cursor < live); and `disconnectPeer` cancels the workers and waits for them before new ones start (`:303-311`, `:711-717`). So a correct requester has at most two waiting requests per bin, with different starts. Ending the older of two same-start requests frees its stream and loses nothing.

Node A's data fits this: one start per bin. With the rule, the neighbour would hold one waiting request per quiet bin, and with the cap at most two, so at most 20 for 10 quiet bins instead of 281 at the time of the dump.

## Design

### 1. Track waiting requests per peer and bin

In the handler (`pkg/pullsync/pullsync.go:139`), after the `Get` is read (`:170`):

```go
reqCtx, cancel := context.WithCancelCause(ctx)
entry := s.waiting.register(p.Address, uint8(rn.Bin), rn.Start, cancel) // may end older entries
offer, err := s.makeOffer(reqCtx, rn)
s.waiting.unregister(entry)
cancel(nil)
if err != nil {
	if cause := context.Cause(reqCtx); errors.Is(cause, errRequestReplaced) {
		err = cause // the debug log names the reason
	}
	return fmt.Errorf("make offer: %w", err)
}
```

- `reqCtx` goes **only** to `makeOffer`. The offer write, the `Want` read, `processWant`, the limiter and the deliveries (`:184-:231`) keep the handler's `ctx`. A request that has its offer is never ended by this rule.
- `unregister` runs straight after `makeOffer` returns, not in a deferred call at the end of the handler.
- `register`, the ending of older entries (calling their cancel functions) and `unregister` all run under one mutex. An entry that was already ended and removed is not removed again; removal is by identity, so it never removes a newer entry.
- **The race at the boundary:** an older request may be cancelled just after its `makeOffer` returned with an offer. Because `reqCtx` is not used after that point, the cancel has no effect and the request completes normally. The entry was already removed or is removed by its own `unregister`; either order is safe under the mutex.
- The map is keyed by (overlay, bin). A key is deleted when its list becomes empty, so the map does not keep one key per peer and bin ever seen. Its size is bounded by the number of waiting requests.

### 2. Which older request is ended

When a new request registers under (peer, bin):

1. **Same start:** every waiting entry with the same start is ended. This is the observed pattern on node A.
2. **More than two:** if, after step 1, more than two requests would be waiting for this (peer, bin), counting the new one, the **oldest** are ended until two remain. A correct requester has at most two (one live, one historical), so the cap should never end a request it still wants. This bounds a requester whose start moves between restarts, which step 1 alone would not catch.

The new request always proceeds.

Keying on (peer, bin) without the start would end a live request when the historical worker for the same bin asks, and the reverse. Each ended request makes a wasp worker retry after a backoff (#573), so the two workers would keep ending each other. Keying on the start as well avoids that; the cap of two covers the rest.

**Dump check during measurement** (because the cap rests on reasoning, not observation): in a goroutine dump, compare each waiting request's start with the server's `ReserveLastBinIDs` for that bin. Two waiting requests from one peer in one bin with different starts can only both wait when each start is above the server's last BinID for the bin, or the range from the lower start up was evicted. Any other case means the cap reasoning is wrong for that requester, and the cap must be revisited before it ends wanted requests.

### 3. Interaction with the shared collection (singleflight)

`collectAddrs` runs inside `s.intervalsSF.Do(ctx, sfKey(bin, start), ...)` (`:503`), a `resenje.org/singleflight` group (v0.4.0) shared by all peers. Each caller's `wait` lowers a per-key counter when that caller returns; the shared function runs on `withoutCancel(ctx)` and is cancelled only when the counter reaches zero. So:

- ending an old request from peer X does not affect a request from peer Y with the same (bin, start);
- registering in the tracker does **not** join the singleflight call; the new request joins only inside `Do`, after the old one was cancelled. If the old request was the only caller for that (bin, start), the shared collection is cancelled and the new request starts a new subscription from the same start. That costs one resubscription and loses nothing.

### 4. What the ended request does

Its context is cancelled with the cause `errRequestReplaced`. Singleflight's `wait` returns `ctx.Err()`, not the cause, so the handler reads `context.Cause(reqCtx)` (section 1) to log the reason. The handler returns the error, and the existing deferred `stream.Reset()` (`:149-:155`) resets the stream. The libp2p wrapper logs it at debug level only. It is not a `DisconnectError` or a `BlockPeerError`, so the peer is not disconnected or blocklisted.

The requester has already abandoned that stream; it was reset on its side when its context ended. A requester that, against expectation, still waited on the ended stream would see a reset and an error from `Sync`:

- a **wasp** requester then backs off from 1 s, doubling to 1 minute (`pkg/puller/puller.go:186-218`, `:493-501`), so nothing is lost;
- an **upstream** requester retries at once (`upstream/master` `pkg/puller/puller.go:380-388`). If this rule ever ended a request an upstream requester still wanted, the result would be a tight loop of new streams. By the hypothesis this does not happen (a correct requester never has two waiting requests with the same start, nor more than two per bin); the dump check in section 2 and the `over_cap` counter are how it would be noticed.

### 5. Metrics

- `bee_pullsync_requests_replaced_total{reason}`: requests ended by this rule, with `reason` = `same_start` or `over_cap`. It is counted by the **new** request, at registration, for each older request it ends.
- `bee_pullsync_waiting_requests`: requests currently waiting for their first chunk, from the registry size.

### 6. Documentation

`docs/DIFFERENCES.md` records the rule and the two metrics.

### 7. Relation to the companion fix (#641)

- **This fix (#640)** covers a requester that asks again: the old request is ended when the new one arrives. It needs no read from the stream and is small, so it ships first, before the next fill cycle.
- **#641** covers a requester that gives up and does not ask again (a radius decrease, a peer dropped from its table, a node that stops syncing a bin). It needs a background read on the stream during the wait, which is harder to get right without consuming the requester's next message. It ships after this fix.
- Together they leave no abandoned waiting request open; #638 remains a backstop against any protocol, and #643 bounds the bin number itself.

## Protocol impact

None. No message, field, stream name, protocol ID or reset code changes. The requester sees a stream reset, which it already handles. `make protocol-freeze` must report the wire surface unchanged.

## Configuration

None. The rule ends only requests a correct requester has abandoned, so it has no trade-off that needs a setting (rule 8). The cap of two follows from the puller's design, not from tuning.

## Measurement

**Mock extensions** (`pkg/storer/mock/mockreserve.go`):

- `SubscribeBin` returns a subscription that stays open and empty when no prepared response is left, instead of indexing past `subResponses` (`:183`). Tests 1 and 6 depend on this for the resubscription after an old request is ended.
- A new method `(*ReserveStore).PublishBin(bin uint8, c *storer.BinC)` delivers a chunk to every open subscription for that bin whose start is at or below `c.BinID`, so a test can let a waiting request complete (tests 4 and 6).

**Tests** (`package pullsync_test`, a real server `Syncer` and the stream recorder; every test calls `Close()` on the server `Syncer`, because `Records()` waits for every handler and the handler runs on `context.Background()` in the recorder):

1. **Reproduction, using only signals that exist upstream:** requester goroutine 1 calls `Sync` for (bin b, start s) in an empty bin and blocks. Requester goroutine 2 calls `Sync` for the same (b, s). On unmodified code goroutine 1 stays blocked until the test's own deadline (run under `synctest` or with a timer), and the test fails with a message. With the change, the server ends request 1; in the recorder the server's `Reset` is a `FullClose` (`streamtest.go:340`), so `Sync` in goroutine 1 returns an error (EOF) and goroutine 2 keeps waiting. The test asserts only that goroutine 1 returns with an error and goroutine 2 does not, so it compiles and fails on upstream code unchanged.
2. Two **different peers** with the same (bin, start): both keep waiting.
3. The **same peer** with **different bins**: both keep waiting.
4. A waiting request whose chunk then arrives (`PublishBin`) completes normally: offer, want, delivery.
5. **Cap:** the same peer and bin, three different starts, none completing: the oldest ends with reason `over_cap`, the two newest remain.
6. **Shared collection:** two peers wait on the same (bin, start); the first peer asks again; after `PublishBin`, the second peer's request and the first peer's new request both receive the chunk.
7. **Boundary race:** an older request is ended just after its `makeOffer` returned (forced with a hook in `export_test.go`); it still completes its offer, want and delivery.
8. Metrics: the counter rises by reason, counted once per ended request, and the gauge goes back to zero after all requests end.

The package is also run under `make test-race`.

Mutation checks, each must fail a test:

- drop the start from the same-start comparison (tests 2, 3 or 5 as they apply);
- end the new request instead of the old one (test 1);
- skip `unregister` (test 8);
- skip the cap (test 5);
- count the cap without the new request, so three may wait (test 5);
- end the newest instead of the oldest under the cap (test 5);
- pass `reqCtx` to the code after `makeOffer` (test 7).

**Real nodes** (role names only): the build goes to one node of the ten-node host first, then to all ten before the next fill cycle.

- Before (measured above): `bee_libp2p_inbound_streams_per_peer_max` at 281 for one neighbour, rising about 115 an hour; 30 repeats of one (bin, start) in a goroutine dump.
- After, in a goroutine dump after at least 2 hours: no (peer, bin, start) repeats; at most 2 waiting requests per (peer, bin), at most 20 in total for a neighbour with 10 quiet bins; the `ReserveLastBinIDs` check of section 2 holds for every pair of different starts.
- `bee_libp2p_inbound_streams_per_peer_max` stays below 64 for that neighbour, and `bee_pullsync_requests_replaced_total{reason="same_start"}` rises by about 10 every 5 minutes while it keeps restarting. `reason="over_cap"` should stay near zero; a steady rise is a finding to explain before the next step.
- During the next fill cycle on the ten-node host: total inbound streams per node stay far below the cap (today 115 to 2,144 after about 8 hours, rising).
- A negative result: repeats still present in the dump, the per-peer maximum still rising steadily for the same neighbour, or the dump check failing.

## Rollout and rollback

Ships in the next build with no setting. Rollback is the previous build; the waiting requests then accumulate again.

## Upstream portability

From `v2.8.2` to upstream `master` the only `pullsync.go` changes are `pageTimeout` (1 s to 250 ms) and client-side SOC validation (checked 2026-10-09); the handler and the `collectAddrs` path are unchanged, so the change ports straight across. The `affects-upstream` label waits for test 1 to fail against unmodified upstream code and pass with the change, which the implementation PR will report.
