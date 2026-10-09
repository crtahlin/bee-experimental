# Pull-sync: end a waiting request when the requester resets or closes its stream

Issue: #641. Builds on #640 (merged as `bb05949a`, spec `docs/experiments/pullsync-duplicate-requests/spec.md`), which ends an older waiting request when the same peer asks again. Backstop: #638 (stream limits per peer). Related: #643 (the requested bin is not checked against `swarm.MaxBins`).

## Terms

- **Request**, **waiting request**, **live** and **historical request:** as in the #640 spec. A waiting request is one whose handler is inside `makeOffer` (`pkg/pullsync/pullsync.go`), which waits with no time limit in `collectAddrs` for the first chunk at or above the requested start.
- **Requester gone:** the requester has reset its stream or closed its write side. A wasp or upstream requester resets the stream whenever `Sync` returns an error, including when its context is cancelled (`Sync`, deferred `stream.Reset()`).
- **Watcher:** the background read this spec adds, which observes the stream while the request waits.

## Problem

While a request waits for its first chunk, the server does not read from the stream, and the handler context is tied to the connection, not the stream (`pkg/p2p/libp2p/libp2p.go`, `context.WithCancel(s.ctx)` per handler, cancelled on disconnect). A reset or close by the requester is therefore not noticed until a chunk arrives in that bin or the peer disconnects. In quiet high bins that can be many hours, and the stream counts against the node-wide inbound stream cap of 5,014 the whole time (#636).

#640 ends such a request when the same peer asks again for the same bin and start. It does not cover a requester that gives up and does **not** ask again for the same start. Reasoned from the puller code (`pkg/puller/puller.go`), not measured, this happens when:

- the requester's storage radius decreases: `disconnectPeer` cancels every worker for every peer and resets the intervals; new requests may then use different starts or not cover the old bins at all;
- the requester's storage radius increases: bins below the new radius are cancelled (`cancelBin`) and never asked again;
- the peer leaves the requester's connected set for a recalculation, or its epoch changes, and the new starts differ from the old ones.

Each such event can leave about one waiting request per quiet bin on each neighbour, and our own nodes cause this on every radius change. #640's cap of two per (peer, bin) bounds how many accumulate from one peer, but each still holds a stream until a chunk arrives.

### Measured so far

On ten nodes of one host running the #640 build, during a refill (2026-10-09, about 95 minutes after a restart): the largest number of inbound streams held by one peer stayed at 22 to 24 on every node, and `bee_pullsync_waiting_requests` was 51 to 277 per node. How many of those waiting requests belong to requesters that are gone is not known: from the server side a request whose requester reset its stream looks the same as a live one. The metric this spec adds is the first way to count them.

## Hypothesis

A waiting request whose stream returns from a read, with an error or with data, has no requester waiting for its offer: a correct requester sends nothing between its `Get` and the server's `Offer`, so a read can only return because the stream was reset (`ErrStreamReset`), its write side was closed (`io.EOF`), or the requester broke the protocol. Ending the request at that moment frees its stream within seconds instead of hours, and loses nothing.

## Design

### 1. The watcher

In `handler`, after `register` (#640) and before `makeOffer`, when the stream supports read deadlines (section 3), start one goroutine that does a single `Read` into a 1-byte buffer on the raw stream:

- **Returns with `n == 0` and an error** (reset, EOF, or any other error except the deadline error of section 2): cancel `reqCtx` with the cause `errRequesterGone` and record the reason (`reset` for a stream reset, `eof` for `io.EOF`, `error` otherwise).
- **Returns with `n > 0`:** the requester sent data before the offer. Cancel `reqCtx` with the cause `errUnexpectedData`, reason `unexpected_data`. The handler returns an error, so the deferred `stream.Reset()` runs, as for any other protocol error. No byte is handed on: a correct requester never sends here, so there is nothing to keep.
- **Returns with the deadline error:** the handler stopped it (section 2); do nothing.

The watcher reads from the stream itself, not through the protobuf reader. The reader created before `makeOffer` (`protobuf.NewWriterAndReader(stream)`) has not read anything yet; the existing code already relies on the requester sending nothing after its `Get` (the reader used for the `Get` is dropped, with whatever it may have buffered).

The handler then proceeds exactly as after #640: `makeOffer(reqCtx, rn)` returns with the context error, the request unregisters itself, and the handler returns `make offer: <cause>` (the existing `context.Cause` check is extended to `errRequesterGone` and `errUnexpectedData`). Cancelling `reqCtx` releases this caller from the shared collection exactly as #640's replacement does, so the singleflight and tracking rules of #640 apply unchanged; no other request waits on this one.

### 2. Stopping the watcher before the offer is written

When `makeOffer` returns (with or without error), and before `unregister`:

1. `SetReadDeadline(time.Now())` on the stream. A pending yamux `Read` returns `ErrTimeout` with no bytes; buffered data, if any, stays in the stream (`go-yamux/v5` `Stream.Read`: the deadline case returns before reading the buffer).
2. Wait for the watcher goroutine to return (a channel it closes).
3. `SetReadDeadline(time.Time{})` to clear the deadline, so the later `Want` read is not affected.
4. If the watcher reported `unexpected_data` or `requester gone` in the same moment (the read returned something other than the deadline error), the handler treats the request as ended for that reason even if `makeOffer` succeeded: it does not write the offer, counts the reason, and returns the error. This covers a reset that arrives just as the chunk does.

The watcher is stopped on every exit path of `makeOffer`, so no goroutine outlives the waiting phase. Steps 1 to 3 run after `makeOffer`, outside the #640 mutex.

yamux applies `SetReadDeadline` only while the read side is open (`readState == halfOpen`); after a reset or close the watcher has already returned, so step 2 does not wait on a read that cannot be woken.

### 3. Streams without read deadlines

`p2p.Stream` does not declare `SetReadDeadline`. The watcher runs only when the stream implements `interface{ SetReadDeadline(time.Time) error }` and a first call `SetReadDeadline(time.Time{})` returns nil. In production every stream is a `*libp2p.stream` embedding `network.Stream` over yamux (TCP and WebSocket transports, `libp2p.go`), which supports it. Otherwise the handler behaves as after #640 and counts the request once in `bee_pullsync_requests_unwatched_total`, so a transport change that loses deadlines is visible.

### 4. Cost

One goroutine and a 1-byte buffer per waiting request, alive only while it waits. Waiting requests are bounded per peer by #640 (two per bin; 64 per peer with 32 bins, more until #643 checks the bin) and node-wide by the inbound stream cap (5,014). The protobuf reader already starts a goroutine per `ReadMsgWithContext`, so the handler's goroutine count per request goes from about 2 to about 3 while waiting. Two `SetReadDeadline` calls per request take the yamux state lock briefly.

### 5. Metrics

- `bee_pullsync_requests_abandoned_total{reason}` with `reason` in `reset`, `eof`, `error`, `unexpected_data`: waiting requests ended because the watcher saw the requester gone or misbehaving. Counted once per request, by the watcher's report in the handler.
- `bee_pullsync_requests_unwatched_total`: requests that waited without a watcher because the stream has no read deadline.
- `bee_pullsync_requests_replaced_total` (#640) is unchanged. A request ended by #640 before the watcher sees anything is counted there only; the watcher's later read returns with the deadline error or after the handler stopped it, and is not counted.

### 6. The cursor handler

`cursorHandler` reads the `Syn` and answers at once from `ReserveLastBinIDs`; it never waits, so it needs no watcher.

### 7. Documentation

A `docs/DIFFERENCES.md` row next to #640's: what is ended, the reasons, the two metrics, and that nothing changes on the wire.

## Protocol impact

None. No message, field, stream name, protocol ID or reset code changes. The server only reads from a stream it already owns and resets a stream whose requester is gone or broke the protocol. `make protocol-freeze` must report the wire surface unchanged.

## Configuration

None. The change ends only requests whose requester is gone or sent data a correct requester never sends, so it has no trade-off that needs a setting (rule 8).

## Measurement

**Test support** (`pkg/p2p/streamtest`): the recorder's `stream` gains `SetReadDeadline(t time.Time) error`. A read on the `record` waits on the data signal, the close, or the deadline, and returns `os.ErrDeadlineExceeded` with no bytes on the deadline, leaving buffered data in place. A zero time clears it. The recorder's `Reset` is `FullClose`, so the requester's reset reaches the server as `io.EOF`; tests assert the reason accordingly (`eof` with the recorder).

**Tests** (`package pullsync_test`, a real server `Syncer` and the recorder; every test closes the server `Syncer`):

1. **Reproduction, using only signals that exist upstream:** a requester calls `Sync` for an empty bin with a context, then cancels the context; `Sync` returns and resets its stream. The test then waits, with a deadline, for the recorder's `Records()` to return, which needs every server handler to have returned. On unmodified code the handler stays in `makeOffer` and the test fails with a message at its deadline; with the change it returns within the deadline. It uses no new metric and no new export, so it runs unchanged on upstream code, given the recorder's `SetReadDeadline` (which an upstream port would add with the change).
2. **Data before the offer:** a raw requester writes a `Get` and then one more byte without waiting: the request ends with reason `unexpected_data` and the stream is reset.
3. **No interference with a normal request:** a waiting request whose chunk then arrives (`PublishBin`, #640's mock extension) completes offer, want and delivery; the `Want` is read correctly after the watcher was stopped (no byte lost), and `requests_abandoned_total` stays 0.
4. **Reset just as the chunk arrives:** with a hook between `makeOffer` and the watcher stop (the test-only hook #640 added in `export_test.go`), reset the requester's stream; the handler does not write the offer and counts the request once.
5. **Interaction with #640:** the same peer asks again for the same (bin, start); the older request ends as `same_start` and is not also counted as abandoned.
6. **Shared collection:** two peers wait on the same (bin, start); the first resets; the second keeps waiting and receives the chunk after `PublishBin`.
7. **No deadline support:** a stream wrapper without `SetReadDeadline`: the request waits as today and `requests_unwatched_total` rises by one.
8. **No goroutine leak:** after a run of requests that end in each way, the handler and watcher goroutines are gone (`goleak` or a goroutine count before and after, as the package's existing tests do).

The package also runs under `make test-race`.

Mutation checks, each must fail a test:

- the watcher not started (test 1);
- the watcher's result ignored (`reqCtx` not cancelled) (test 1);
- the deadline not cleared after the stop (test 3: the `Want` read fails);
- the stop skipped, so the watcher keeps reading during the `Want` (test 3, or the race detector);
- data before the offer treated as a normal wait (test 2);
- a reset at the boundary not checked, so the offer is written (test 4);
- an abandoned request counted twice with #640's replacement (test 5).

**Real nodes** (role names only): the build goes to the ten-node host with the next deployment.

- `bee_pullsync_requests_abandoned_total` by reason, `bee_pullsync_requests_unwatched_total` (expected 0), `bee_pullsync_waiting_requests` and `bee_libp2p_inbound_streams_per_peer_max`, compared with the #640 build over the same kind of period (a refill and the hours after it).
- A radius change on one node of the host (a capacity change and restart, as in the radius-change scenarios) should raise `requests_abandoned_total{reason="reset"}` on its neighbours on the host by about the number of its quiet live bins each, within seconds, where with #640 alone those requests stay waiting. Note the restart itself also disconnects, which already ends requests; the cleaner trigger is a radius change without restart, if one occurs during the run, read from the `radius decrease` log and the counter.
- A negative result: `unwatched` above 0 on production streams, `unexpected_data` above 0 from wasp or upstream requesters (a protocol assumption is wrong), or the waiting-request gauge unchanged after radius changes of neighbours.

## Rollout and rollback

Ships in the next build with no setting. Rollback is the previous build; abandoned requests that #640 does not catch then wait again until a chunk arrives or the peer disconnects.

## Upstream portability

The handler path is the same upstream except for #640's tracking, which this change builds on; the upstream port would carry both. The `affects-upstream` label for #641 waits for test 1 to fail against unmodified upstream code (with only the recorder's `SetReadDeadline` added) and pass with the change, which the implementation PR will report.
