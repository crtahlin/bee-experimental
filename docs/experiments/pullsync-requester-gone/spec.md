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

In `handler`, after `register` (#640) and before `makeOffer`, when the stream supports read deadlines (section 3), start one goroutine that does a single `Read` into a 1-byte buffer on the raw stream. Whether the handler has **stopped** the watcher is decided by an atomic flag the handler sets **before** it calls `SetReadDeadline(now)` (section 2), never by the error value: yamux returns `yamux.ErrTimeout` (a `*yamux.Error` with `Timeout() == true`), which does not match `os.ErrDeadlineExceeded`, and the test recorder returns something else again. When the `Read` returns:

- **The stop flag is set and `n == 0`:** the handler stopped it; do nothing, whatever the error.
- **`n > 0`** (stop flag set or not): the requester sent data before the offer. Cancel `reqCtx` with the cause `errUnexpectedData`, reason `unexpected_data`. The handler returns an error, so the deferred `stream.Reset()` runs, as for any other protocol error. No byte is handed on: a correct requester never sends here, so there is nothing to keep. Detection is **best-effort**: bytes that arrived together with the `Get` were already buffered and discarded by the first protobuf reader (`pullsync.go`, the reader created before `ReadMsgWithContext(ctx, &rn)`, up to 4,096 bytes), so the watcher sees only data sent after the request started waiting.
- **The stop flag is not set and `n == 0`:** cancel `reqCtx` with the cause `errRequesterGone` and record the reason:
  - `reset`: the error is a `*network.StreamError` (`github.com/libp2p/go-libp2p/core/network`, `mux.go`) with `Remote == true`, that is, the requester reset this stream;
  - `disconnect`: any other reset, including the one yamux gives every stream when the connection closes (`session.go`, `ErrStreamReset` with "connection closed", which libp2p maps to `network.ErrReset` without `Remote`), and `network.ErrReset` itself;
  - `eof`: `io.EOF`, the requester closed its write side;
  - `error`: anything else.
  The classification uses only `errors.As` / `errors.Is` against these types from `core/network` and `io`; the yamux package is not imported by pullsync (the libp2p stream wrapper is the package boundary).

The watcher reads from the stream itself, not through the protobuf reader. The reader created before `makeOffer` (`protobuf.NewWriterAndReader(stream)`) has not read anything yet; the existing code already relies on the requester sending nothing after its `Get` (the reader used for the `Get` is dropped, with whatever it may have buffered).

The handler then proceeds as after #640: `makeOffer(reqCtx, rn)` returns with the context error, the request unregisters itself, and the handler returns `make offer: <cause>` (the existing `context.Cause` check is extended to `errRequesterGone` and `errUnexpectedData`). Cancelling `reqCtx` releases this caller from the shared collection (`intervalsSF`) the same way #640's replacement does.

**Singleflight:** #640 avoids the data race inside `resenje.org/singleflight` v0.4.0 (a leaving caller reads `c.shared` after unlocking, `singleflight.go:86`, while a joining caller writes it, `:43`) only for requests it ends itself, by waiting until they have left before the new request joins. A request the watcher ends leaves asynchronously, whoever is joining at that moment, so this change makes the existing race between peers (#647) somewhat more frequent; it does not add a new kind. #647 tracks the library fix.

**Counting with #640:** the abandoned counter is incremented only when `context.Cause(reqCtx)` is the watcher's own error (`errRequesterGone` or `errUnexpectedData`), so a request #640 replaced first is counted only as replaced. #640's `register` currently counts every tracked same-start entry, including one the watcher has already cancelled but that has not yet unregistered; `register` is changed to skip entries whose context is already done (no cancel, no count, still removed when its own handler unregisters). With the watcher in place, `bee_pullsync_requests_replaced_total{reason="same_start"}` is expected to fall toward 0 for a requester that resets before asking again, which is every wasp and upstream requester (prediction to check in the measurement).

### 2. Stopping the watcher before the offer is written

When `makeOffer` returns (with or without error), and before `unregister`:

1. Set the stop flag, then `SetReadDeadline(time.Now())` on the stream. A pending yamux `Read` with an empty buffer returns `ErrTimeout`. A yamux `Read` consumes buffered data whatever the deadline (it checks the deadline only when the buffer is empty, `stream.go`), so this step is lossless only because a correct requester sends nothing before the `Offer`; any byte there is the protocol error of section 1.
2. Wait for the watcher goroutine to return (a channel it closes).
3. `SetReadDeadline(time.Time{})` to clear the deadline, so the later `Want` read is not affected.
4. If the watcher read a byte (`n > 0`), the handler treats the request as ended with `unexpected_data` even if `makeOffer` succeeded: it does not write the offer, counts it, and returns the error. A reset or close that arrives after the stop flag is set is **not** acted on here (the `Read` returned with `n == 0` after the stop); the handler writes the offer, and the write or the following `Want` read fails as it does today. Only `n > 0` counts at the boundary.

The watcher is stopped on every exit path of `makeOffer`, so no goroutine outlives the waiting phase. Steps 1 to 3 run after `makeOffer`, outside the #640 mutex.

yamux applies `SetReadDeadline` only while the read side is open (`readState == halfOpen`); after a reset or close the watcher's `Read` has already returned or returns at once, so step 2 does not wait on a read that cannot be woken. Setting and clearing the deadline closes and remakes a channel in yamux's `pipeDeadline`; no timer is started for a deadline already in the past.

### 3. Streams without read deadlines

`p2p.Stream` does not declare `SetReadDeadline`. The watcher runs only when the stream implements `interface{ SetReadDeadline(time.Time) error }`. yamux's `SetReadDeadline` always returns nil, so the type assertion is the only check that matters; a non-nil return from a first `SetReadDeadline(time.Time{})` is still treated as "unsupported" for other implementations. In production every stream is a `*libp2p.stream` embedding `network.Stream` over yamux (TCP and WebSocket transports, `libp2p.go`), which supports it. Otherwise the handler behaves as after #640 and counts the request once in `bee_pullsync_requests_unwatched_total`, so a transport change that loses deadlines is visible.

### 4. Cost

One goroutine and a 1-byte buffer per request while it builds its offer. The watcher starts for every request, not only those that end up waiting; for a request that finds chunks at once it lives for the duration of `makeOffer` (milliseconds). Requests in the waiting phase are bounded per peer by #640 (two per bin; 64 per peer with 32 bins, more until #643 checks the bin) and node-wide by the inbound stream cap (5,014). Today a request has the handler goroutine and its quit goroutine while waiting; the watcher adds a third. Two `SetReadDeadline` calls per request take the yamux state lock briefly and close and remake one channel; no timer. Negligible against the stream itself.

### 5. Metrics

- `bee_pullsync_requests_abandoned_total{reason}` with `reason` in `reset`, `disconnect`, `eof`, `error`, `unexpected_data`: waiting requests ended because the watcher saw the requester gone or misbehaving. Counted once per request, in the handler, only when `context.Cause(reqCtx)` is the watcher's error.
- `bee_pullsync_requests_unwatched_total`: requests that waited without a watcher because the stream has no read deadline.
- `bee_pullsync_requests_replaced_total` (#640) keeps its meaning; with the `register` change of section 1 it no longer counts entries the watcher already ended. A request ended by #640 before the watcher sees anything is counted there only.
- The handler's new errors (`errRequesterGone`, `errUnexpectedData`) do not wrap `network.ErrReset`, so libp2p's `StreamHandlerErrResetCount` (`libp2p.go`) does not count these requests; the new counter is where they show.

### 6. The cursor handler

`cursorHandler` reads the `Syn` and answers at once from `ReserveLastBinIDs`; it never waits, so it needs no watcher.

### 7. Documentation

A `docs/DIFFERENCES.md` row next to #640's: what is ended, the reasons, the two metrics, and that nothing changes on the wire.

## Protocol impact

None. No message, field, stream name, protocol ID or reset code changes. The server only reads from a stream it already owns and resets a stream whose requester is gone or broke the protocol. `make protocol-freeze` must report the wire surface unchanged.

## Configuration

None. The change ends only requests whose requester is gone or sent data a correct requester never sends, so it has no trade-off that needs a setting (rule 8).

## Measurement

**Test support:**

- `pkg/p2p/streamtest`: the recorder's `stream` gains `SetReadDeadline(t time.Time) error`. A read on the `record` waits on the data signal, the close, or the deadline, and setting a deadline **wakes a read that is already blocked** (as yamux's `pipeDeadline` does), returning `os.ErrDeadlineExceeded` with no bytes. A zero time clears it. The recorder's `Reset` is `FullClose`, so a requester's reset reaches the server as `io.EOF`; recorder tests assert `eof`.
- A **real yamux stream** for the reasons the recorder cannot produce (`reset` with `Remote`, `disconnect`, and the real `ErrTimeout` at the stop): a client and server `yamux` session pair over `net.Pipe`, with a thin adapter giving a yamux stream the `p2p.Stream` methods (and `SetReadDeadline`), wrapping errors as libp2p's stream wrapper does so the `core/network` types are what the handler sees. Test file `pkg/pullsync/watcher_yamux_test.go`.

**Tests** (`package pullsync_test`, a real server `Syncer` and the recorder; every test closes the server `Syncer`):

1. **Reproduction, using only signals that exist upstream:** a requester calls `Sync` for an empty bin with a context, then cancels the context; `Sync` returns and resets its stream. The test calls the recorder's `Records()` **from a separate goroutine** (it holds `recordsMu` while it waits for every handler) and waits for it with a deadline. On unmodified code the handler stays in `makeOffer` and the test fails with a message at its deadline, then closes the server `Syncer` so the handler and `goleak` (already run for the package, `main_test.go`) are satisfied; with the change `Records()` returns within the deadline. It uses no new metric and no new export, so it runs unchanged on upstream code, given the recorder's `SetReadDeadline` (which an upstream port would add with the change).
2. **Data before the offer:** a raw requester writes a `Get`, the test waits until `WaitingTracked() == 1` (the request is in its waiting phase), and only then writes one more byte: the request ends with reason `unexpected_data` and the stream is reset. (A byte sent together with the `Get` is discarded by the first reader; that case is not detected, by design.)
3. **No interference with a normal request:** a waiting request whose chunk then arrives (`PublishBin`, #640's mock extension) completes offer, want and delivery; the `Want` is read correctly after the watcher was stopped (no byte lost), and `requests_abandoned_total` stays 0.
4. **Data at the boundary:** with a hook between `makeOffer` and the watcher stop (the test-only hook #640 added in `export_test.go`), the requester writes one byte; the handler does not write the offer and counts `unexpected_data` once. A reset at the same point (yamux test) is not counted as abandoned: the offer write or the `Want` read fails as today.
5. **Interaction with #640:** (a) the same peer asks again for the same (bin, start) **without** resetting: the older request ends as `same_start` and is not counted as abandoned; (b) the peer resets and then asks again: the older request ends as abandoned (`eof` on the recorder) and `same_start` stays 0.
6. **Shared collection:** two peers wait on the same (bin, start). The test first makes sure peer 2's request is inside the shared collection's `Do` (a hook on the mock's `SubscribeBin`, or `WaitingTracked() == 2` plus the subscription count), then peer 1 resets; peer 2 keeps waiting and receives the chunk after `PublishBin`.
7. **No deadline support:** a stream wrapper without `SetReadDeadline`: the request waits as today and `requests_unwatched_total` rises by one.
8. **No goroutine leak:** after a run of requests that end in each way, the handler and watcher goroutines are gone; the package's `goleak` check covers this.
9. **Real yamux stream** (`watcher_yamux_test.go`): (a) the requester resets: reason `reset`; (b) the client session closes: reason `disconnect`; (c) the requester closes its write side: reason `eof`; (d) a normal request with a chunk: the stop's real `ErrTimeout` is ignored, the offer is written, the `Want` is read intact, and nothing is counted.

The package also runs under `make test-race`.

Mutation checks, each must fail a test:

- the watcher not started (test 1);
- the watcher's result ignored (`reqCtx` not cancelled) (test 1);
- the deadline not cleared after the stop (test 3: the `Want` read fails);
- the stop skipped, so the watcher keeps reading during the `Want` (test 3, or the race detector);
- data before the offer treated as a normal wait (test 2);
- the stop decided by the error value instead of the flag (test 9d: the normal request is counted as `error` and its offer not written);
- a connection close classified as `reset` (test 9b);
- abandonment counted without checking `context.Cause` (test 5a);
- `register` counting entries already done (test 5b);
- data at the boundary not checked, so the offer is written (test 4);
- an abandoned request counted twice with #640's replacement (test 5).

**Real nodes** (role names only): the build goes to the ten-node host with the next deployment.

- `bee_pullsync_requests_abandoned_total` by reason, `bee_pullsync_requests_unwatched_total` (expected 0), `bee_pullsync_waiting_requests` and `bee_libp2p_inbound_streams_per_peer_max`, compared with the #640 build over the same kind of period (a refill and the hours after it).
- A radius change of a neighbour without a restart (read from its `radius decrease` log) should raise `requests_abandoned_total{reason="reset"}` on our node by about the number of its quiet live bins, within seconds, where with #640 alone those requests stay waiting. A restart of a neighbour shows as `disconnect`, not `reset`, and also ends requests today through the connection close.
- `bee_pullsync_requests_replaced_total{reason="same_start"}` should fall toward 0 compared with the #640 build (prediction from section 1).
- A negative result: `unwatched` above 0 on production streams, `unexpected_data` above 0 from wasp or upstream requesters (a protocol assumption is wrong), or the waiting-request gauge unchanged after radius changes of neighbours.

## Rollout and rollback

Ships in the next build with no setting. Rollback is the previous build; abandoned requests that #640 does not catch then wait again until a chunk arrives or the peer disconnects.

## Upstream portability

The handler path is the same upstream except for #640's tracking, which this change builds on; the upstream port would carry both. The `affects-upstream` label for #641 waits for test 1 to fail against unmodified upstream code (with only the recorder's `SetReadDeadline` added) and pass with the change, which the implementation PR will report.
