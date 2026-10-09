# Pull-sync: three follow-ups to ending older waiting requests

Issues: #645, #646, #648. All three come from the review of #644, the code for #640 (end an older waiting pull-sync request when the same peer asks again). Related: #641 (end a waiting request when the requester is gone), #647 (a race inside the singleflight library, specified separately).

## Terms

- **Waiting request:** an inbound pull-sync request that is still building its offer, tracked by `waitingRequests` (`pkg/pullsync/waiting.go`) under a key of peer and bin.
- **Wait loop:** the handler's loop that waits, after `register`, until the requests it ended have left (`pkg/pullsync/pullsync.go`, the `for _, e := range ended` loop before `makeOffer`).

## Problem

1. **#645: nothing pins that a request is unregistered right after its offer is built.** In the review of #644, a mutation that moved `s.waiting.unregister(entry)` to a deferred call at the end of the handler passed every test. If that regressed, a request in the Want or delivery phase would still be tracked. It would count against the cap of two per peer and bin, could push out a request that is really waiting, would inflate `bee_pullsync_waiting_requests`, and a newer request would sit in its wait loop until the old one finished delivering.
2. **#646: nothing pins that empty keys are deleted.** `unregister` deletes a key whose list becomes empty. A mutation that keeps empty keys passed every test, because `WaitingTracked` reports the request count, not the map size. Without the deletion the map keeps one key for every peer and bin it has ever seen.
3. **#648: an ended request still in its wait loop starts work it cancels at once.** When a request is ended (by a newer request, #640, or by its requester going away, #641) while it is still in its own wait loop, the loop exits on `reqCtx.Done()` and the handler goes on to call `makeOffer` with that cancelled context. If no other caller holds the key, singleflight starts a new collection goroutine and calls `SubscribeBin`, and both are cancelled straight away. It always ends, so it is wasted work, not a leak.

## Change

1. **#648, the only behaviour change.** Right before `makeOffer`, if `reqCtx.Err() != nil`, skip `makeOffer` and take the same path as a `makeOffer` that returned that error: `afterMakeOffer`, `watch.stop()`, `unregister`, and the existing handling of the cancel cause (#641 abandonment counting, #640 replacement wording). The metrics and the error returned are the same as today for an ended request; only the extra collection and subscription are gone.
2. **#646:** a test-only accessor in `export_test.go`, `WaitingKeys() int`, returning `len(s.waiting.m)` under the mutex.
3. No change to the wire, to settings, or to metrics.

## Tests

1. **Unregistered after the offer (#645).** A requester on a recorder stream sends a `Get` for a bin with chunks prepared, reads the `Offer`, and does not send its `Want`. While the server waits for the `Want`, `WaitingTracked()` is 0 and the gauge reads 0. The requester then sends its `Want` and the handler completes normally.
2. **Empty keys deleted (#646).** Extend `TestWaitingRequestsGauge`: after all requests have ended, `WaitingKeys()` is 0, as well as `WaitingTracked()`.
3. **No collection for a request ended in its wait loop (#648).** Request A (peer P, bin b, start s) waits. The `afterMakeOffer` hook is set to block A's handler, so A does not unregister after it is ended. Request B (P, b, s) ends A and enters its wait loop. Request C (P, b, s) ends B. Then A is released. The storer mock's `SubscribeBinCalls()` must show no call for B: A's call and C's call only. On today's code B adds one.

**Mutation checks** (each must make a test fail):
- unregister moved to a deferred call at the end of the handler (test 1);
- empty keys kept in `unregister` (test 2);
- the `reqCtx.Err()` check removed (test 3).

Run `go test -race` on `pkg/pullsync` and read the exit status directly.

## Upstream

None of the three is an upstream defect: the code they touch was added by #640 (#644).

## Measurement

None needed on a node: #645 and #646 add tests only, and #648 removes a collection that ends at once. The production check of #640 (waiting requests per peer stay at most 2 per bin) covers this code.
