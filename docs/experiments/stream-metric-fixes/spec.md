# Two metric fixes: abandoned pull-sync requests on a disconnect, and the inbound stream group gauge

Issues: #672 (the `disconnect` count of abandoned pull-sync requests undercounts) and #687 (the inbound stream group gauge can be set out of order). Both are metrics of wasp's own recent changes (#641 and #638), neither changes what a node does, and each is a few lines; one short spec and one pull request, as #645, #646 and #648 shared one. Neither concerns upstream code.

## #672: abandoned requests on a disconnect

### Problem

When a waiting pull-sync request's connection drops, two things race:
- libp2p cancels the handler's context (`pkg/p2p/libp2p/peer.go`, `peerRegistry.Disconnected`), which is the parent of the request's own context (`reqCtx`, `pkg/pullsync/pullsync.go`, created from the handler context);
- the #641 watcher's read fails, and the watcher cancels `reqCtx` with `errRequesterGone` and records the reason `disconnect`.

The handler counts an abandoned request only when `context.Cause(reqCtx)` is the watcher's error (`pullsync.go:237-240`). When the parent's cancel arrives first, the cause is `context.Canceled`, the watcher's cancel is a no-op, and the request is not counted at all, whatever the watcher saw. When `watch.stop()` then wins the race against the watcher's read, the watcher returns without a reason. So `bee_pullsync_requests_abandoned_total{reason="disconnect"}` undercounts by a varying amount, and the #641 measurement ("a neighbour's restart shows as `disconnect`") reads low.

### Change

After `makeOffer`, `watch.stop()` and `unregister`, the abandoned request is counted when either:
1. the cause is the watcher's error, as today, counted under the watcher's reason; or
2. the handler's own context has ended (the connection dropped) and the node is not stopping (`s.quit` is still open): counted as `disconnect`, unless the watcher recorded a reason, which is used instead.

A request ended because the same peer replaced it (#640) is not affected: its handler context is still live. A node that is stopping counts nothing, as today. The handler's returned error wraps `errRequesterGone` in case 2 as well. The metric's help text says the count covers requests whose connection ended.

### Tests

1. **Parent first:** a request waits; the test cancels the handler context (as libp2p does on a disconnect) without any stream error. The request is counted once as `disconnect`. On unmodified code it is not counted.
2. **Watcher first:** the stream is reset by the requester; counted once under `reset`, as today.
3. **Shutdown:** the syncer is closed while a request waits; nothing is counted.
4. **Replacement:** a request ended by a newer one from the same peer is counted only under `requests_replaced_total`, as today.

**Mutation checks:** case 2 removed (test 1); the `s.quit` check removed (test 3); case 2 applied while the handler context is still live (test 4).

## #687: the inbound stream group gauge

### Problem

`reserveGroup` and `releaseGroup` (`pkg/p2p/libp2p/streamlimits.go:253-280`) change the group's count under the mutex and then, after unlocking, `Set` the gauge `bee_libp2p_inbound_streams_group{group}` to the count they computed. Two concurrent changes can call `Set` in the opposite order of their counts, leaving the gauge one off until the next change. Each `Set` also looks up the label child.

**Evidence:** reasoned from the code. A stress test (64 goroutines, 2,000 reserve-and-release pairs each, checking the gauge reads 0 at the end) did not show it in 270 runs on unmodified code on a 15-core Mac, so a test cannot be the proof here.

### Change

The two gauge children are looked up once, in `newStreamLimits`, and kept. `reserveGroup` calls `Inc` and `releaseGroup` calls `Dec` on them when (and only when) the count changes, outside the mutex. Increments and decrements commute, so the gauge equals the count whatever the order in which concurrent changes reach it; no ordering under the lock is needed, and no label lookup per stream remains.

### Tests

1. **Gauge tracks the count:** after a sequence of reserves, refusals at the limit and releases in both groups, each gauge equals the group's count, and refusals do not change it.
2. **Concurrent changes end at 0:** the stress test above, kept as a regression test for the commutative form (it cannot fail on the old form in practice; that is stated in the test).

**Mutation checks:** `Inc` on a refused reserve (test 1); `Dec` missing on release (tests 1, 2); the wrong group's child (test 1).

## Common

No wire change (rule 6), no setting, no behaviour change. Race detector on `pkg/pullsync` and `pkg/p2p/libp2p` (the latter's full race run on the Mac hits the known NAT-PMP race in go-libp2p, so the new tests run under `-race` by name and the package's full race run is CI's); check the exit status directly.

## Measurement

None beyond the tests for #687. For #672, on the dense host after deployment: restart one node and compare its neighbours' `requests_abandoned_total{reason="disconnect"}` steps with the number of pull-sync requests that node had open (from its `bee_pullsync_waiting_requests` just before the restart); they should now match within a few.

Generated with help of AI.
