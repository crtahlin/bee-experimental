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
2. the stream's handler context (`streamCtx`, which libp2p cancels on a disconnect; not the handler's local `ctx`, which `s.quit` also cancels, `pullsync.go:173-180`) has ended and `s.quit` is still open, and the cause is not `errRequestReplaced`: counted under the watcher's reason when it recorded one, otherwise as `disconnect`. A **nil watcher** (a stream without a read deadline, `watcher.go:56-59`) counts as `disconnect`; such unwatched requests were never counted before and now are.

A request whose offer was built just as `streamCtx` ended is counted as `disconnect` and its offer is not written; the write would fail anyway. A request the same peer replaced (#640) keeps that cause and is counted only under `requests_replaced_total`, also when its connection then drops. A node that is stopping mostly counts nothing; during a real shutdown the handler contexts can end shortly before `s.quit` closes (`pkg/node/node.go:1975-1985`), so a few requests can be counted as `disconnect` then. The handler's returned error wraps `errRequesterGone` in case 2 as well.

The metric's help text says the count covers requests whose connection ended, including connections this node closed itself (a disconnect or blocklist, `pkg/p2p/libp2p/peer.go:228-235`), and unwatched requests.

### Tests

The handler is called directly (`s.handler` through an export), with a `streamCtx` the test cancels, because `streamtest` passes `context.Background()` to handlers (`pkg/p2p/streamtest/streamtest.go:168`).

1. **Parent first:** a request waits; the test cancels `streamCtx` without any stream error. The request is counted once as `disconnect`. On unmodified code it is not counted.
2. **Watcher first:** the stream is reset by the requester; counted once under `reset`, as today.
3. **Nil watcher:** a stream without a read deadline, `streamCtx` cancelled: counted once as `disconnect`.
4. **Shutdown:** the syncer is closed while a request waits, `streamCtx` live; nothing is counted.
5. **Replacement, then disconnect:** a request ended by a newer one from the same peer, then its `streamCtx` cancelled: counted only under `requests_replaced_total`.

**Mutation checks:** case 2 removed (tests 1, 3); the nil-watcher case dereferenced or dropped (test 3); the `s.quit` check removed (test 4); the replacement cause not given precedence (test 5). Checking the local `ctx` instead of `streamCtx` is an **equivalent mutation**: with `s.quit` open the two contexts end together (`pullsync.go:170-180`), and with it closed the `s.quit` check decides; it is not a test target. (Noted in the review of this spec.)

## #687: the inbound stream group gauge

### Problem

`reserveGroup` and `releaseGroup` (`pkg/p2p/libp2p/streamlimits.go:253-280`) change the group's count under the mutex and then, after unlocking, `Set` the gauge `bee_libp2p_inbound_streams_group{group}` to the count they computed. Two concurrent changes can call `Set` in the opposite order of their counts, leaving the gauge one off until the next change. Each `Set` also looks up the label child.

**Evidence:** reasoned from the code. A stress test (64 goroutines, 2,000 reserve-and-release pairs each, checking the gauge reads 0 at the end) did not show it in 270 runs on unmodified code on a 15-core Mac, so a test cannot be the proof here.

### Change

The two gauge children are looked up once, in `newStreamLimits`, and kept. `reserveGroup` calls `Inc` and `releaseGroup` calls `Dec` on them when (and only when) the count changes, inside the existing lock, so the gauge always equals the count and never briefly reads -1 (a release's `Dec` cannot reach the gauge before its reserve's `Inc`). No label lookup per stream remains.

### Tests

1. **Gauge tracks the count:** after a sequence of reserves, refusals at the limit and releases in both groups, each gauge equals the group's count, and refusals do not change it.
2. **Concurrent changes end at 0:** the stress test above, kept as a regression test for the commutative form (it cannot fail on the old form in practice; that is stated in the test).

**Mutation checks:** `Inc` on a refused reserve (test 1); `Dec` missing on release (tests 1, 2); the wrong group's child (test 1).

## Common

No wire change (rule 6), no setting, no behaviour change. Race detector on `pkg/pullsync` and `pkg/p2p/libp2p` (the latter's full race run on the Mac hits the known NAT-PMP race in go-libp2p, so the new tests run under `-race` by name and the package's full race run is CI's); check the exit status directly.

## Measurement

None beyond the tests for #687. For #672, on the dense host after deployment: restart one node (node R) and compare, on each of its neighbours that runs this build, the step in `requests_abandoned_total{reason="disconnect"}` at the restart with the drop in that neighbour's own `bee_pullsync_waiting_requests` at the same moment (the requests R had waiting there). Summed over the neighbours they should match within a few; as a rough cross-check only, the total can be compared with R's `bee_puller_worker` just before the restart; that gauge is node-wide and includes historical workers (`pkg/puller/metrics.go`), so it cannot be split by neighbour and only bounds the total from above. (`waiting_requests` on R itself counts the neighbours' requests waiting at R, not R's, so it is not the comparison.)

Generated with help of AI.
