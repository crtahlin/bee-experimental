# Pause the postage listener after a failed log query

Issue: [#578](https://github.com/crtahlin/wasp/issues/578)

## Problem

`Listen` in `pkg/postage/listener/listener.go` sleeps at the top of each pass only when `paged` is false. A pass that finds the node 5,000 or more blocks behind sets `paged = true` before it calls `FilterLogs`, so that the next page follows without a pause.

When that `FilterLogs` call fails, the error branch logs a Warning, sets `lastConfirmedBlock = 0` and runs `continue` with `paged` still true. The next pass skips the sleep and calls `BlockNumber` and `FilterLogs` again at once. This repeats for as long as the error lasts, at the rate of the RPC round trip. The only limit is `stallingTimeout` (10 minutes), after which `Listen` returns `ErrPostageSyncingStalled` and the node shuts down.

The other error paths in the loop, a failed `BlockNumber`, `to < confirmationDepth` and `to < from`, run `continue` with `paged` already false, so they wait `backoffTime` (5 s) or the block-time estimate.

Upstream `v2.8.2` has the same loop.

## When it happens

- The node is catching up: it was offline for more than about 7 hours at 5 s blocks, or about 2.8 hours at 2 s blocks after GIP-153, or it is a new node without the batch snapshot.
- `eth_getLogs` keeps failing for a page. Possible causes: a provider limit on block range or result size, rate limiting (HTTP 429, which the immediate retries make worse), or an outage of the log endpoint while `BlockNumber` still answers.

## Hypothesis

The retry rate comes only from `paged` staying true on the error path. If the error branch clears `paged`, a failing log query is retried after `backoffTime`, like every other error in the loop. One catch-up attempt then makes at most about 120 failed queries before the 10-minute stall limit, instead of one per round trip.

## Design

In the `FilterLogs` error branch, set `paged = false` before `continue`. Because the branch also sets `lastConfirmedBlock = 0`, the next pass waits `backoffTime`, which the select at the top of the loop ends early when `ctx` ends.

A successful page is unchanged: it still sets `paged = true` and the next page follows at once.

This deliberately does not add a growing wait. `backoffTime` is already the wait for every other error in this loop, and `stallingTimeout` already bounds how long the node keeps trying. A growing wait would also delay recovery when the provider comes back.

## Protocol impact

None. Only the timing of this node's own chain queries changes. No peer message, stream or protocol version is involved, and `.github/protocol-freeze.lock` is untouched.

## Measurement

This is a defect fix, not an optimization. It is accepted on a test that fails on the current code.

1. **Unit test, `pkg/postage/listener`.** A filterer whose `BlockNumber` returns 200000 and whose `FilterLogs` always fails at once, with the start block at 0 so that the first page is paged. `backoffTime` is set to 200 ms. Count the `FilterLogs` calls in 1 s.
   - Expected with the fix: at most 6, the first call plus one every 200 ms.
   - On the current code the count is in the thousands.
2. **The page that follows a recovered error is still fetched.** Two failures, then success: the listener reaches the second page without a pause, which shows that a success still sets `paged`.
3. **Mutation check:** remove the new `paged = false` and confirm the first test fails with a message.
4. **The existing tests** for the listener pass unchanged, in particular `TestListenerPageSize` and the `BlockNumber` error cases.

No node measurement is planned. Reproducing the condition on a node needs a long catch-up with a failing log endpoint, and the change has no effect when queries succeed.

## Rollout and rollback

Always on. Rolling back means running a build without it. Nothing is stored.

## Upstream portability

The change is one line in `Listen` and applies to upstream `v2.8.2` as written. The issue carries `affects-upstream`.

## Configuration

None. `backoffTime` is already a constructor argument, set by the node.
