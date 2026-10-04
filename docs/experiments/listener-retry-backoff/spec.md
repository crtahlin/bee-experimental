# Pause the postage listener after a failed log query

Issue: [#578](https://github.com/crtahlin/wasp/issues/578)

## Problem

`Listen` in `pkg/postage/listener/listener.go` sleeps at the top of each pass only when `paged` is false. A pass that finds the node a full page or more behind (5,000 blocks with the RPC backend, 50,000 with the snapshot backend) sets `paged = true` before it calls `FilterLogs`, so that the next page follows without a pause.

When that `FilterLogs` call fails, the error branch logs a Warning, sets `lastConfirmedBlock = 0` and runs `continue` with `paged` still true. The next pass skips the sleep and calls `BlockNumber` and `FilterLogs` again at once. This repeats for as long as the error lasts, at the rate of the RPC round trip. The only limit is `stallingTimeout` (10 minutes), after which `Listen` returns `ErrPostageSyncingStalled` and the node shuts down.

The other error paths in the loop, a failed `BlockNumber`, `to < confirmationDepth` and `to < from`, run `continue` with `paged` already false, so they wait `backoffTime` (5 s) or the block-time estimate.

Upstream `v2.8.2` has the same loop.

## When it happens

- The node is catching up: it was offline for more than about 7 hours at 5 s blocks, or about 2.8 hours at 2 s blocks after GIP-153, or it is a new node. The batch snapshot built into the binary is usually days or weeks old, so a node that uses it still pages through the RPC endpoint for the blocks after the snapshot ends.
- `eth_getLogs` keeps failing for a page. Possible causes: a provider limit on block range or result size, rate limiting (HTTP 429, which the immediate retries make worse), or an outage of the log endpoint while `BlockNumber` still answers.

## Hypothesis

The retry rate comes only from `paged` staying true on the error path. If the error branch clears `paged`, a failing log query is retried after `backoffTime`, like every other error in the loop. The node then makes at most about 120 failed queries in any 10 minutes without progress, instead of one per round trip. The 10-minute limit counts from the last successful page, so errors that come and go can still add up to more queries in total, but never at the round-trip rate.

## Design

In the `FilterLogs` error branch, set `paged = false` before `continue`. Because the branch also sets `lastConfirmedBlock = 0`, the next pass waits `backoffTime`, which the select at the top of the loop ends early when `ctx` ends.

A successful page is unchanged: it still sets `paged = true` and the next page follows at once.

This deliberately does not add a growing wait. `backoffTime` is already the wait for every other error in this loop, and `stallingTimeout` already bounds how long the node keeps trying. A growing wait would also delay recovery when the provider comes back.

**Not a goal of this change:** keeping the node running when every query for a page fails. With a provider limit on block range or result size, each retry asks for the same page and fails again, so the node still stops after 10 minutes, as it does today. This change only stops the immediate retries. Making the page smaller after a failed query is a separate change, tracked in #583.

## Protocol impact

None. Only the timing of this node's own chain queries changes. No peer message, stream or protocol version is involved, and `.github/protocol-freeze.lock` is untouched.

## Measurement

This is a defect fix, not an optimization. It is accepted on a test that fails on the current code.

1. **Unit test, `pkg/postage/listener`.** A new test filterer, because the existing `mockFilterer` never returns an error: `BlockNumber` returns 200000, `FilterLogs` always fails at once, and an atomic counter records the `FilterLogs` calls. The start block is 0, so the first page is paged. `backoffTime` is 200 ms and `stallingTimeout` is 5 s. Wait about 1 s, read the counter and the elapsed time together, then stop the listener.
   - Expected with the fix: at least 2 calls, which shows the listener still retries, and at most `1 + elapsed / backoffTime` calls. Measuring the elapsed time keeps the bound correct when the test goroutine wakes late on a busy runner.
   - On the current code the count is in the thousands.
2. **The page that follows a recovered error is still fetched without a pause.** Two failures, then a successful page with no events, then the next page. The time between the third and fourth `FilterLogs` calls must be below `backoffTime / 2`. The test uses an updater that does not block (the existing one sends on a channel with a buffer of 1). This test passes on the current code too: it guards against a fix that is too broad, such as clearing `paged` after every query, rather than demonstrating the defect.
3. **Mutation check:** remove the new `paged = false` and confirm the first test fails with a message.
4. **The existing tests** for the listener pass unchanged, in particular `TestListenerPageSize` and the `BlockNumber` error cases.

No node measurement is planned. Reproducing the condition on a node needs a long catch-up with a failing log endpoint, and the change has no effect when queries succeed.

## Rollout and rollback

Always on. Rolling back means running a build without it. Nothing is stored.

## Upstream portability

The change is one line in `Listen` and applies to upstream `v2.8.2` as written. The issue carries `affects-upstream`.

## Configuration

None. `backoffTime` is already a constructor argument, set by the node.
