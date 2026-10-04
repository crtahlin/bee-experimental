# Pause the puller after a failed pull-sync request

Issue: [#573](https://github.com/crtahlin/wasp/issues/573)

## Problem

The puller's sync worker retries a failed pull-sync request at once, with no pause.

In `syncPeerBin` (`pkg/puller/puller.go`):
- An error other than `p2p.ErrPeerNotFound` is logged at debug level, and the loop continues.
- The only wait is `p.limiter.WaitN(ctx, count)`. After a failure `count` is 0, so it returns at once.
- The next `p.syncer.Sync` call follows straight away.

A peer that keeps failing therefore makes every worker for that peer spin as fast as the network allows.

Measured on sw-1, a test node: wasp `main` at eae4ae1a, `reserve-capacity-doubling: 3`, filled and otherwise idle.
- `bee_puller_worker_errors` and `bee_puller_worker_iterations` both rose by 2,009 per second.
- 20,121 of 20,122 failures in 6 minutes came from one peer, across bins 6 to 18. The error was always `send headers: read message: stream reset: stream reset`.
- The node used 1.4 CPU cores while idle, received about 800 kB/s and sent about 650 kB/s. Its goroutine count rose from about 6,000 to 8,400 overnight.
- The same configuration on bench-1, without such a peer, idles at 0.069 cores, 72 kB/s received and 47 kB/s sent (#566).
- A CPU profile taken during the condition put about 54 per cent in `pkg/puller` and `pkg/pullsync`, and their libp2p streams.

Upstream `v2.8.1` and `v2.8.2` have the same loop.

## Hypothesis

The cost comes from the immediate retries, not from the failures themselves. With a pause that grows while a peer keeps failing, one failing peer costs a few attempts per minute per worker, instead of thousands per second. The node's idle CPU and traffic then return to the level seen on bench-1. Why the peer resets the streams is not known. This change does not depend on it.

## Design

In the sync worker loop in `syncPeerBin`:

1. Keep a count of consecutive failed `Sync` calls for that worker.
2. After a failed call, other than `p2p.ErrPeerNotFound`, which still ends the worker as today, wait
   `min(retryBackoffBase * 2^(failures - 1), retryBackoffMax)` before the next iteration. The wait honours `ctx`: if the context ends during the wait, the worker returns, as it does on cancellation today.
3. A call without an error resets the count to 0. Behaviour after a success is unchanged, including the rate limiter wait.
4. The constants are `retryBackoffBase = 1 * time.Second` and `retryBackoffMax = 1 * time.Minute`. They are package variables, so a test can shorten them through `export_test.go`.

This pauses a failing worker for at most a minute between attempts. A peer that recovers is synced again within that minute. Live syncing of a healthy peer is untouched, because the pause applies only after an error.

## Protocol impact

None. Only the timing of the node's own retries changes. Messages, streams and protocol versions are unchanged, and `.github/protocol-freeze.lock` is untouched.

## Measurement

1. **Unit test, `pkg/puller`.** A mock peer whose every `Sync` call fails, using `pullsync/mock` with `WithReplies` and `WithSyncError`. With the base pause shortened to 100 ms, the test counts the calls a worker makes in a fixed window:
   - **with the change:** a handful, bounded by the pause schedule;
   - **without it:** every queued reply, consumed at once.

   A second case checks that a success after failures resets the pause.

   **Mutation checks:**
   - remove the wait;
   - stop resetting the count on success;
   - ignore `ctx` during the wait.

   Each must fail the test, the last one through a test deadline rather than a hang.
2. **sw-1.** Record `bee_puller_worker_errors` per second, idle process CPU and network traffic for 30 minutes on the current build. Then deploy the change and record the same for 30 minutes. If the failing peer is gone by then, this is reported as not reproduced, not as a success.
   - **Success:** the error rate falls from about 2,000 per second to a few per minute, with idle CPU and traffic close to bench-1's.
   - **A negative result:** the error rate falls but CPU and traffic do not. That would mean the cost lies elsewhere.

## Rollout and rollback

Always on. Rolling back means running a build without it. Nothing is stored.

## Upstream portability

The change is local to `syncPeerBin` and applies to upstream `v2.8.1` and `v2.8.2` as written. The issue carries `affects-upstream`.

## Configuration

None. These constants bound a retry against a failing peer, not a throughput a node tunes for its hardware:
- a shorter maximum brings back part of the load on both nodes;
- a longer one delays recovery after a peer comes back.

Following rule 8, an option would only follow a measurement showing the values matter.
