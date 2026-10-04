# Pause the puller after a failed pull-sync request

Issue: [#573](https://github.com/crtahlin/wasp/issues/573)

## Problem

The puller's sync worker retries a failed pull-sync request at once, with no pause.

In `syncPeerBin` (`pkg/puller/puller.go`):
- An error other than `p2p.ErrPeerNotFound` is logged at debug level, and the loop continues.
- The only wait is `p.limiter.WaitN(ctx, count)`. After a failure `count` is 0, so it returns at once.
- The next `p.syncer.Sync` call follows straight away.

A peer that keeps failing therefore makes every worker for that peer repeat the request as fast as the network allows.

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

`pullsync.Sync` returns an error in two different situations:
- **No progress.** The request itself failed, for example a stream reset, and it returns `0, 0, err`.
- **Partial success.** Some chunks in an interval failed, for example on an unverified or expired stamp or an invalid chunk. It returns `topmost, count, chunkErr`, and the interval still advances: `top >= start` leads to `addPeerInterval` and `start = top + 1`.

Only the first case is a peer that keeps failing. A healthy peer whose offers include some bad chunks is in the second case and must not be slowed.

In the sync worker loop in `syncPeerBin`:

1. Keep a count of consecutive calls that made no progress: `err != nil` and `top < start`.
2. After such a call (other than `p2p.ErrPeerNotFound`, which still ends the worker as today), wait
   `min(base * 2^(failures - 1), max)`, plus a random 0 to 20 per cent of that.
   - The random part keeps a peer's workers from retrying in step. With 13 bins and two workers each, there are 26 of them.
   - The wait uses a timer that is stopped when the wait ends early, and it returns as soon as `ctx` ends.
3. A call that advances the interval (`top >= start`) resets the count to 0, with or without an error. Behaviour after progress is unchanged, including the rate limiter wait.
4. `base` defaults to 1 s and `max` to 1 minute. Both are fields on `Puller`, set from these defaults in `New`. `export_test.go` gets a setter that tests use before `Start`. They are not package variables, because the puller tests run in parallel, and changing a package variable from one test while another test's workers read it is a data race.

**Why the wait must end with `ctx`, not only at shutdown.** When a peer is removed, or the radius changes, `disconnectPeer` calls `syncPeer.stop()`, which waits for that peer's workers while holding `peer.mtx`, and is itself called under `syncPeersMtx` from the manage loop. A worker that ignored `ctx` during a 1-minute wait would block the whole manage loop for that long. `Close` would not reveal it: it gives up after 10 s and returns nil.

The result: a peer that keeps failing gets at most about 26 attempts per minute once the waits reach their maximum, instead of thousands per second. A peer that recovers is synced again within a minute.

## Protocol impact

None. Only the timing of the node's own retries changes. Messages, streams and protocol versions are unchanged, and `.github/protocol-freeze.lock` is untouched.

## Measurement

1. **Unit tests, `pkg/puller`.**
   - **Mock change:** `pullsync/mock` gets an error per reply: a field on `SyncReply` that, when set, overrides the mock-wide `syncErr`. Today `syncErr` applies to every reply, so a sequence of failures followed by a success cannot be built. Existing tests are unaffected.
   - **Peer that keeps failing:** queue N identical replies with no progress (`Topmost` below `Start`) and an error. With `base` at 100 ms, count the calls a worker makes in 1 s. Expected: at most 5 with the change. Without it, all N are used at once.
   - **Partial success is not paused:** replies that advance the interval and carry an error. All of them are used at once, as today. The existing `TestContinueSyncing` already sends such a reply.
   - **Reset:** two failures, then progress, then a failure. The wait after the last failure is the base wait, not the third step.
   - **The wait ends with `ctx`:** set `base` to 1 minute, put a worker into a wait, then remove the peer (`kad.ResetPeers()` and `kad.Trigger()`). The worker gauge (`SyncWorkerCounter`) must reach 0 within 1 s.
   - **Mutation checks:**
     - remove the wait;
     - pause on every error, including partial success;
     - do not reset after progress;
     - ignore `ctx` during the wait.

     Each must fail one of the tests above with a message, the last one through the 1 s assertion rather than a hang.
2. **sw-1.** Record `bee_puller_worker_errors` per second, idle process CPU, network traffic and goroutines for 30 minutes on the current build. Then deploy the change and record the same for 30 minutes. If the failing peer is gone by then, this is reported as not reproduced, not as a success.
   - **Success:** the error rate falls from about 2,000 per second to below one per second, and idle CPU, traffic and the goroutine count fall towards bench-1's level.
   - **A negative result:** the error rate falls but CPU and traffic do not. That would mean the cost lies elsewhere.

## Rollout and rollback

Always on. Rolling back means running a build without it. Nothing is stored.

## Upstream portability

The change is local to `syncPeerBin` plus two fields on `Puller`, and applies to upstream `v2.8.1` and `v2.8.2` as written. `waitWhileSampling`, which sits next to it in wasp, is fork-only and is not part of this change. The issue carries `affects-upstream`.

## Configuration

None. `base` and `max` bound a retry against a failing peer, not a throughput a node tunes for its hardware:
- a shorter maximum brings back part of the load on both nodes;
- a longer one delays recovery after a peer comes back.

Following rule 8, an option would only follow a measurement showing the values matter.
