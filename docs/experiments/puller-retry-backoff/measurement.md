# Puller retry wait: measurement

Issue: [#573](https://github.com/crtahlin/wasp/issues/573). Change: [#575](https://github.com/crtahlin/wasp/pull/575), merged as [`ab7ed195`](https://github.com/crtahlin/wasp/commit/ab7ed195).

## Result: not reproduced on a node

The defect was measured before the change. The change itself could not be measured on a node, because restarting the node cleared the condition on both builds.

**Table: sw-1, a test node at `reserve-capacity-doubling: 3`, filled and otherwise idle, 30 minutes per run, 2026-10-04**

| Run | Build | Puller errors per second | Process CPU (cores) | Received (kB/s) | Sent (kB/s) | Goroutines at end |
|---|---|---|---|---|---|---|
| Before, node running since the previous day | `main` eae4ae1a | 2,025.6 | 1.097 | 811 | 643 | 8,707 |
| After, node restarted on the change | 2a076ed4 | 0.0 | 0.102 | 61 | 58 | 5,151 |

**Control:** the node was then restarted on the old build (eae4ae1a), and `bee_puller_worker_errors` was read once a minute for 14 minutes. It stayed at 0. After a restart, the failing peer no longer caused failures on either build, so the drop in the table comes from the restart, not from the change.

How each figure was taken: the difference between two readings 1,800 s apart of the process's CPU time (`/proc/<pid>/stat`), the network interface counters (`/proc/net/dev`), `bee_puller_worker_errors` and `go_goroutines`.

## What is shown

- **The defect.** On the old build, one peer caused about 2,000 failed pull-sync calls per second for hours, at 1.1 to 1.4 cores. See the spec.
- **The change, in unit tests only.** `pkg/puller/retry_test.go` covers the pause, no pause after partial success, the reset after progress, the wait ending with the worker's context, and the wait bounds. Four mutations of the change are each caught by one of these tests.

## Still open

sw-1 runs the change. If the condition returns, it will show as a low error rate, a few calls per minute per worker, instead of thousands per second. That would be the field measurement, and it would move the ledger status from `merged` to `validated`.
