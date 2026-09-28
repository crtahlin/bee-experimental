# Results: using the observed block time, and checking the round

Issue: [#540](https://github.com/crtahlin/wasp/issues/540). Spec:
[spec.md](spec.md). Merged as `8f3f362c` (#549), tag `exp-observed-block-time`.

## Outcome

**Validated for what a 5 s chain can show.** On `bench-1` (Gnosis Chain,
5 s blocks), the node used the observed block time when `block-time` was
unset, used an explicit `block-time` exactly as set, and logged the drift
warning once when the two differed. The behaviour at 2 s blocks can only be
measured after Gnosis Chain's switch (GIP-153).

## Setup

- `bench-1` on `main` at `870dbd54`, 2026-09-28.
- The block time the node uses is read back from the API: the TTL of the
  longest-lived batch divided by its remaining blocks, from `/batches` and
  `/chainstate`. The backend's own block-time metric is not exposed on
  `/metrics`.

## Results

**Table: block time the node used, read back from the stamp TTL, bench-1, 2026-09-28**

| Configuration | Sample 1 | Sample 2 | Sample 3 | Drift warning |
|---|---|---|---|---|
| `block-time` unset (observed) | 5.0 s | 5.556 s | 5.0 s | none, as expected |
| `block-time: 2` set explicitly | 2.0 s | 2.0 s | 2.0 s | once, at 18:49:51, 11 min 51 s after the restart: `observed=5s configured=2s` |
| unset again, after restoring the configuration | 5.0 s | | | |

- **Unset:** the node used the chain's block time. 5.556 s is a real
  measurement: 9 blocks in 50 s, a window containing a missed slot. That makes
  the TTL move by about 11% between refreshes, filed as
  [#556](https://github.com/crtahlin/wasp/issues/556).
- **Explicit:** a wrong value is used as set, and the operator is told once.
  The warning appears 10 minutes after the first measurement that differs,
  which is the designed delay plus the time to the first measurement.
- The node restarted cleanly each time (`NRestarts` 0).

## Not measured on a node

- **The round check.** `bench-1` is not staked, so it never commits. The check
  is covered by unit tests, including the one-round tolerance added in review.
- **The phase-check interval.** It is covered by a deterministic unit test
  (`TestAgentPhaseCheckFollowsBlockTime`).
- **Following a live change of block time.** This needs the chain to change,
  so it waits for the switch or the EEZ testnet.

Generated with help of AI.
