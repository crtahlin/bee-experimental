# Use the chain's observed block time, and check the round against the contract

Issue: [#540](https://github.com/crtahlin/wasp/issues/540).
Type: fix.

## Problem

Gnosis Chain is becoming a rollup that settles on Ethereum
([GIP-153](https://forum.gnosis.io/t/gip-153-should-gnosis-chain-transition-into-the-ethereum-economic-zone/12397),
passed 2026-08-19). Genesis is targeted for December 2026 or January 2027. Its
blocks go from 5 s to 2 s, and the chain ID, addresses and state are kept.

Most of the node counts in blocks and needs no change. Three places turn blocks
into time with a block time the node assumes rather than observes. For mainnet
that is 5 s, hard-coded in `cmd/bee/cmd/start.go`, `getConfigByNetworkID`,
unless `block-time` is set.

**Table: where the node uses its configured block time, and what goes wrong at 2 s blocks**

| Use | Where | At 2 s blocks with the configured 5 s |
|---|---|---|
| Stamp TTL reported by the API: `remaining × blockTime ÷ price` | `pkg/api/postage.go`, `estimateBatchTTL` | Overstates every batch's remaining life 2.5 times, at the moment it really shrinks |
| How often the storage-incentives agent checks for a phase change: every `blockTime × 5` | `pkg/storageincentives/agent.go`, `start` | 25 s, which is 12.5 blocks. A phase lasts 38 blocks (76 s) and the sampling window about 114 blocks (228 s), so up to a third of a phase is lost before the node notices it. |
| How long the postage listener sleeps between syncs | `pkg/postage/listener/listener.go` | Sleeps 2.5 times too long; the listener falls behind, then catches up |

The node already measures the real block time. `pkg/transaction/wrapped`
computes an average from block headers, publishes it as the metric
`average_block_time_seconds`, and uses it to estimate block numbers. Nothing
else can read it.

The round length is compiled in (`DefaultBlocksPerRound = 152`) and never
checked against the contract, although the contract exposes `currentRound`. If
the Swarm contracts are ever redeployed with a different round length, an old
node would commit in the wrong phase and never notice.

The wasp docs budget the reserve sample against "190 s", in
`docs/experiments/staking-node/spec.md` (Gate 1),
`reserve-capacity-doubling/results.md` and `storage-engine-eval/followups.md`.
The code allows about 114 blocks: from claim start (block 76) to the next
round's reveal (block 190). That is 570 s at 5 s blocks and 228 s at 2 s blocks.

## Design

### 1. The observed block time, exposed and used

The wrapped backend exposes the average it already computes, as
`AverageBlockTime() time.Duration`. Until the first measurement it returns the
configured value, as its internal use does today. The three consumers read it
each time they need it, instead of taking a fixed duration at start. So a node
running through the switch adjusts without a restart.

- **TTL:** `remaining × AverageBlockTime ÷ price`, computed in nanoseconds and
  rounded to seconds. Today it multiplies by whole seconds.
- **Phase check:** every `5 × AverageBlockTime`, as today. At 2 s blocks that
  is 10 s instead of 25 s.
- **Listener sleep:** uses `AverageBlockTime`.

**Precedence.** An explicitly set `block-time` still wins: an operator who sets
it gets the old behaviour exactly. The hard-coded 5 s for mainnet becomes only
the starting value before the first measurement.

**A warning.** When the observed average differs from the configured value by
more than 20% for 10 minutes, the node logs a warning once. The warning names
both values and says the observed one is in use, or that the operator's
explicit setting overrides it.

**Why not only read the block time once at start:** a node running through the
switch would then use the wrong value until restarted.

### 2. The round, checked against the contract

At the start of each commit phase, the agent reads the contract's
`currentRound()`, one `eth_call` per round, and compares it with
`block ÷ 152`.
- If they differ, the node logs an error, does not commit in that round, and
  sets a metric. A node that does not commit is not penalised; one that commits
  in the wrong phase wastes gas.
- If the call fails, the node plays as today and logs at debug level. A failing
  RPC call must not stop the node from playing.

Reading every phase boundary from the contract instead is left for a later
change. It is needed only if the round length actually changes, and that would
come with a new contract and a new release anyway.

### 3. The sampling budget, stated in blocks

The three documents above state the budget as about 114 blocks, followed by its
wall-clock value at 5 s and at 2 s. Gate 1 in the staking-node spec is
restated the same way.

### Not in this spec

- **How many blocks the postage listener treats as final** (`tailSize = 4`).
  After the switch, Ethereum reorgs roll the chain back about 6 blocks. Making
  this a setting needs the reorg depth measured on the EEZ testnet first
  (rule 8), so it is its own issue.
- **Contract-side changes** (the stamp price at the switch, the round length).
  They belong to the Swarm team, not to the client. They are described in the
  Swarm team's analysis.

## If upstream Bee fixes it first

Upstream issue ethersphere/bee#5386 asks to detect the block time from the
RPC endpoint. If an upstream *release* covers part 1, it is absorbed by the
normal sync, and only what remains is implemented here. A fix only on upstream
`master` is not taken (`docs/agent-playbooks/upstream-sync.md`).

## Protocol impact

None. No message, field or constant in `.github/protocol-freeze.lock` changes.
The node's own timing and one extra read-only contract call change.

## Tests

- **Wrapped backend:** `AverageBlockTime` returns the configured value before
  any measurement, and the observed average after.
- **API:** the TTL follows a stubbed average (2 s gives 40% of the 5 s value).
- **Agent:** the phase-check interval follows a stubbed average. An explicit
  `block-time` overrides it.
- **Round check:** a mismatch skips the commit and sets the metric; a failing
  call does not.
- **Warning:** logged once after the difference has lasted the full period, and
  not for a brief difference.

Mutations:
- use the configured value in the TTL;
- drop the explicit override;
- commit despite a round mismatch;
- stop playing when the round call fails.

## Measurement

1. **Now, on Sepolia:** Swarm's testnet runs there at 12 s blocks, which already
   differs from mainnet's 5 s. A testnet node with `block-time` left unset
   reports a TTL within 5% of the batch's real remaining blocks × 12 s, and
   polls phases every 60 s. Without the fix it uses the testnet default of
   12 s, so this checks the plumbing, not a mismatch.
2. **A mismatch:** a bench node on mainnet with `block-time: 2` explicitly set
   (wrong on purpose) against the observed 5 s. It logs the warning, and uses 2
   s, because an explicit setting wins. Unset, it uses the observed 5 s.
3. **After the switch,** on the EEZ testnet or mainnet: the TTL and the phase
   detection delay, three runs each.

## Rollout and rollback

No new setting. `block-time` keeps its meaning when set. Rollback is reverting
the merge.

## Upstream portability

All three consumers and the hard-coded mainnet block time are unmodified
upstream code (`upstream/v2.8.2`). The change applies to upstream as is.

## Files

- `pkg/transaction/wrapped/wrapped.go` (expose the average)
- `pkg/api/postage.go`, `pkg/storageincentives/agent.go`,
  `pkg/postage/listener/listener.go`, `pkg/node/node.go` (wiring)
- `pkg/storageincentives/redistribution` (the `currentRound` call)
- the three documents in part 3
- tests next to each
- `docs/DIFFERENCES.md`, `docs/experiments/INDEX.md`

Generated with help of AI.
