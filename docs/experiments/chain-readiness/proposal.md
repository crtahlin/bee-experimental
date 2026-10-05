# Chain readiness: Glamsterdam, Gnosis EEZ, and chains with different parameters

Status: proposal, 2026-10-05. Nothing here is implemented yet except where it says so. Each work item below is meant to become one issue, and then follow the usual order: issue, merged spec, code with tests, review, merge.

## Why

Changes to the chain a node runs on can reach every node, on dates wasp does not control. The client should not assume one chain's parameters.

**Table: chain changes that affect a Swarm node, with what is known about their dates on 2026-10-05**

| Change | What it means for a node | Date |
|---|---|---|
| **Glamsterdam**, an Ethereum hard fork that adds state gas (EIP-8037) | Some transactions need much more gas. A chequebook deployment goes from about 142,000 gas to an estimated 616,000. | Sepolia, where the Swarm testnet runs: 2026-10-06 13:53:36 UTC. Ethereum mainnet: "Q4 2026", no date. Gnosis Chain: not announced; it followed the previous Ethereum fork about four months later. |
| **Gnosis EEZ** ([GIP-153](https://forum.gnosis.io/t/gip-153-should-gnosis-chain-transition-into-the-ethereum-economic-zone/12397)), Gnosis Chain becomes a rollup on Ethereum | Blocks go from 5 s to 2 s. Ethereum reorganisations of the chain are inherited. Chain ID, addresses and contract state are kept. | Genesis targeted for December 2026 or January 2027. Testnet date not announced. |
| **Any other EVM chain or rollup** (general readiness, no specific plan) | A different chain ID and contracts, a different block time (anywhere from under a second to 12 s), different gas costs and finality. | None. Listed so that the client does not depend on one chain's parameters. |

The goal is that wasp is ready for each of these without waiting for an upstream Bee release, and that as much as possible either adjusts itself or can be set by configuration.

## What is already done

These are merged on `main` and independently reviewed:

- [#541](https://github.com/crtahlin/wasp/issues/541): swap transactions (chequebook deployment, token transfer, chequebook withdrawal, cashout) estimate their gas, with today's fixed limits as minimums. A reverted chequebook deployment no longer stops the node from ever starting again. **Not yet measured after the fork.**
- [#540](https://github.com/crtahlin/wasp/issues/540): the stamp TTL in the API, the storage-incentives phase check and the postage listener use the **observed** average block time. An explicit `block-time` still overrides it. The node warns when the two differ for 10 minutes. Before committing, it checks `currentRound()` on the contract and skips the commit if the rounds differ by more than one.
- [#545](https://github.com/crtahlin/wasp/issues/545): the setting `postage-confirmation-depth`, from 1 to 64 blocks, default 4.

So for Glamsterdam and most of the EEZ change, the work is known and mostly done. What remains:
1. the measurement after the fork;
2. a few values that are still fixed;
3. the structure of a storage-incentives round. That is the one thing that is not configurable and that should not simply become a setting.

## Main finding: the round structure has to come from the contract

The client computes the round and phase itself, from compiled-in constants (`pkg/storageincentives/agent.go`):
- `DefaultBlocksPerRound = 152`;
- `DefaultBlocksPerPhase = 152 / 4 = 38`;
- commit for blocks 0 to 37 of a round, reveal for 38 to 75, claim for 76 to 151.

In the Swarm contracts (`go-storage-incentives-abi` v0.9.4), the round length is a constant **with no getter**, and the contracts cannot be upgraded. A different round length therefore means new contracts, and every client must then match them exactly. A setting on its own would mostly be a new way to configure a node wrongly.

The contract does expose what is needed to check the client:
- `currentRound()`;
- `currentPhaseCommit()`, `currentPhaseReveal()` and `currentPhaseClaim()`;
- `currentCommitRound()`, `currentRevealRound()` and `currentClaimRound()`.

The client calls only `currentRound()`, and only before a commit.

So the proposal is that the node **learns** the round structure from the contract and confirms it every round. A setting is there for a new deployment, and the node does not play when its view and the contract's disagree. Work item W3 covers this.

Block time is different: it already adjusts itself through the observed value. Only its unit and two places that still use the fixed value need to change (W2 and W5).

## Work items

### W1. Glamsterdam: measure and finish (urgent)

- **Measure #541 on Sepolia after the fork.** Use fresh accounts and the harness from the baseline run on 2026-09-28. Run five cases, each with v0.1.5 and with `main`, three times:
  - chequebook deployment;
  - first deposit;
  - withdrawal to an owner who holds no tokens;
  - first cashout;
  - a postage batch, against the 500,000 fallback limit.

  The results PR closes #541.
- **[#547](https://github.com/crtahlin/wasp/issues/547):** the balance check before a chequebook deployment asks for a fixed 250,000 gas (`pkg/settlement/swap/chequebook/init.go`). Base it on the gas estimate, with 250,000 as the minimum.
- **[#548](https://github.com/crtahlin/wasp/issues/548):** an initial chequebook deposit that reverts is never retried. Warn, and retry when a deposit is configured and the chequebook balance is 0.
- **New:** with `transaction-debug-mode`, every contract call gets a fixed limit of 1,000,000 gas (`pkg/node/node.go`). Use the estimate, with 1,000,000 as the minimum.
- **New:** the storage-incentives funds check assumes 15 transactions of 250,000 gas each (`minTxCountToCover`, `avgTxGas` in `pkg/storageincentives/agent.go`). Base it on the gas the node's own last commit, reveal and claim used, with today's value as the minimum.

None of this depends on upstream Bee.

### W2. Gnosis EEZ at 2 s blocks: close the remaining gaps

- **[#556](https://github.com/crtahlin/wasp/issues/556):** the observed block time jumps by about 11 per cent when one slot is missed, and the stamp TTL jumps with it. Smooth the value used for the TTL; keep the raw value for estimating block numbers.
- **New:** two places still use the fixed block time instead of the observed one (`pkg/node/chain.go`, `InitChain`):
  - the transaction monitor's polling interval;
  - the wrapped backend's starting estimate.
- **New:** the phase check runs every 5 blocks when a phase is longer than 10 blocks (`agent.go`, the `blocksPerPhase > 10` branch). A new phase can therefore be noticed up to 5 blocks late. At 2 s blocks the sampling window is about 228 s, and 10 s of it is lost this way. Check every block. The block number used there is an estimate from the wrapped backend, so this should not add RPC calls; the spec must confirm that.
- **[#545](https://github.com/crtahlin/wasp/issues/545):** decide the confirmation depth from a reorg measurement on the EEZ testnet, and check whether the chain serves the `safe` and `finalized` block tags (see W5).
- **[#552](https://github.com/crtahlin/wasp/issues/552), [#554](https://github.com/crtahlin/wasp/issues/554):** missing tests for the block-time watcher and the confirmation-depth wiring.

### W3. Learn the round structure from the contract

- At startup, read `currentRound()` together with the block number, and infer the number of blocks per round. Check the phase boundaries with `currentPhaseCommit/Reveal/Claim()` close to the predicted boundary.
- Repeat the check once per round.
- New setting `redistribution-blocks-per-round`. The default is the inferred value. An operator sets it explicitly only for a deployment of the contracts with a different round length. The phase split (one quarter, one quarter, one half) comes from the same source.
- When the inferred value, the setting and the contract disagree, the node does not play. It logs one clear error and counts the event in a metric. Today's check runs only before a commit; extend it to reveal and claim.
- The contract rejects a transaction in the last block of a phase (`PhaseLastBlock`); the client ignores this today. Do not send a transaction predicted to land in that block.
- Everything else derived from 152 follows the learned value:
  - the batch balance cut-off (`minBatchBalance`);
  - the stamp time cut-off for a sample (`getPreviousRoundTime`);
  - the timeout of the phase check.
- **Sample budget check.** Compute the sampling window in seconds: from the start of claim to the start of the next round's reveal, which is 114 blocks today, times the observed block time. Warn when the node's last measured sample time takes more than a set share of it. Operators then learn before a chain change that their node will miss rounds.

  **Table: sampling window for 152-block rounds, against measured sample times**

  | Chain | Window |
  |---|---|
  | Gnosis today, 5 s blocks | about 570 s |
  | Gnosis EEZ, 2 s blocks | about 228 s |
  | Ethereum, 12 s blocks | about 23 minutes |

  For comparison, a test node covering eight neighbourhoods (`reserve-capacity-doubling: 3`) took 145 to 192 s for its slowest neighbourhood in October 2026, while other nodes on the machine were syncing.

### W4. A chain profile: point a node at any chain by configuration

Today a node can reach a new chain with:
- a `network-id` other than 1, and `mainnet: false`;
- an RPC endpoint;
- six contract addresses;
- `postage-stamp-start-block`.

The chain ID then comes from the RPC endpoint. These are still fixed in code:

- **Contract ABIs.** An unknown chain silently gets the Sepolia ABIs (`pkg/config/chain.go`, `GetByChainID`). New contracts with a changed interface need a code change.
- **Accepted chequebook bytecode hashes.** These are empty for an unknown chain, so the check is skipped.
- **The embedded postage snapshot.** It is chosen by `network-id == 1` (`useEmbeddedSnapshot`), not by chain ID. On any chain other than Gnosis with network ID 1, new nodes would load Gnosis batch data unless `skip-postage-snapshot` is set.
- **Token symbols, the funding link and the retired staking contracts.** These are keyed to Gnosis and Sepolia.

Proposal:
- An optional **chain profile file** in YAML, loaded with `chain-profile: <path>`. It holds:
  - chain ID, contract addresses and start block;
  - ABI files and accepted bytecode hashes;
  - whether to use the snapshot;
  - token symbols;
  - the per-chain values from W5.
- The built-in Gnosis and Sepolia profiles stay the defaults, so nothing changes for current operators.
- Tie the embedded snapshot to the **chain ID**, and refuse it on any other chain.

### W5. Turn fixed block counts into time or chain tags

**Table: values counted in blocks today, and what happens on other chains**

| Value | Where | Problem | Proposal |
|---|---|---|---|
| `block-time` in whole seconds | `cmd/bee/cmd/cmd.go` | Cannot express sub-second block times | Accept a duration (`250ms`, `2s`); a plain number still means seconds |
| Postage confirmation depth in blocks | `pkg/postage/listener` | 4 blocks is about 1 s on a chain with 0.25 s blocks and 48 s at 12 s blocks | Option to follow the chain's `finalized` or `safe` block instead |
| Log page of 5,000 blocks | `pkg/postage/listener` | Providers limit `eth_getLogs` ranges; fast chains need more pages | Page size in the profile, and a smaller page after a failure ([#583](https://github.com/crtahlin/wasp/issues/583)) |
| Failover: an endpoint may lag 8 blocks | `pkg/transaction/failover` | 2 s at 0.25 s blocks, so endpoints would be dropped too often | Express the lag as time |
| Transaction cancellation depth of 12 blocks | `pkg/node/chain.go` | 3 s at 0.25 s blocks, 144 s at 12 s blocks | Express as time |
| Block time measured from whole-second header timestamps | `pkg/transaction/wrapped` | On sub-second chains, the difference between two anchors can be 0 or 1 s | Measure over a longer span when the difference is small |

### W6. Rollup-specific behaviour (record only)

- **Rollups where Solidity's `block.number` is the parent chain's block number**, not the rollup's own. The client would compute rounds, freeze checks and the stamp time cut-off from the wrong number. W3 fixes the round. Freeze checks would need the parent chain's block number, which such rollups report with each block. Some rollups also return a constant `prevrandao`, which affects contract randomness; that is a contract question, not a client one. Recorded only; nothing is planned to be built for it.
- **Rollups that charge a separate data fee.** The funds check does not include that fee. Recorded only.

### W7. Test chain changes without waiting for real chains

- **Local test chain.** anvil or geth in development mode with a chosen block time: 0.25 s, 2 s, 5 s and 12 s. The Swarm contracts are deployed from their sources with a round length of 152 and with a different value, and one wasp node runs against each. Checks:
  - how late a phase change is noticed;
  - whether the round structure is inferred correctly;
  - that the node refuses to play on a mismatch;
  - stamp TTL accuracy;
  - the sample budget warning.

  W3 needs this, because there is no real chain to test it on before a change.
- **Sepolia** is already a real Ethereum chain with 12 s blocks, and it has Glamsterdam from 2026-10-06.
- **The EEZ testnet** (`eez-rollup0` on Chiado), once Swarm contracts can be deployed there: the reorg measurement for #545.

## Not in wasp's hands

Listed so the picture is complete. These are contract or chain properties, not client changes:

- The contracts count in blocks. With 2 s blocks, a round of 152 blocks lasts about 5 minutes, and a price per block buys 2.5 times less wall-clock time. The price and `minimumValidityBlocks` are contract parameters; the client only reads them.
- Whatever round length the contracts use, W3 lets the client follow it.
- Open questions about the EEZ chain:
  - Do block numbers continue across the switch?
  - Where does `prevrandao` come from?
  - Will the chain serve `safe` and `finalized` blocks?
  - When will it adopt EIP-8037?

## Order

1. **W1:** the Sepolia fork is on 2026-10-06. The measurement runs after 13:53:36 UTC.
2. **W2 and W3:** before EEZ genesis. W3 also covers contracts with a different round length.
3. **W7:** alongside W3, which cannot be tested on a real chain before a change.
4. **W5 and W4:** general readiness for chains with other parameters.
5. **W6:** recorded only.

## Proposed issues

Existing issues are referenced, not duplicated: #541, #545, #547, #548, #552, #554, #556, #583.

**Table: new issues to file from this proposal**

| Work item | Title | Labels | Priority |
|---|---|---|---|
| W1 | transaction: use the gas estimate in transaction debug mode, with 1,000,000 as the minimum | fix | p2 |
| W1 | storageincentives: base the funds check on the gas the node's own transactions used | fix, area/incentives | p2 |
| W2 | node: use the observed block time for transaction monitor polling and the wrapped backend's starting estimate | fix | p1 |
| W2 | storageincentives: check for a phase change every block | fix, area/incentives | p1 |
| W3 | storageincentives: learn the round structure from the contract, with a `redistribution-blocks-per-round` setting for new deployments | fix, area/incentives, config | p1 |
| W3 | storageincentives: do not send a commit, reveal or claim into the last block of a phase | fix, area/incentives | p2 |
| W3 | storageincentives: warn when the last sample took too much of the sampling window | fix, area/incentives | p2 |
| W4 | config: chain profile file for contracts, ABIs, bytecode hashes and per-chain values | config | p2 |
| W4 | postage: tie the embedded snapshot to the chain ID | fix, area/postage | p1 |
| W5 | config: accept a duration for `block-time` | config | p2 |
| W5 | postage: option to follow the chain's `finalized` or `safe` block instead of a confirmation depth | fix, area/postage, config | p2 |
| W5 | transaction: express failover lag and cancellation depth as time | fix | p2 |
| W7 | test: local test chain with chosen block time and round length | chore | p1 |
| W6 | storageincentives: rollups whose `block.number` is the parent chain's (record only) | fix, area/incentives, icebox | p2 |

Each issue that turns out to concern unmodified upstream code gets `affects-upstream` once it is shown, not before.

Generated with help of AI.
