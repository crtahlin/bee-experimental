# Storage lottery: do not lose a reveal to a stop

Issue: #725. Related: #634/#635 (clean stop, `cmd/bee/cmd/lifecycle.go` and the storer gate), #649 (lottery gates), #540 (2-second blocks).

## Terms

- **Round:** 152 blocks. **Phases** (`pkg/storageincentives/agent.go:209-216`): commit = blocks 0-37 of the round, reveal = 38-75, claim = 76-151 (`blocksPerPhase` = 38).
- **Commit / reveal:** `handleCommit` (`agent.go:271`) sends the obfuscated sample hash with a fresh random key; `handleReveal` (`:329`) sends the sample hash and that key. Both go through `contract.sendAndWait` (`pkg/storageincentives/redistribution/redistribution.go:219`): send, then wait for the receipt.
- **Missed reveal:** a node that committed in a round and did not reveal in the same round's reveal phase. The contract freezes it for `penaltyMultiplierNonRevealed` (2) x 152 x 2^truth depth blocks: 155,648 blocks (about 9 days at 5 s blocks) at depth 9, twice a wrong reveal.
- **Pending reveal:** the node has a commit key for the current round and has not revealed (`RoundData.CommitKey` set, `HasRevealed` false), and the round's reveal phase has not ended.
- **Commit in flight:** `commit()` has sent, or is about to send, the commit transaction and has not yet stored the key.

## Problem

Chain data, last 30 days to 2026-10-10 (Gnosis, the storage-incentives contracts): 10,774 reveals in 3,393 rounds; 26 freezes for a wrong reveal (0.24 % of reveals) and 36 for a missed reveal (0.33 % of commits). Missed reveals are the larger share of freezes and each lasts twice as long. Why those 36 nodes missed is not known (hypothesis: restarts, crashes, chain endpoint failures).

What wasp does today (verified by reading `origin/main`, 55f72b44c):

1. **The key is stored only after the commit receipt.** `commit()` (`agent.go:610-633`) makes the key in memory, sends the commit and waits for its receipt, and only then calls `SetCommitKey`. A stop or crash between sending and storing loses the key; the commit can still be mined, and then the node can never reveal: a certain 9-day freeze. The receipt wait is typically one to a few blocks.
2. **Shutdown closes the chain before the agent.** `Bee.Shutdown` (`pkg/node/node.go:1917-2025`) closes the transaction monitor and transaction service (`:1996-1997`) and the chain client (`ethClientCloser`, `:2011`) before it closes the agent (`:2017`); the localstore closes after the agent (`:2020`). The agent keeps running during shutdown but cannot send a reveal once those are closed.
3. **A stop just after a commit always misses the reveal.** Nothing waits for the reveal phase. The worst case is a commit at block 0-2 and a stop right after: the reveal phase opens about 36 blocks later (about 180 s at 5 s blocks, about 72 s after #540), plus the reveal's own receipt wait.
4. **After a restart the reveal can still be sent, but only if the node is back in time.** The key and sample are persisted with the redistribution state in the state store (`redistributionstate.go:113-118`, `SetCommitKey` `:271-280`). On start the agent runs `phaseCheck` at once (`agent.go:253`) and, if the round is in its reveal phase, `handleReveal` uses the stored key; `handleReveal` checks no sync or health gate. But the agent is created late in the build (`node.go` around `:1711`), after the chain sync wait (`:850`), the store open and the postage catch-up (`batchSvc.Start`, `:1305`). A restart longer than the remaining reveal phase (at most about 190 s, about 76 s after #540) misses it.
5. **systemd stops the process after 90 s.** `packaging/bee.service` sets no `TimeoutStopSec`, so systemd's default applies, then SIGKILL. systemd sends one SIGTERM, so the lifecycle's "second signal exits" (`lifecycle.go:129-133`) helps only at a terminal.

The same code is on upstream `master` (`agent.go:556` stores the key after the receipt; `node.go:1573-1593` closes the transaction service and chain client before the agent), so this likely affects upstream; the label waits for a reproduction test on the upstream tree.

## Design

### 1. Store the key before sending the commit (fixes Problem 1)

`commit()` stores the key, marked "commit sent: unknown", before `contract.Commit`, and marks it confirmed after the receipt. On a send error before the transaction is broadcast, the key is removed. After a restart, a key whose commit is "unknown" is treated as pending: the reveal is attempted; if the commit was not mined, the contract reverts the reveal (gas spent, no freeze). Compared with today this trades a possible wasted reveal transaction for a certain freeze.

### 2. A stop waits for a pending reveal (fixes Problems 2 and 3)

On `Bee.Shutdown`, before closing the transaction service and the chain client:

- If there is a commit in flight or a pending reveal, log a Warning once ("stop waits for the storage-lottery reveal of round R, up to block B") and wait until the reveal receipt arrives, the reveal phase ends, or a second signal arrives. Everything that the reveal does not need is shut down first and in the usual order: API, p2p, pull-sync, puller, the storer (the reveal reads only the state store, never the storer, so the localstore close moves ahead of the wait and the #634 gate closes the reserve cleanly before it; today it closes last, `node.go:2020`).
- Kept open during the wait: the agent, the transaction service and monitor, the chain client, the wallet signer, and the state store (closed after the wait, as today).
- Bound: the end of the reveal phase (block 75 of the round) plus one receipt timeout. If the bound passes, stop as today and log that the reveal was missed.
- No pending reveal: no wait, no change.
- Windows service: the service manager's stop request goes through the same `stop()` path; the service manager has its own stop timeout, likely shorter than the worst case (to check on a Windows build), so on Windows the wait is best effort. Stated in `DIFFERENCES.md`.

### 3. Reveal early after a restart (narrows Problem 4)

Right after the chain client, transaction service and state store exist in the build (around `node.go:729-760`, before the chain sync wait, the store open and the postage catch-up), check the persisted redistribution state. If the current round has a pending reveal, start a one-off reveal goroutine that waits for the reveal phase (polling the block number) and sends the reveal; the agent, when it starts later, sees `HasRevealed` and skips. The commit is never attempted from this path. Guard against a double reveal with the same commit lock the agent uses.

### 4. Expose and alert (helps operators and scripts)

- `/redistributionstate` gains two fields, additive JSON (existing clients ignore unknown fields; `openapi/Swarm.yaml` updated, a minor API version bump per its SemVer): `revealPending` (bool) and `safeToRestartAfterBlock` (uint64, 0 when nothing is pending).
- Metrics: `bee_storageincentives_stop_waited_for_reveal_total`, `bee_storageincentives_reveal_missed_on_stop_total`, `bee_storageincentives_reveal_after_restart_total`.
- Log: the Warning in section 2; an Info line when the early reveal of section 3 is sent.

### Settings and decisions for the operator

- **Wait on stop (section 2):** proposed on by default, as a setting `stop-wait-for-reveal` (true; false = today). Cost: a stop can take up to about 3.5 minutes at 5 s blocks (about 1.5 minutes after #540), only when a reveal is pending, which a node at depth 9 has in about 1 of 512 rounds per neighbourhood it holds (at doubling 3, about 8 in 512).
- **systemd `TimeoutStopSec` in `packaging/bee.service`:** 90 s today. With the wait on, the worst case at 5 s blocks exceeds it and systemd would SIGKILL during the wait (the store is already closed cleanly by then, so only the reveal is lost, as today). Options: raise it to 300 s in the packaged unit (operator decision; affects every packaged install), or leave 90 s and accept that the wait completes only when the reveal phase opens within about 80 s of the stop. After #540 the worst case fits in 90 s.
- **Sections 1, 3 and 4** are fixes without a setting.

No wire change: the contract calls are the same; only their timing and persistence change.

## Tests

1. **Key stored before send** (`agent_test.go`, mock contract that blocks in `Commit` after "broadcast"): stop during the block; restart from the same state store; the key is present and marked unknown; in the reveal phase a reveal is sent. On unmodified code the key is absent (reproduction).
2. **Stop waits for a pending reveal** (node-level test with the mock contract and a mock block source): commit at block 2, stop at block 3; shutdown does not close the transaction service until the reveal at block 38 is sent; the storer is closed before the wait. On unmodified code the transaction service closes first and no reveal is sent (reproduction).
3. **No pending reveal, no wait:** stop in the claim phase returns at once.
4. **Bound:** the mock block source jumps past block 75; the wait ends, `reveal_missed_on_stop_total` is 1.
5. **Second signal ends the wait** (lifecycle test, extending the #710 tests).
6. **Early reveal after restart:** persisted pending reveal, build paused before the postage catch-up; the reveal is sent; the agent later skips it (no double reveal).
7. **API:** `/redistributionstate` reports `revealPending` and `safeToRestartAfterBlock`; old fields unchanged.
8. **Setting off:** today's behaviour.

Mutation checks (each must fail a test): key stored after the receipt again; wait removed; transaction service closed before the wait; storer closed after the wait; bound removed (test hangs, caught by the test's deadline); early-reveal path not guarded by the commit lock (double reveal); `revealPending` always false.

## Measurement

- **No staked bench node exists.** Realistic plan: a Sepolia testnet node (the funded test wallet and client-owned batch are recorded privately), staked at the testnet minimum, run for a week with scripted restarts: one in the commit phase after a commit, one at the start of the reveal phase, one in the claim phase. Pass: no freeze event for the node (`StakeFrozen` logs), the stop durations logged, `stop_waited_for_reveal_total` matches the restarts after a commit. Whether the testnet's selection rate gives enough commits in a week is not known; if not, the mock-contract node tests are the evidence and mainnet staking (separate decision) carries the canary.
- **Chain baseline to compare against later:** 0.33 % of commits end in a missed reveal across the network (30 days to 2026-10-10).

## Rollout

Implementation after this spec merges; sections 1 and 3 can ship first (no behaviour visible to operators except a reveal that would otherwise be lost). Record in `docs/DIFFERENCES.md` (behaviour, setting, API fields, metrics). `affects-upstream` only after the reproduction tests run on the upstream tree.

Generated with help of AI.
