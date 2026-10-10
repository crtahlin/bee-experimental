# Storage lottery: do not lose a reveal to a stop

Issue: #725. Related: #634/#635 (clean stop, `cmd/bee/cmd/lifecycle.go` and the storer gate), #649 (lottery gates), #540 (2-second blocks).

## Decided (operator, 2026-10-10)

1. **`stop-wait-for-reveal` defaults to true.** A stop waits for a pending reveal, bounded by the end of the reveal phase.
2. **`TimeoutStopSec=300` in the packaged systemd unit** (`packaging/bee.service`, the only packaged unit file in the repository), so systemd does not kill the process during the longest wait at 5-second blocks. This changes every packaged install's stop timeout; it is recorded in `docs/DIFFERENCES.md`.

## Terms

- **Round:** 152 blocks. **Phases** (`pkg/storageincentives/agent.go:209-216`): commit = blocks 0-37 of the round, reveal = 38-75, claim = 76-151 (`blocksPerPhase` = 38).
- **Commit / reveal:** `handleCommit` (`agent.go:271`) sends the obfuscated sample hash with a fresh random key; `handleReveal` (`:329`) sends the sample hash and that key. Both go through `contract.sendAndWait` (`pkg/storageincentives/redistribution/redistribution.go:219`): send, then wait for the receipt.
- **Missed reveal:** a node that committed in a round and did not reveal in the same round's reveal phase. The contract freezes it for `penaltyMultiplierNonRevealed` (2) x 152 x 2^truth depth blocks: 155,648 blocks (about 9 days at 5 s blocks) at depth 9, twice a wrong reveal.
- **Pending reveal:** the node has a commit key for the current round and has not revealed (`RoundData.CommitKey` set, `HasRevealed` false), and the round's reveal phase has not ended.
- **Commit in flight:** `commit()` has stored the key and is sending the commit or waiting for its receipt (section 1).

## Problem

Chain data, last 30 days to 2026-10-10 (Gnosis, the storage-incentives contracts): 10,774 reveals in 3,393 rounds; 26 freezes for a wrong reveal (0.24 % of reveals) and 36 for a missed reveal (0.33 % of commits). Missed reveals are the larger share of freezes and each lasts twice as long. Why those 36 nodes missed is not known (hypothesis: restarts, crashes, chain endpoint failures).

What wasp does today (verified by reading `origin/main`, 55f72b44c):

1. **The key is stored only after the commit receipt.** `commit()` (`agent.go:610-633`) makes the key in memory, sends the commit and waits for its receipt, and only then calls `SetCommitKey`. A stop or crash between sending and storing loses the key; the commit can still be mined, and then the node can never reveal: a certain 9-day freeze. The receipt wait is typically one to a few blocks.
2. **Shutdown cuts the reveal's dependencies before the agent.** In `Bee.Shutdown` (`pkg/node/node.go:1917-2025`):
   - `b.ctxCancel()` (`:1987`) cancels the node context first; it stops chain-endpoint failover recovery and the block-time watcher (`watchBlockTime`, `:757`), both of which run on it.
   - The transaction monitor is closed at `:1996`; a receipt wait then fails with `ErrMonitorClosed`.
   - The chain client is closed at `:2011` (`ethClientCloser`).
   - Only then the agent (`:2017`) and the localstore (`:2020`).
   - `:1997` closes `b.transactionCloser`, which is set to the tracer closer (`:758`), so the transaction service itself is never closed there and the tracer is closed twice (again at `:2015`). Upstream has the same defect.
   The agent keeps running during shutdown, but once the monitor and the chain client are closed it cannot send a reveal or see its receipt.
3. **A stop just after a commit always misses the reveal.** Nothing waits for the reveal phase. The worst case is a commit at block 0-2 and a stop right after: the reveal phase opens about 36 blocks later (about 180 s at 5 s blocks, about 72 s after #540), plus the reveal's own receipt wait.
4. **After a restart the reveal can still be sent, but only if the node is back in time.** The key and sample are persisted with the redistribution state in the state store (`redistributionstate.go:113-118`, `SetCommitKey` `:271-280`). On start the agent runs `phaseCheck` at once (`agent.go:253`) and, if the round is in its reveal phase, `handleReveal` uses the stored key; `handleReveal` checks no sync or health gate. But the agent is created late in the build (`node.go` around `:1711`), after the chain sync wait (`:850`), the store open and the postage catch-up (`batchSvc.Start`, `:1305`). A restart longer than the remaining reveal phase (at most about 190 s, about 76 s after #540) misses it.
5. **systemd stops the process after 90 s.** `packaging/bee.service` sets no `TimeoutStopSec`, so systemd's default applies, then SIGKILL. systemd sends one SIGTERM, so the lifecycle's "second signal exits" (`lifecycle.go:129-133`) helps only at a terminal.

6. **A late commit can lose its key with no stop at all.** The reveal phase cancels the commit handler's context (`agent.go:157`, `phaseEvents.Cancel(commit, sample)`). A commit sent near the end of the commit phase whose receipt has not arrived returns an error (`:179-183`); the key, kept only in memory, is dropped, and the mined commit can never be revealed.

The same code is on upstream `master` (`agent.go:556` stores the key after the receipt; `node.go:1573-1593` closes the monitor and the chain client before the agent), so this likely affects upstream; the label waits for a reproduction test on the upstream tree.

## Design

### 0. One state, one reveal (needed by everything below)

- **One `RedistributionState` per node.** Today the agent builds its own (`agent.go:115`). Two instances would each write the whole status blob (`redistributionstate.go:113-118`) and overwrite each other: for example `SetCurrentBlock` (`agent.go:203`) would erase another instance's `HasRevealed`, and `handleClaim` would then skip a won claim (`:360-363`). The node builds the state once, early (section 3), and injects it into the agent (`storageincentives.New` takes it as a parameter).
- **One reveal at a time.** A per-round single-flight `revealOnce(round)` on the shared state, taken by `handleReveal`, the stop wait and the early-reveal path. Under it, `HasRevealed(round)` is re-checked first, and a reveal whose transaction is still pending is not sent again (section 1, pending store check).

### 1. The commit key: stored before the send, with an explicit state

Per round, `RoundData` gains `CommitTx` (the commit transaction hash, empty until known) next to `CommitKey`.

1. `commit()` stores the key with `CommitTx` empty **before** `contract.Commit`. The store write must succeed: `SetCommitKey` returns its error instead of only logging it (today `save()` logs, `redistributionstate.go:113-118`), and on error the commit is not sent. The write is synced to disk (a synced `Put`, or a sync of the state store after it), so a power loss after the send cannot lose it.
2. After `Send` returns a hash, `CommitTx` is stored.
3. **Errors:**
   - `Send` failed **before broadcast** (signing, gas estimation, nonce, balance, the `SendTransaction` RPC returning a definite rejection): the key is removed; nothing was broadcast.
   - `Send` failed in a way that may have broadcast (a timeout or a lost connection on `SendTransaction`; `sendAndWait` returns a zero hash for every `Send` error, `redistribution.go:229-231`): the key is **kept** with `CommitTx` empty.
   - The commit was mined and **reverted** (`:238`): the key is removed and the round is not played.
   - The receipt wait failed (context cancelled at the reveal phase, monitor closed): the key and `CommitTx` are kept.
4. **The transaction service records a transaction as pending only after `SendTransaction` returns** (`pkg/transaction/transaction.go:200-224`). So a key with an empty `CommitTx` cannot be checked against the pending store; it is treated as "possibly committed": the reveal is attempted, and if the commit never landed the contract reverts the reveal (gas only, no freeze). A key with a `CommitTx` is checked against its receipt after a restart: reverted or not found after the reveal phase starts means no reveal.
5. `handleCommit`'s `exists` check (`agent.go:277`) now also sees a key stored before the send, so a second commit in the same round is never sent, also after a restart.

### 2. A stop waits for a pending reveal

**Interface.** The agent exposes `RevealGuard`: `Pending() (round, deadlineBlock uint64, ok bool)` and `Wait(ctx) error`. `Bee.Shutdown` gets a context (section 2b) and calls `Wait` at the point below. Nothing else in shutdown depends on the agent.

**Order in `Bee.Shutdown`** (pulled out into a function that takes the steps as values, so the order is testable, like `shutdownClosers()`):

1. **Refuse new commits** (`commitLock` taken; `shuttingDown` set). A commit already sending is allowed to finish its send and store `CommitTx`; no new commit starts.
2. API, then the closers that run today in parallel, then p2p and the price oracle, as today.
3. **The localstore, closed concurrently with the wait**, not after it: the reveal and the phase checks read only the state store and values in memory (verified: `reserve.go:703`, `held.go:255`, `salud.go:257`), so the clean stop of #710 runs alongside.
4. **The wait**, if `Pending` is ok. Kept alive until it ends: the agent goroutine, the transaction service and its monitor, the chain client, the wallet signer, the state store, and a **wait context** derived from the shutdown context, not from `b.ctx`. Because `ctxCancel` (`:1987`) also stops chain-endpoint failover recovery and the block-time watcher, `ctxCancel` moves after the wait; the other closers already cancel their own work.
5. Then, as today: transaction monitor, chain client, listener and postage service, the rest, the state store last.

**The wait.**
- Polls the block number every block (not every 5 blocks as the agent's loop does, `agent.go:256-260`).
- Sends the reveal through `revealOnce` as soon as the reveal phase opens. **A failed reveal is retried** every block while the phase lasts (today `handleReveal` runs once per phase entry, `:156-160`, so a single failure would leave the wait idle).
- **Bound:** the agent's own claim-phase cancel (`agent.go:163`), which ends `WaitForReceipt` (it waits on its context only, `transaction.go:415-430`). So the wait ends at the receipt or at the start of the claim phase, whichever comes first; there is no separate receipt timeout.
- A Warning when the wait starts ("stop waits for the storage-lottery reveal of round R; up to block B"), and an Info line when it ends (revealed, or the reveal was missed).

**2b. The second signal.** `lifecycle.stop` returns on the second interrupt (`lifecycle.go:130-132`) while `Shutdown` still runs, so today the state store is never closed. The second signal now cancels the shutdown context: the wait returns at once, and `Shutdown` continues to close everything, the state store included, before the process exits.

**Windows service and containers.**
- The Windows service manager has its own stop timeout (to check on a Windows build); there the wait is best effort.
- Docker sends SIGKILL 10 s after SIGTERM by default: `packaging/docker/docker-compose.yml` gets `stop_grace_period: 5m`. Kubernetes' default is 30 s (`terminationGracePeriodSeconds`); documented, not packaged here.

### 3. Reveal early after a restart

- Built right after the chain client, transaction service and state store exist (around `node.go:729-760`). It needs the redistribution contract's address and ABI, which move up from where they are read today (`chainCfg`, around `:860`).
- **Check that the chain backend is synced** (`transaction.IsSynced`, as `:842-857` does) before trusting the block number for the phase; if not synced, wait for it within the reveal phase.
- Runs in its own goroutine with its own context, cancelled by `Shutdown`, and is **registered in `b` before any later error return** in the build, so the deferred `Shutdown` on a build error (`node.go:566`) still waits for it or cancels it.
- Uses the shared state (section 0) and `revealOnce`; the agent, when it starts, sees `HasRevealed` and does not reveal again.
- Never commits.

### 4. Expose and alert

- `/redistributionstate` gains `revealPending` (bool) and `safeToRestartAfterBlock` (uint64, 0 when nothing is pending). Additive JSON; the schema is in `openapi/SwarmCommon.yaml` (around `:1068`), and the API version goes from 8.2.2 to 8.3.0.
- **Counts persisted in the state store**, because `/metrics` is served by the API server, which shuts down first (`router.go:128`, `node.go:1957-1972`): `stopWaitedForReveal`, `revealMissedOnStop`, `revealAfterRestart`, exposed after the next start as gauges and in `/redistributionstate`. The log lines of section 2 carry the same information for each stop.

### Settings

- **Wait on stop:** on by default (decided), setting `stop-wait-for-reveal` (true; false = today's behaviour except sections 0, 1 and 3, which have no setting).
- **Stop budget:** at 5 s blocks, a stop just after an early commit waits about 180 s for the reveal phase plus the reveal's receipt, plus up to 15 s of API shutdown. That fits the decided `TimeoutStopSec=300`. After #540 (2 s blocks) it is about 72 s plus the same, still under 300 s.
- **Accepted costs:**
  - A SIGKILL during the reveal's receipt wait leads to a second reveal after the restart, because `SetHasRevealed` comes after the receipt (`agent.go:354`). Section 0's pending-store check catches it when the reveal's hash was recorded; otherwise it costs gas only (the contract rejects a second reveal).
  - A commit stuck in the mempool (low gas) queues the reveal behind it on the same nonce sequence; the wait may then end at the claim phase without a reveal. Accepted; the freeze is the same as today.

No wire change: the contract calls are the same; only their timing and persistence change.

## Tests

1. **Key stored before the send** (mock contract that blocks after "broadcast"): stop during the block, restart from the same state store; the key is present; a reveal is sent in the reveal phase. On unmodified code the key is absent (reproduction).
2. **Late commit, no stop:** the reveal phase cancels a commit whose receipt has not arrived; the key is kept and the reveal is sent. On unmodified code the key is lost (reproduction).
3. **Key write fails:** the commit is not sent.
4. **Pre-broadcast error removes the key; a possibly-broadcast error keeps it; a reverted commit removes it.**
5. **No second commit** in a round with a stored key, also after a restart.
6. **Shutdown order** (on the extracted ordering function): new commits refused first; the localstore closes concurrently with the wait; the monitor, chain client and `ctxCancel` come after the wait; the state store last. On unmodified code the monitor and chain client close before any wait (reproduction).
7. **The wait:** commit at block 2, stop at block 3; the reveal is sent at block 38; a reveal failing once is retried at the next block; the wait ends at the claim phase with `revealMissedOnStop` persisted as 1.
8. **No pending reveal, no wait.**
9. **Second signal:** the wait returns at once and the state store is closed (asserted).
10. **Shutdown during a commit's send:** the send completes and its `CommitTx` is stored; no new commit starts.
11. **One reveal:** the early path and the agent race on the same round; exactly one reveal is sent and `HasRevealed` survives the agent's `SetCurrentBlock`.
12. **Early reveal:** backend not synced at first, then synced; the reveal is sent before the postage catch-up; a build error after it is registered does not leak the goroutine.
13. **API:** the two fields and the persisted counts; old fields unchanged.
14. **Setting off:** no wait; sections 0, 1 and 3 still apply.

Mutation checks (each must fail a test): key stored after the receipt again; key removed on a possibly-broadcast error; key-write error ignored; `exists` ignored after a restart; wait removed; monitor or chain client closed before the wait; `ctxCancel` before the wait; localstore closed after the wait; reveal not retried; second signal not reaching `Shutdown`; commits not refused after shutdown starts; `revealOnce` not re-checking `HasRevealed`; two state instances; early reveal without the sync check; `revealPending` always false.

## Measurement

- **No staked bench node exists.** Plan: a Sepolia testnet node (funded test wallet, recorded privately), staked at the testnet minimum, with scripted restarts in the commit phase after a commit, at the start of the reveal phase, and in the claim phase.
  - Sepolia has 12 s blocks, so a wait can take about 450 s plus the receipt: above 300 s. Run the test node outside systemd, or with `TimeoutStopSec=600` on that node only.
  - Pass: no `StakeFrozen` event for the node; stop durations from the log lines; the persisted `stopWaitedForReveal` matches the restarts after a commit.
  - Whether the testnet selects the node often enough in a week is not known; if not, the tests above are the evidence and the decision to stake on mainnet carries the canary.
- **Chain baseline:** 0.33 % of commits end in a missed reveal across the network (30 days to 2026-10-10).

## Rollout

Implementation after this spec merges, in one PR or in this order: sections 0 and 1 (no behaviour visible to operators except a reveal that would otherwise be lost), then 2 with the packaging changes (`TimeoutStopSec=300`, `stop_grace_period: 5m`), then 3 and 4. The tracer double close and the never-closed transaction service (Problem 2) are fixed in the same change, since the shutdown order is rewritten anyway. Record in `docs/DIFFERENCES.md` (behaviour, setting, API fields, persisted counts, packaging). `affects-upstream` only after the reproduction tests run on the upstream tree.

Generated with help of AI.
