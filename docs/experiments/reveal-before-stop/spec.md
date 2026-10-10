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
   - The node context is already cancelled when `Shutdown` runs: the first signal cancels it in `lifecycle.stop` (`l.cancel()`). That stops chain-endpoint failover recovery and the block-time watcher (`watchBlockTime`, `:757`); for a wait of a few minutes this is harmless (the chain client keeps its current endpoint, and the block time is the last value read), so `b.ctxCancel()` (`:1987`) is left where it is.
   - The transaction monitor is closed at `:1996`; a receipt wait then fails with `ErrMonitorClosed`.
   - The chain client is closed at `:2011` (`ethClientCloser`).
   - Only then the agent (`:2017`) and the localstore (`:2020`).
   - `:1997` closes `b.transactionCloser`, which is set to the tracer closer (`:758`), so the transaction service itself is never closed there and the tracer is closed twice (again at `:2015`). Upstream has the same defect.
   The agent keeps running during shutdown, but once the monitor and the chain client are closed it cannot send a reveal or see its receipt.
3. **A stop just after a commit always misses the reveal.** Nothing waits for the reveal phase. The worst case is a commit at block 0-2 and a stop right after: the reveal phase opens about 36 blocks later (about 180 s at 5 s blocks, about 72 s after #540), plus the reveal's own receipt wait.
4. **After a restart the reveal can still be sent, but only if the node is back in time.** The key and sample are persisted with the redistribution state in the state store (`redistributionstate.go:113-118`, `SetCommitKey` `:271-280`). On start the agent runs `phaseCheck` at once (`agent.go:253`) and, if the round is in its reveal phase, `handleReveal` uses the stored key; `handleReveal` checks no sync or health gate. But the agent is created late in the build (`node.go` around `:1711`), after the chain sync wait (`:850`), the store open and the postage catch-up (`batchSvc.Start`, `:1305`). A restart longer than the remaining reveal phase (at most about 190 s, about 76 s after #540) misses it.
5. **systemd stops the process after 90 s.** `packaging/bee.service` sets no `TimeoutStopSec`, so systemd's default applies, then SIGKILL. systemd sends one SIGTERM, so the lifecycle's "second signal exits" (`lifecycle.go:129-133`) helps only at a terminal.

6. **A late commit can lose its key with no stop at all.** The reveal phase cancels the commit handler's context (`agent.go:157`, `phaseEvents.Cancel(commit, sample)` in the reveal handler). A commit sent near the end of the commit phase whose receipt has not arrived returns an error (`:179-183`); the key, kept only in memory, is dropped, and the mined commit can never be revealed.

The same code is on upstream `master` (`agent.go:556` stores the key after the receipt; `node.go:1573-1593` closes the monitor and the chain client before the agent), so this likely affects upstream; the label waits for a reproduction test on the upstream tree.

## Design

### 0. One state, one reveal (needed by everything below)

- **One `RedistributionState` per node.** Today the agent builds its own (`agent.go:115`). Two instances would each write the whole status blob (`redistributionstate.go:113-118`) and overwrite each other: for example `SetCurrentBlock` (`agent.go:203`) would erase another instance's `HasRevealed`, and `handleClaim` would then skip a won claim (`:360-363`). The node builds the state once, early (section 3), and injects it into the agent (`storageincentives.New` takes it as a parameter).
- **One revealer.** A `Revealer` object built with the shared state owns every reveal: `handleReveal`, the stop wait and the early-reveal path all call `Revealer.Reveal(ctx, round)`, a per-round single-flight. Under it, `HasRevealed(round)` is re-checked first, then `RevealTxs` (section 1): a reveal that may have been broadcast is not sent again while it can still be mined.
- **`RevealGuard` lives on the `Revealer`, not on the agent,** so it exists from the moment the shared state is built (section 3), before the agent; `Shutdown` uses it whether or not the agent was ever created.

### 1. The commit key: stored before the send, with an explicit state

Per round, `RoundData` gains `CommitTx` (the commit's signed hash) and `RevealTxs` (every reveal hash sent; empty until known) next to `CommitKey` and `HasRevealed`.

1. `commit()` stores the key, with `CommitTx` empty, **before** `contract.Commit`. The store write must succeed: `SetCommitKey` returns its error instead of only logging it (today `save()` logs, `redistributionstate.go:113-118`), and on error the commit is not sent. The write is synced to disk, so a power loss after the send cannot lose it. Plumbing: `storage.StateStorer` gains `PutSync(key, value)`, implemented in every layer of the node's state store and its tests: along the node's path `pkg/statestore/storeadapter` (passes it to the `storage.IndexStore` below) -> `pkg/storage/cache` (`cache.Wrap`, writes through) -> `pkg/storage/leveldbstore` (`WriteOptions{Sync: true}`), plus `pkg/statestore/leveldb` and `pkg/statestore/mock` (a plain `Put`). `storage.IndexStore` therefore gains `PutSync` too. A layer without it is a compile error, not a silent unsynced write.
2. `CommitTx` is stored as soon as the signed hash is known, before `SendTransaction` (see the next item).
3. **Errors:**
   - **The signed transaction is known before it is sent.** The transaction service computes the signed transaction's hash before `SendTransaction` (`pkg/transaction/transaction.go:193-200`) and, with this change, stores the signed transaction under that hash first (as it does today only after a successful send, `:207`), so the commit's hash is known whatever `SendTransaction` returns.
   - **`ErrNotBroadcast` only for errors raised before `SendTransaction` is called** (signing, gas estimation, nonce, balance). No error text is parsed and no lookup is made at send time: a "not found" right after a send error is not definitive, since the transaction can still reach the network through the endpoint.
   - **Any `SendTransaction` error keeps the key and `CommitTx`** (the signed hash). The transaction service also writes the pending-transaction key for such an ambiguous send, so `nextNonce` counts it and a later transaction does not reuse its nonce. Whether to reveal is then decided by point 5, after the reveal phase starts. Keeping the key is the safe default: at worst a reveal is sent for a commit that never landed and the contract reverts it (gas only).
   - `ErrNotBroadcast` removes the key and clears the round's "commit in progress" mark (section 2), so `Pending()` no longer reports it.
   - The commit was mined and **reverted** (`:238`): the key is removed and the round is not played.
   - The receipt wait failed (context cancelled at the reveal phase, monitor closed): the key and `CommitTx` are kept.
4. **A key with an empty `CommitTx`** can exist only for a crash between the key write and the signing (nothing was broadcast then, but the node cannot know it after a restart). It is treated as "possibly committed": the reveal is attempted, and if the commit never landed the contract reverts it (gas only, no freeze). The transaction service's pending store is written only after `SendTransaction` returns (`pkg/transaction/transaction.go:200-224`), so the signed hash stored in item 3 is what makes every other case checkable.
5. **After a restart, a key with a `CommitTx` is checked against its receipt, and the reveal is skipped only on a definite answer:** a mined receipt with status reverted, or "not found" from a backend that `transaction.IsSynced` reports synced, after the reveal phase has started. Any lookup error, or an unsynced backend, means "reveal": a wasted reveal costs gas, a skipped one costs a freeze.
6. **The reveal transactions:** `RevealTxs` is the list of every reveal transaction hash sent for the round, appended before each send (synced write, the signed hash as for the commit). `HasRevealed` is set when **any** of them has a successful receipt. During the reveal phase, before anything is re-sent, the receipts of all listed hashes are checked; then, for the newest hash:
   - **receipt successful:** done;
   - **not found on a synced backend:** rebroadcast the **exact stored signed transaction** (the same nonce, the stored `GasFeeCap`/`GasTipCap`, so the same hash) through a new `RebroadcastTransaction(ctx, hash)` on the transaction service. Today's `ResendTransaction` (`transaction.go:491`) re-signs with new fees, refuses when the hash changes, and swallows send errors, so it is not used here. If the fee has to be raised instead (the stored one is too low to be mined), a **same-nonce replacement** is signed, its hash appended to `RevealTxs` with a synced write **before** it is sent, and then sent. Send errors are returned in both cases and the block loop tries again;
   - **reported cancelled** by the transaction service (its nonce consumed by another transaction): a fresh send with a new nonce, appended to the list;
   - **pending, or a lookup error, or an unsynced backend:** wait for the next block.
   A reveal is never sent while an earlier listed hash may still be mined; an older hash mined after a re-send still sets `HasRevealed` and the newer one, if also mined, reverts in the contract (gas only).
7. `handleCommit`'s `exists` check (`agent.go:277`) now also sees a key stored before the send, so a second commit in the same round is never sent, also after a restart.

### 2. A stop waits for a pending reveal

**Interface.** The `Revealer` (section 0) exposes `RevealGuard`: `Pending() (round, deadlineBlock uint64, ok bool)` and `Wait(ctx) error`. `Bee.Shutdown` changes to `Shutdown(ctx context.Context) error`; `runningNode` and its callers follow, and the deferred `Shutdown` on a build error (`node.go:566`) passes a context with a bound (the reveal-phase deadline, if a reveal is pending). Nothing else in shutdown depends on the agent.

**Order in `Bee.Shutdown`** (pulled out into a function that takes the steps as values, so the order is testable, like `shutdownClosers()`):

1. **Refuse new commits:** `shuttingDown` is set under a small mutex, `commitGate`, owned by the shared state's `Revealer` (section 0), so it exists before the agent and both `handleCommit` and `Pending()` reach it. `handleCommit` takes `commitGate`, checks `shuttingDown`, and if it is clear stores the key and marks the round "commit in progress" before releasing it; `Pending()` takes the same mutex. So a commit is either refused or already visible to `Pending()` when `Shutdown` asks; there is no window in between. A commit past the check finishes its send and stores `CommitTx`. `Shutdown` takes no other agent lock.
2. API, then the closers that run today in parallel, then p2p and the price oracle, as today.
3. **The localstore, closed concurrently with the wait**, not after it: the reveal and the phase checks read only the state store and values in memory (verified: `reserve.go:703`, `held.go:255`, `salud.go:257`), so the clean stop of #710 runs alongside.
4. **The wait**, if `Pending` is ok. Kept alive until it ends: the `Revealer`, the transaction service and its monitor, the chain client, the wallet signer and the state store. The wait runs on the shutdown context, not on `b.ctx` (already cancelled, see Problem 2). **An early reveal in progress (section 3) is waited for, not cancelled:** it is the `Revealer`'s reveal, and `Wait` covers it.
5. Then, as today: transaction monitor, chain client, listener and postage service, the rest, the state store last.

**The wait.**
- Polls the block number every block (not every 5 blocks as the agent's loop does, `agent.go:256-260`).
- Sends the reveal through `Revealer.Reveal` as soon as the reveal phase opens. **A failed reveal is retried** every block while the phase lasts (today `handleReveal` runs once per phase entry, `:156-160`, so a single failure would leave the wait idle).
- **Bound:** the agent's own claim-phase cancel (`agent.go:163`), which ends `WaitForReceipt` (it waits on its context only, `transaction.go:415-430`). So the wait ends at the receipt or at the start of the claim phase, whichever comes first; there is no separate receipt timeout.
- A Warning when the wait starts ("stop waits for the storage-lottery reveal of round R; up to block B"), and an Info line when it ends (revealed, or the reveal was missed).

**2b. Signals.** `lifecycle.stop` returns on the second interrupt (`lifecycle.go:130-132`) while `Shutdown` still runs, so today the state store is never closed. New behaviour:
- **Second signal:** cancels the shutdown context. The wait returns at once and `Shutdown` continues to close everything, the state store included.
- **Third signal, or the remaining close running longer than 30 s after the second:** `stop` returns and the process exits at once, keeping #710's escape for a close that hangs (for example a commit stuck in a pebble write stall).
- **During the build:** a signal before the node is built already ends `stop`'s wait for the build (#710). An early reveal running then is in the `Revealer`; the build's deferred `Shutdown` waits for it with a context bounded by the reveal phase. A second signal during the build is accepted as "exit now": the early reveal may be lost, as today.

**Windows service and containers.**
- The Windows service manager has its own stop timeout (to check on a Windows build); there the wait is best effort.
- Docker sends SIGKILL 10 s after SIGTERM by default: `packaging/docker/docker-compose.yml` gets `stop_grace_period: 5m`. Kubernetes' default is 30 s (`terminationGracePeriodSeconds`); documented, not packaged here.

### 3. Reveal early after a restart

- Built right after the chain client, transaction service and state store exist (around `node.go:729-760`). It needs the redistribution contract's address and ABI, which move up from where they are read today (`chainCfg`, around `:860`).
- **Check that the chain backend is synced** (`transaction.IsSynced`, as `:842-857` does) before trusting the block number for the phase; if not synced, wait for it within the reveal phase.
- Runs in its own goroutine on its own context, as part of the `Revealer`, which is **registered in `b` before any later error return** in the build, so the deferred `Shutdown` on a build error (`node.go:566`) waits for it through `RevealGuard`.
- **While the backend is not synced,** the phase is estimated from the persisted last block and the wall-clock time it was seen (`Status.Block` and a new `Status.BlockSeenAt`, both written by `SetCurrentBlock`) plus the elapsed time divided by the last known block time; the path keeps polling `IsSynced` and gives up once that estimate is past the reveal phase. Only a synced block number is used to send.
- Uses the shared state (section 0) and `Revealer.Reveal`; the agent, when it starts, sees `HasRevealed` and does not reveal again.
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
6. **Shutdown order** (on the extracted ordering function): new commits refused first; the localstore closes concurrently with the wait; the monitor and chain client come after the wait; the state store last. On unmodified code the monitor and chain client close before any wait (reproduction).
7. **The wait:** commit at block 2, stop at block 3; the reveal is sent at block 38; a reveal failing once is retried at the next block; the wait ends at the claim phase with `revealMissedOnStop` persisted as 1.
8. **No pending reveal, no wait.**
9. **Signals:** the second signal ends the wait and the state store is closed (asserted); a third signal, or a hung close 30 s after the second, exits at once.
10. **Shutdown during a commit's send:** the send completes and its `CommitTx` is stored; no new commit starts.
11. **One reveal:** the early path and the agent race on the same round; exactly one reveal is sent and `HasRevealed` survives the agent's `SetCurrentBlock`. A stop during the early reveal waits for it.
12. **Early reveal:** backend not synced at first, then synced; the reveal is sent before the postage catch-up; a build error after it is registered does not leak the goroutine.
13. **API:** the two fields and the persisted counts; old fields unchanged.
14. **Setting off:** no wait; sections 0, 1 and 3 still apply.
15. **Reveal not re-sent while pending:** a listed reveal hash still pending; after a restart the `Revealer` waits on it and sends nothing.
16. **Skip only on a definite answer:** reverted commit receipt, and not-found from a synced backend, skip; a lookup error and an unsynced backend reveal.
17. **Not broadcast only before the send:** an error before `SendTransaction` removes the key; any `SendTransaction` error keeps the key and `CommitTx` and writes the pending-transaction key (the next transaction gets the following nonce); point 5 decides at the reveal phase. No error text is parsed.
18. **No window:** `Shutdown` racing `handleCommit`; the commit is either refused or reported by `Pending()`.
19. **Same-nonce rebroadcast:** the newest reveal hash is not found on a synced backend; the rebroadcast transaction's hash equals the stored hash. **Replacement:** with the fee raised, the replacement's hash is in `RevealTxs` (synced) before the send call is made.
20. **Cancelled reveal:** the hash is reported cancelled; a new send with a new nonce is appended to `RevealTxs`.
21. **Older hash mined after a re-send:** `HasRevealed` is set from the older receipt; no further send.

Mutation checks (each must fail a test): key stored after the receipt again; key removed on a possibly-broadcast error; key-write error ignored; `exists` ignored after a restart; wait removed; monitor or chain client closed before the wait; localstore closed after the wait; early reveal cancelled instead of awaited on stop; a reveal sent while an earlier hash may still be mined; a not-found reveal re-signed instead of rebroadcast (hash changes); a replacement hash listed after the send; a rebroadcast send error swallowed; `HasRevealed` only from the newest hash; skip on a lookup error; `ErrNotBroadcast` on a `SendTransaction` error; the ambiguous send's nonce reused; a failed pre-broadcast send leaving the in-progress mark; `Pending()` not taking `commitGate`; reveal not retried; second signal not reaching `Shutdown`; third signal not exiting; commits not refused after shutdown starts; `Revealer.Reveal` not re-checking `HasRevealed`; two state instances; early reveal without the sync check; `revealPending` always false.

## Measurement

- **No staked bench node exists.** Plan: a Sepolia testnet node (funded test wallet, recorded privately), staked at the testnet minimum, with scripted restarts in the commit phase after a commit, at the start of the reveal phase, and in the claim phase.
  - Sepolia has 12 s blocks, so a wait can take about 450 s plus the receipt: above 300 s. Run the test node outside systemd, or with `TimeoutStopSec=600` on that node only.
  - Pass: no `StakeFrozen` event for the node; stop durations from the log lines; the persisted `stopWaitedForReveal` matches the restarts after a commit.
  - Whether the testnet selects the node often enough in a week is not known; if not, the tests above are the evidence and the decision to stake on mainnet carries the canary.
- **Chain baseline:** 0.33 % of commits end in a missed reveal across the network (30 days to 2026-10-10).

## Rollout

Implementation after this spec merges, in one PR or in this order: sections 0 and 1 (no behaviour visible to operators except a reveal that would otherwise be lost), then 2 with the packaging changes (`TimeoutStopSec=300`, `stop_grace_period: 5m`), then 3 and 4. The tracer double close and the never-closed transaction service (Problem 2) are fixed in the same change, since the shutdown order is rewritten anyway. Record in `docs/DIFFERENCES.md` (behaviour, setting, API fields, persisted counts, packaging). `affects-upstream` only after the reproduction tests run on the upstream tree.

Generated with help of AI.
