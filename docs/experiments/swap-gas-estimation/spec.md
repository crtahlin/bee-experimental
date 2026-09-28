# Estimate gas for the swap transactions, and recover from a reverted chequebook deployment

Issue: [#541](https://github.com/crtahlin/wasp/issues/541).
Type: fix.

## Problem

Three swap transactions are sent with a fixed gas limit, and a non-zero
`GasLimit` makes `prepareTransaction` (`pkg/transaction/transaction.go`) skip
`eth_estimateGas` entirely:

**Table: swap transactions with a fixed gas limit, v2.8.2 and wasp `main`**

| Transaction | Where | Fixed limit |
|---|---|---|
| Chequebook deployment (`deploySimpleSwap`) | `pkg/settlement/swap/chequebook/factory.go`, `Deploy` | 175,000 |
| Token transfer: chequebook deposit and the wallet's BZZ withdrawal | `pkg/settlement/swap/erc20/erc20.go`, `Transfer` | 90,000 |
| Chequebook withdrawal | `pkg/settlement/swap/chequebook/chequebook.go`, `Withdraw` | 95,000 |
| Cheque cashout | `pkg/settlement/swap/chequebook/cashout.go`, `CashCheque` | 300,000, unless the request's `Gas-Limit` header sets one |

Ethereum's Glamsterdam upgrade activates on Sepolia on 2026-10-06 at 13:53:36
UTC, and Swarm's testnet runs on Sepolia. It includes EIP-8037, which raises
the cost of creating state:
- new account: 25,000 → 183,600
- CREATE: 32,000 → 183,600
- code deposit: 200 → 1,530 per byte
- new storage slot: 20,000 → 97,920

It also includes EIP-8038, which raises state-access costs. The EIP values are
still marked "not yet final". Gnosis Chain adopts Ethereum's EIPs at a date not
yet announced.

- **The chequebook deployment** used 141,893 to 141,905 gas in three recent
  Gnosis deployments. It creates one account, 45 bytes of code and about three
  new storage slots. Under EIP-8037 that comes to about 616,000 gas (the
  breakdown is in #541), so it reverts.
- **The token transfer and the withdrawal** need more than 100,000 whenever the
  recipient's token balance slot is new. That is always true of a fresh
  chequebook's first deposit.

**A reverted deployment cannot be recovered without deleting node state.**
`InitChequebookService` (`init.go`) saves the deployment's transaction hash
(`ChequebookDeploymentKey`) before it confirms. `WaitDeployed` then fails with
"contract deployment failed: transaction reverted", and the node exits. Every
later start finds the saved hash, waits for the same reverted transaction, and
exits again. Nothing clears the key.

## Design

### 1. Estimate, with today's value as the floor

The deployment, the token transfer and the withdrawal send `GasLimit: 0` and set
`MinEstimatedGasLimit` to the value they use today (175,000, 90,000 and 95,000).
`prepareTransaction` then does what it already does for postage, staking and
redistribution:
- estimate, add 33%, raise to the floor if lower, and cap at 10,000,000;
- if estimation fails, use the floor.

So on today's gas schedule the limits are never lower than now, and under
Glamsterdam's schedule they follow the estimate. EIP-8037 requires
`eth_estimateGas` to include the new state-gas charges.

The cashout keeps the `Gas-Limit` header as an override. Without the header, it
estimates with 300,000 as the floor instead of using 300,000 fixed.

The API's per-request `Gas-Limit` header still wins wherever it applies today.

### 2. A reverted deployment clears its saved hash

In `InitChequebookService`, when waiting for a saved deployment fails because
the transaction reverted (`transaction.ErrTransactionReverted` in the error
chain), the node:
1. deletes `ChequebookDeploymentKey`;
2. logs an error that names the transaction and says the next start will deploy
   again;
3. returns the error, as today.

It does **not** deploy again in the same start. A revert means something about
the build or the chain is wrong, and deploying again at once would spend gas on
the same mistake. The operator's restart is the retry. Under a service manager
that restarts on failure, this means one new deployment per restart. That is
the same exposure as any other repeated startup failure, and the log says why.

Any other error while waiting (RPC failure, timeout, a receipt not found yet)
keeps the key exactly as today, so a deployment that is merely slow is never
sent twice.

### Not changed

- The fallback of 500,000 used when estimation fails for a transaction without
  a floor (postage, staking), and the cap of 10,000,000. Whether 500,000 is
  still enough under Glamsterdam is measured below (creating a postage batch),
  not assumed.
- The balance check before deploying (`checkBalance` requires native balance
  for 250,000 gas at the current price). It is a warning threshold, not a gas
  limit, and a deployment that needs about 616,000 gas at Sepolia prices still
  costs little.
  **Open point for review:** raising it to match the estimate.

## If upstream Bee fixes it first

wasp takes upstream changes only from final release tags
(`docs/agent-playbooks/upstream-sync.md`).
- **If an upstream release carrying a fix appears before this is implemented,**
  it is absorbed by the normal sync. This spec is then closed as covered by
  that sync, after the Sepolia measurement below confirms the fix.
- **If the fix exists only on upstream `master`,** it is not taken. This spec is
  implemented instead, following upstream's approach where it is compatible, so
  that the next sync merges cleanly.

## Protocol impact

None. No message, field or constant in `.github/protocol-freeze.lock` changes.
Only the gas limit of on-chain transactions changes, and nothing is sent to
peers.

## Tests

In `pkg/settlement/swap/...`, with the existing transaction service mocks:

- **Deployment, token transfer and withdrawal** each send `GasLimit` 0 and
  `MinEstimatedGasLimit` equal to today's value.
- **Cashout** sends `GasLimit` 0 with a floor of 300,000 when no header is set,
  and the header's value as a fixed limit when one is set.
- **A saved deployment that reverted** is cleared, and the error is returned. On
  the next init a new deployment is sent.
- **A saved deployment whose wait fails for another reason** (RPC error) keeps
  its key, and no new deployment is sent.

In `pkg/transaction`, existing tests already cover the floor and the fallback.

Mutations:
- put each fixed limit back;
- clear the key on any wait error;
- do not clear it on a revert.

## Measurement

On Sepolia after 2026-10-06, with a bench node in testnet mode and a fresh
account funded from the testnet wallet. Each case is run with v0.1.5 and with
the fix, three times per condition:

- deploy a chequebook;
- make the first deposit into it;
- withdraw to an owner holding no sBZZ;
- make the first cashout to a fresh beneficiary;
- create a postage batch, recording its gas against the 500,000 fallback.

Expected: v0.1.5 reverts on the deployment and on the transfers into empty
balances, and the fix succeeds on all of them. For each transaction, record the
estimated limit and the gas used. A negative result is any revert with the fix,
or a limit below the gas used.

## Rollout and rollback

No configuration. Rollback is reverting the merge. Nodes that already have a
chequebook are unaffected by both the change and a rollback.

## Upstream portability

All four call sites and `init.go` are unmodified upstream code, so the change
applies to upstream as is. #541 carries `affects-upstream`.

## Files

- `pkg/settlement/swap/chequebook/factory.go`, `chequebook.go`, `cashout.go`,
  `init.go`
- `pkg/settlement/swap/erc20/erc20.go`
- tests next to each
- `docs/DIFFERENCES.md`, `docs/UPSTREAM.md`, `docs/experiments/INDEX.md`

Generated with help of AI.
