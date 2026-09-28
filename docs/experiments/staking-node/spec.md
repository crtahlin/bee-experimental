# An always-on staking node, running Pebble

Issue: [#485](https://github.com/crtahlin/wasp/issues/485)

A hosted node, always on, holding stake, running a tagged wasp release with the
Pebble index engine. Referred to here as `stake-1`.

## What makes this different from every existing node

`bench-1` and `bench-2` can be restarted, misconfigured and rolled back freely,
because nothing is staked on them. `stake-1` cannot: a missed redistribution
round is forgone income, and a rejected proof puts stake at risk. It is also
meant to tell us how Pebble behaves under a real staking workload over months,
which is the only way that question gets answered.

Those two purposes are compatible for exactly one reason: **Pebble is the shipped
default for a new data directory from v0.1.4**, so a fresh install of a tagged
release is already a Pebble node. Nothing has to be switched on, no branch has
to be run, and no setting has to be moved off its default. The experiment is
observation of a default configuration, and it therefore adds no risk beyond
running the node at all.

That framing is load-bearing. The moment something has to be *changed* on
`stake-1` to learn something, it belongs on a bench node instead.

## Rules for the node

- **Tagged releases only.** Never `main`, never an experiment branch, never a
  locally built binary. A change wanted there goes through a release first.
- **Never an arm in a comparison.** Anything needing matched node state,
  restarts or configuration changes runs on `bench-1` and `bench-2`.
- **Rule 4 applies with no exception.** A change that could affect stake or data
  pauses for the operator. A clean review is not sufficient authority here.
- **Role name only in this repository**, per rule 10. Host, credentials and
  configuration live in `bee-experimental-infra`, uncommitted. Wallet and stake
  keys stay on the node.

## Pre-stake gates

These run **after provisioning and before any stake is placed**. That ordering is
not a preference: the numbers depend on the box's disk, so they cannot be
measured earlier, and they decide whether staking is viable at all, so they must
not be measured later.

### Gate 1: the reserve sample must finish inside a phase

This is the gate that decides viability, and it is about the disk rather than
about wasp.

A redistribution round is `DefaultBlocksPerRound = 152` blocks and divides into
phases of `152 / 4 = 38` blocks (`pkg/storageincentives/agent.go:37-38`). At the
5 second default `block-time` (`cmd/bee/cmd/cmd.go:416`) that is **760 s per
round and 190 s per phase**, which matches the observed round cadence on the
bench of roughly 13 minutes.

**Correction (2026-09-28, #540):** this spec first said the sample must fit in
one 190 s phase. The code allows more. Sampling starts when the claim phase
starts (block 76 of the round) and is cancelled when the next round's reveal
phase starts (block 190). So the budget is about **114 blocks, less the time to
send the commit**: 570 s at 5 s blocks. Gnosis Chain is moving to 2 s blocks
(GIP-153, targeted for December 2026 or January 2027), which makes it about
228 s. The pass line below is kept at 120 s, and at 2 s blocks it leaves far
less headroom than the 190 s figure suggested.

Measured on bench hardware, the sample takes 69.0 s with SIMD hashing and 92.0 s
without, over 243 runs. So the bench has roughly 2.7x headroom. A slow hosted
disk can plausibly consume that.

**Pass:** three runs at the intended reserve size, every one at or under
**120 s**, with the spread reported per rule 7.

120 s rather than the phase length deliberately. The remaining time absorbs reserve growth,
accumulated Pebble compaction debt, and a noisy neighbour on shared hosting. A
node that passes at 185 s passes today and fails silently in a month, and the
failure mode is missed rounds with a node that otherwise looks healthy.

**Measured at the intended reserve size, not at an empty reserve.** An empty
reserve samples quickly and proves nothing. If the reserve cannot be filled
before the measurement, the gate is deferred, not waived.

**On failure:** reduce `reserve-capacity`, or use a different host. Not a tuning
parameter. Sampling faster than the disk allows is not something a setting can
grant.

### Gate 2: no write pauses while the reserve fills

Filling the reserve is the heaviest sustained write the node will ever do. If
level-0 depth reaches the write-pause trigger during it, the disk is too slow for
the chosen reserve size and the size comes down.

The instrumentation already exists and needs no new code: index store level-0
depth and write-pause state (#113), a log line when the store stops accepting
writes (#180), and Pebble's physical write bytes (#218).

**Pass:** the write-pause state is never entered during the fill, and level-0
depth stays below `db-compaction-l0-trigger`.

### Gate 3: reachability, established properly

**Pass:** the node's own AutoNAT verdict reads
`bee_kademlia_reachability_status{reachability_status="Public"}`, **and** inbound
accepted connections rise across three samples spaced at least ten minutes apart.

Both, not either. AutoNAT is a third-party verdict, since it is set by other
peers dialling back in successfully, and the rising count is direct evidence of
peers choosing to connect. A port opened and checked from the same network
proves nothing, and testing from inside the same NAT already produced one
retracted claim in this project.

## Configuration

| Setting | Value | Why |
|---|---|---|
| `reserve-proof-mode` | `classic` | Only `classic` is what the live contract verifies. `windowed` is experimental. This is the single setting most likely to cost stake if it is wrong. |
| `storage-engine` | `pebble`, written explicitly | It is the default for a fresh data directory, but stating it means the engine is never in doubt after an upgrade or a config regeneration. |
| `use-simd-hashing` | on, the wasp default | Worth 25 per cent on the sample, which goes straight to gate 1. |
| `stake-recovery-on-startup` | `off` | Only relevant with stranded stake in a retired contract. |
| `reserve-capacity` | set by gate 1 | Sized to what the disk can sample in time, rather than to what it can store. |

## Monitoring, and the gap that has to close first

The node-side sampler running on the bench nodes records health, readiness,
restarts, peers, blocklist, connection counts, retrieval and accounting error
counters, goroutines, and per-interval panic and unclean-shutdown detection. It
survives the operator's machine going away, which a laptop-side poller did not.

**None of that tells you whether a staking node is earning.** A node can report
every one of those fields as healthy while failing every round. Before `stake-1`
stakes, the sampler gains:

- whether the node entered each round, and the round number
- whether commit and reveal were made, and whether either failed
- whether a submitted proof was rejected
- **the reserve sample duration per round**, which is both the gate 1 number and
  the first thing that degrades as a Pebble store ages

The last one is the single most valuable field on this node. A rising sample
duration is the early warning for gate 1 failing later, and it is observable
long before a round is actually missed.

`phase failed` with `IsPlaying ... execution reverted` is the **normal** line for
a node not playing a round, and both bench nodes emit it every round. The
sampler must not treat it as a fault, or every sample will carry a false
positive and the log becomes useless.

## What maintaining it means

In scope: deploying tagged releases, health and configuration checks, log and
metric diagnosis, upgrades, incident investigation, and keeping the records
current.

Out of scope, stated because it would otherwise be assumed: **continuous
reaction.** Sessions are not always running. The node-side sampler records
continuously and systemd restarts a crashed unit, but nothing diagnoses or
decides in between. Anything that must react without a human or a live session
is node-side automation and is separate work, not part of this.

## Rollback, and one asymmetry

A tagged release rolls back to the previous tagged release, and the previous
binary is kept on the node, as on the bench nodes.

**The engine does not roll back the same way.** A data directory is bound to the
engine it was created with, so going from Pebble to goleveldb is a new data
directory and a full resync, not a binary swap. On a staking node that means
downtime measured in hours and a reserve rebuilt from the network.

The consequence for this experiment: **Pebble is effectively a one-way decision
for `stake-1`.** If Pebble under staking turns out to be the wrong answer, the
recovery is rebuilding the node, not reverting a setting. That has to be accepted
before staking, and it is the strongest argument for gates 1 and 2 being strict.

## Verification

The gates above are the verification, and the node does not stake until all
three pass. After staking, the first month of per-round sample durations is the
result this experiment exists to produce, and it gets a results document and a
ledger row.

## Files

No Go code. Configuration and harnesses live in `bee-experimental-infra`. The
sampler additions are to `node-soak.sh` there, not to this repository.

Generated with help of AI.
