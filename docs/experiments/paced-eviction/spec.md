# Paced eviction: a radius increase that does not take the node offline

Issue: [#623](https://github.com/crtahlin/wasp/issues/623), part of the epic [#621](https://github.com/crtahlin/wasp/issues/621). Measurement: [#622](https://github.com/crtahlin/wasp/issues/622).

## Terms

- **Eviction:** deleting chunks from the reserve because it is over capacity. After a radius increase this is the chunks below the new storage radius, about half the reserve.
- **Round:** one step of eviction: read up to a fixed number of chunks to evict, delete them, release the locks.
- **Urgent eviction:** eviction while the disk under the data directory is nearly full, when freeing space matters more than staying gentle.

## Problem

**Measured (sw-1, 2026-10-08, radius-change scenario A):** ten nodes with reserve doubling 3 on one machine (8 threads, 32 GB, two SATA SSDs, no swap) were restarted with doubling 2. Each had to evict about 14 million chunks. The load average rose to about 95 with about 31 % of CPU time waiting on disk, and from about 2 hours after the restart the machine stopped answering SSH, and the nodes stopped answering the libp2p greeting, for at least 4.5 hours. The operator reports outages of a day or longer from real evictions on other machines.

**From the code (`origin/main`; the eviction code is unchanged from upstream v2.8.2):**

1. `unreserve` (`pkg/storer/reserve.go` 483 to 563) asks for the whole excess at once: `target` is reserve size minus capacity, and each batch is asked for `max(target-totalEvicted, minEvictCount)` chunks.
2. `EvictBatchBin` (`pkg/storer/internal/reserve/reserve.go` 331 to 430) collects every chunk to evict into memory first (lines 354 to 385), then deletes them with one transaction per chunk on `runtime.NumCPU()` goroutines (lines 390 to 408). Nothing limits the rate.
3. It holds the batch's lock for the whole call (line 337). `Put` takes the same lock (line 104), so a store into that batch waits until the batch's eviction ends.
4. The radius is raised after one pass at the old radius (`pkg/storer/reserve.go` 555 to 558), so the node stops claiming the chunks below the new radius minutes after the restart, long before it has deleted them.
5. New chunks are accepted while the reserve is over capacity (`ReservePutter`, `pkg/storer/reserve.go` 461 to 481); only the eviction brings it back.

So eviction is as fast and as wide as the machine allows, on purpose, although nothing about a radius increase needs it to be fast: chunks below the radius are no longer the node's responsibility and only take disk space until they are gone.

## Hypothesis

Deleting at a bounded rate, in small rounds that release their locks, keeps a node responsive during a radius increase: it keeps serving retrievals, syncing, answering handshakes and sampling within its budget, and a machine with several such nodes stays reachable. The cost is a longer eviction, hours instead of the fastest possible, which is acceptable while the disk has room. When the disk is nearly full, eviction switches to full speed, as today.

The hypothesis that the sw-1 stall was caused by eviction load is not yet confirmed: the machine's logs are not readable yet (see "Assumptions to revise").

## Design

All changes are inside the node. Nothing depends on other nodes on the same machine or on server settings.

### 1. Rounds instead of one long call

`unreserve` evicts in rounds of at most `evictionRound` chunks (compiled in, 1,000). A round reads up to that many items of the batch below the radius, deletes them, updates the counters, and releases the batch lock before the next round. This also bounds the memory of one round (the subject of [#628](https://github.com/crtahlin/wasp/issues/628), which this design satisfies for the paced path) and gives stores into the batch a chance between rounds.

Deleting the items of one round may keep the existing per-chunk transactions and the `NumCPU` limit at first; batching them into fewer transactions is [#624](https://github.com/crtahlin/wasp/issues/624) and changes only the cost per round.

### 2. A rate limit between rounds

A token bucket (`golang.org/x/time/rate`, already a dependency) with rate `reserve-eviction-rate` chunks per second and a burst of one round. Before each round, eviction waits for tokens for the whole round. The wait observes the context and `db.quit`, so shutdown and a batch-expiry signal still stop it as today.

Expired batches (`evictExpiredBatches`) use the same limiter: an expiry of a large batch on every node at the same block is the other way a whole network evicts at once.

### 3. Urgent eviction when the disk is nearly full

Before each round, eviction checks the free space of the filesystem holding the data directory (`statfs` on Unix, `GetDiskFreeSpaceEx` on Windows, both through `golang.org/x/sys`, already a dependency). When it is below `reserve-eviction-urgent-free-space`, the round runs without waiting for tokens. Back above it, pacing resumes. The check costs one system call per round.

This is what makes a slow rate safe: the node never runs out of disk because of pacing, since it evicts at full speed as soon as space is short.

### 4. Yielding to a sample

Pausing eviction while a reserve sample runs is [#625](https://github.com/crtahlin/wasp/issues/625). Rounds make it a check of one flag before each round. It is specified there, not here, but this design is what it builds on.

### 5. What stays the same

- The radius is still raised at the same point (`pkg/storer/reserve.go` 555 to 558). Changing when the radius rises changes what the node claims to the network, and is out of scope.
- What is evicted, and in which order of batches, is unchanged.
- `reserve-eviction-rate: 0` gives today's behaviour: no waiting, and one call per batch for the whole remaining target.

### 6. Counters and log

The evicted counter and reserve size are updated after each round (today: after each batch; [#626](https://github.com/crtahlin/wasp/issues/626) covers progress reporting in full). One Info line when an eviction starts says whether it is paced or urgent, the target and the rate.

## Protocol impact

None. Eviction is local to the node. Nothing under `pkg/p2p`, `pkg/swarm` or `pkg/config` changes, and no message or timing seen by peers changes. A paced node holds chunks below its radius for longer and can still serve them if asked; it does not claim them.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `reserve-eviction-rate` | 2,000 chunks per second (see "Decision for the operator") | The most chunks the node evicts per second while the disk has room. 0 means no limit, today's behaviour. |
| `reserve-eviction-urgent-free-space` | 10 GB | Below this much free space on the data directory's filesystem, eviction runs at full speed. 0 turns urgent eviction off. |

**Raising `reserve-eviction-rate`:** a radius increase finishes sooner and the disk space comes back sooner, at the cost of more disk and CPU load during it, closer to today's behaviour that took sw-1 offline. Other nodes pay only if this node becomes unresponsive: retrievals and syncs through it fail or slow down, and a host with several nodes can take all of them offline together.

**Lowering it:** the node stays more responsive, but holds the excess chunks for longer. At 2,000 per second an excess of 14 million chunks takes about 2 hours; at 500 about 8 hours. The disk must hold the excess for that time, and the reserve keeps growing with new chunks meanwhile; if new chunks arrive faster than the rate, the excess only shrinks when urgent eviction starts. Other nodes are not affected.

**Raising `reserve-eviction-urgent-free-space`:** urgent eviction starts earlier, so a small disk is protected sooner, but more evictions run at full speed and lose the benefit of pacing. **Lowering it:** pacing holds closer to a full disk; set too low, the disk can fill before urgent eviction frees space, and a full disk stops the node from storing anything.

Both settings take effect at start; they are not on-disk layout.

## Measurement

Accepted against [#622](https://github.com/crtahlin/wasp/issues/622), with 3 runs per condition.

**Single node, bench-1 or bench-2:** one node, reserve doubling lowered by one (an eviction of about half the reserve), nothing else running. Conditions: `reserve-eviction-rate: 0` (today) and the default. Recorded every 15 s: radius, reserve size, evicted count, resident memory, CPU, disk utilisation, API `/health` latency, a libp2p greeting answered or not, a known chunk retrieved or not; plus the duration and failed-chunk counts of a worst-case sample taken during the eviction, and the total eviction time.

**Ten nodes at once, sw-1:** scenario A with the default rate, compared with the run of 2026-10-08. Each run on sw-1 is a full increase and refill cycle of about a day, so 3 runs there take about 3 days of rental; if that is not available, sw-1 gives one run and the bench gives the three, and the result says so.

**Accepted when**, in all runs:
1. the node answers `/health` within 1 s and the libp2p greeting within 5 s throughout the eviction (today on sw-1: no answer for hours);
2. a worst-case sample during the eviction finishes within the budget of the machine's chain (about 570 s today, about 230 s with 2-second blocks) with no failed chunks;
3. the eviction completes, and the reserve returns to capacity, within the time the rate predicts plus 20 %.

**A negative result** looks like this: with pacing the node is still unresponsive or samples still fail, which would mean the load comes from something other than the deletion rate (for example memory, [#628](https://github.com/crtahlin/wasp/issues/628), or compaction, [#627](https://github.com/crtahlin/wasp/issues/627)); or the eviction never finishes at the default rate because new chunks arrive faster, which would mean the default is too low.

## Rollout and rollback

On by default (see the decision below). An operator gets today's behaviour with `reserve-eviction-rate: 0`. A node interrupted in the middle of a paced eviction resumes it after a restart, because `reserveWorker` triggers `unreserve` whenever the reserve is over capacity (`pkg/storer/reserve.go` 245 to 247). Rolling back means installing the previous release; no data changes.

## Upstream portability

Self-contained in `pkg/storer` and `pkg/storer/internal/reserve`, which are unchanged from upstream v2.8.2, so the change applies to Bee as it is. Upstream would need the two settings and the decision on a default.

## Operator extras (optional)

On top of the node-level pacing, an operator running several nodes on one machine may also:
- set a memory limit per node (`GOMEMLIMIT` in the service environment), so one node cannot take memory the others need;
- give the services a lower I/O and CPU weight (`IOWeight=`, `CPUWeight=` in systemd), so eviction yields to interactive use of the machine.

Neither is needed for the fix to work, and the measurement runs without them.

## Assumptions to revise with the sw-1 data

The sw-1 logs from 2026-10-08 are not readable yet ([#622](https://github.com/crtahlin/wasp/issues/622)). These parts of the spec rest on assumptions:

1. **The stall was caused by eviction load (disk and CPU), not only by memory.** If the kernel log shows out-of-memory kills or memory pressure as the cause, [#628](https://github.com/crtahlin/wasp/issues/628) comes first, and pacing is still needed but second.
2. **The deletion rate of today's code.** From Pebble's flushed bytes it was in the order of 1,400 chunks per second per node with ten nodes at once; not counted directly. The default rate is chosen against this.
3. **New chunks arrive much slower than the default rate** on a settled node, so a paced eviction finishes. To be checked against the pull-sync rates of the sw-1 nodes during the eviction.
4. **Samples failed or slowed during the eviction.** Expected from the code, not yet seen in a sample log.

## Decision for the operator

1. **The default rate.** Rule 8 says a new setting defaults to the current behaviour, which is no limit. The spec proposes 2,000 chunks per second instead, because the current behaviour is what takes nodes offline. The alternative is a default of 0 and documentation that recommends a rate.
2. **The urgent threshold of 10 GB**, or a percentage of the disk instead.

## Open questions

- Whether expired-batch eviction should use the same rate or a separate one: a large batch expiring is predictable for the whole network at the same block, so a separate, higher rate might be preferred.

Generated with help of AI.
