# Paced eviction: a radius increase that does not take the node offline

Issue: [#623](https://github.com/crtahlin/wasp/issues/623), part of the epic [#621](https://github.com/crtahlin/wasp/issues/621). Related: [#622](https://github.com/crtahlin/wasp/issues/622) (measurement), [#628](https://github.com/crtahlin/wasp/issues/628) (memory), [#625](https://github.com/crtahlin/wasp/issues/625) (sampling), [#627](https://github.com/crtahlin/wasp/issues/627) (compaction).

## Terms

- **Eviction:** deleting chunks from the reserve because it is over capacity (`unreserve`), or because their batch expired (`evictExpiredBatches`). After a radius increase, eviction deletes the chunks below the new storage radius, about half the reserve.
- **Round:** one step of eviction: read up to a fixed number of a batch's chunks to evict, delete them, update the counters, release the batch lock.
- **Arrival rate:** how many chunks per second the node adds to its reserve, measured inside the node from its own puts.

## Problem

**Measured (sw-1, 2026-10-08, radius-change scenario A):** ten nodes with reserve doubling 3 on one machine (8 threads, 32 GB, two SATA SSDs, no swap) were restarted with doubling 2. Each had to evict about 14 million chunks. The load average rose to about 95 with about 31 % of CPU time waiting on disk, and from about 2 hours after the restart the machine stopped answering SSH, and the nodes stopped answering the libp2p greeting, for at least 4.5 hours. The operator reports outages of a day or longer from real evictions on other machines. The machine's logs are not yet readable, so the cause (eviction load, memory, or both) is not confirmed; see "Assumptions to revise".

**From the code (`origin/main`).** `pkg/storer/internal/reserve/reserve.go` is unchanged from upstream v2.8.2. `pkg/storer/reserve.go` is not: wasp added, among other things, the shutdown checks of #407 in `evictExpiredBatches` (lines 316 to 325) and `unreserve` (lines 522 to 527), which the new loop must keep.

1. `unreserve` (`pkg/storer/reserve.go` 483 to 563) fixes its `target`, reserve size minus capacity, when it starts (line 497). Chunks that arrive later raise the reserve again and start the next over-capacity eviction. Each batch is asked for `max(target-totalEvicted, minEvictCount)` chunks.
2. `EvictBatchBin` (`pkg/storer/internal/reserve/reserve.go` 331 to 430) collects every chunk to evict into memory first (lines 354 to 385), then deletes them with one transaction per chunk on `runtime.NumCPU()` goroutines (lines 390 to 408). Nothing limits the rate or the memory.
3. It holds the batch's lock for the whole call (line 337). `Put` (line 104) and `Get` (lines 306 to 308) take the same lock, so stores into the batch and retrievals of its chunks wait until the batch's eviction ends.
4. The radius is raised after one pass at the old radius (`pkg/storer/reserve.go` 555 to 558), before the main deletion at the new radius.
5. New chunks are accepted while the reserve is over capacity (`ReservePutter`, lines 461 to 481).
6. Deleting a chunk does not shrink files on disk. Sharky's `release` only marks the slot reusable (`pkg/sharky/shard.go` 197 to 198); its files shrink only through `TruncateAt` during recovery at start (`pkg/sharky/recovery.go` 106 to 113). Pebble frees space only after compaction (#627).

So eviction runs as fast and as wide as the machine allows, although a radius increase does not need it to be fast: chunks below the radius are no longer the node's responsibility.

## Hypothesis

Deleting in small rounds, with few workers and a bounded rate, keeps a node responsive during a radius increase: it keeps serving retrievals, syncing, answering handshakes and sampling within its budget, and a machine with several such nodes stays reachable. The cost is a longer eviction, which is acceptable because the chunks below the radius cost only disk space while they wait. Unconditional rounds also bound the memory of an eviction, whatever the rate.

## Design

All changes are inside the node. Nothing depends on other nodes on the same machine or on server settings.

### 1. Rounds, always

`EvictBatchBin` works in rounds of at most `evictionRound` chunks (compiled in; 1,000, smaller only if the bench measurement shows rounds of 1,000 still cause latency spikes). A round reads at most one burst of the rate limiter, which is one round. A round:

- iterates the batch's `BatchRadiusItem` entries (key: batch ID, bin, address, stamp hash; `pkg/storer/internal/reserve/items.go` 35 to 38) with `Prefix: batchID` in the first round and, after it, `PrefixAtStart` from the last key the previous round read, so it never walks again over the deletion markers of earlier rounds. Not `SkipFirst`: the resume key was deleted, so skipping the first result would skip a live item. With `PrefixAtStart` pebblestore bounds the iteration only by the namespace, not by the batch (`pkg/storage/pebblestore/store.go` 243 to 246), so the callback stops explicitly when an item's `BatchID` differs from the batch being evicted, as well as at `Bin >= bin` as today. The leveldb engine is still selectable (`pkg/storer/storer.go` 331, 337) and positions a `PrefixAtStart` query differently, so the resume behaviour is tested on both engines;
- stops when the batch's entries end, when it has collected `evictionRound` items, or when it reaches the count asked for;
- deletes those items, adds them to `size` and to the evicted counter, and releases the batch lock.

Rounds apply at every rate, including 0. This answers the collect-everything part of #628: the memory of an eviction is bounded by one round instead of growing with the target. #628's other parts (measuring the memory curve, and the cost of `pinUuids` lookups per item) stay with that issue.

**Interface.** `EvictBatchBin(ctx, batchID, count, bin, hooks)` gains a `hooks` value with two per-round callbacks supplied by `pkg/storer`: `before(n int) error`, called after a round has read `n` items and before it deletes them (it waits for tokens, and returns an error to abort on shutdown or expiry), and `after(n int)`, called with the number deleted (it updates metrics and the arrival accounting). With empty hooks, rounds run at full speed.

**Releasing the lock between rounds is safe.** All eviction runs on the single `reserveWorker` goroutine (`pkg/storer/reserve.go` 253 to 305), so two rounds never overlap. A chunk stored into the batch between rounds is at or above the radius, and not evicted, or below it: then a later round takes it if its key comes after the resume point, and the next `unreserve` takes it if it comes before. `size` is updated after each round. A batch expiry aborts `unreserve` as today, and the expired-batch marker is deleted only after the batch is gone (lines 334 to 336). After such an abort the resume key is lost, and a restarted `unreserve` walks the deleted entries of that batch once more; acceptable for a rare event. The radius is persisted by `SetRadius` (`pkg/storer/internal/reserve/reserve.go` 717 to 723).

### 2. Pacing: rate and workers

A token bucket (`golang.org/x/time/rate`, already a dependency) with a burst of one round. A round **reads first, then takes tokens for the items it actually read** (`Limiter.ReserveN(now, n)` in the `before` hook), and does not wait at all when it read nothing. The batch-expiry signal is a channel (`pkg/storer/reserve.go` 504, 528), not a context, so the wait is a `select` on a timer for the reserved delay, `db.quit`, the batch-expiry channel and the context; any of the last three cancels the reservation and returns as today. A setting of 0 maps to `rate.Inf`: in `x/time/rate`, `Limit(0)` allows only the burst and then blocks.

**Effect on the radius increase.** The first pass of `unreserve` runs at the old radius over every batch (`pkg/storer/reserve.go` 518 to 553), and most batches hold nothing below it. Because rounds that read nothing do not wait, that pass takes about as long as today, and the radius still rises about 1 s after the eviction starts (measured on sw-1, #621). Taking tokens before reading would have made each batch after the first burst wait about 3.3 s at 300 per second; with about 426 batches the radius would have risen after about 23 minutes, during which `CommittedDepth()` stays one below the network's and salud marks the node unhealthy for a storage radius discrepancy (`pkg/salud/salud.go` 247 to 249).

When the rate is finite, a round deletes on `evictionWorkers` goroutines (compiled in at 2 unless the bench shows otherwise) instead of `runtime.NumCPU()`. Limiting the rate alone would only cap the average: each round would still run its deletions on every CPU thread, so the peak load would stay. At rate 0 the round keeps `runtime.NumCPU()`, today's behaviour apart from the round size.

### 3. Keeping up with arrivals

The node counts arrivals as increases of its reserve `size` over a sliding window of one minute, not as `ReservePutter` calls: `Put` returns early for a chunk the reserve already holds (`pkg/storer/internal/reserve/reserve.go` 115 to 118), and a stamp-index replacement does not grow `size` (lines 284 to 288).

The effective rate is `max(reserve-eviction-rate, arrival rate)`. When that is above what `evictionWorkers` workers delete, the worker count rises, up to `runtime.NumCPU()`, and falls back when arrivals slow down: each round uses the smallest worker count that met the effective rate in the previous rounds, measured from how long each round's deletions took. So the excess does not grow during a paced eviction. The cost is the pacing benefit while arrivals are high: during a pull-sync burst the node deletes about as fast as it receives, with more workers and more disk load, even when the disk has room. This is chosen over capping the floor, because a capped floor lets the excess, and with it the shard files, grow without bound during a long burst. It needs no system call and no knowledge of the filesystem.

**Coupling.** Eviction and pull-sync compete for the same disk, so a busy eviction slows the puller, which lowers the arrival rate and with it the floor; with a one-minute window the risk of the two oscillating is low. While the puller pauses for a sample (#23), arrivals drop to 0 and the floor with them, so eviction falls back to the configured rate, which is the intended behaviour during a sample.

An earlier draft switched to full speed when free disk space was low. It is dropped: deleting chunks does not free filesystem space (Problem, point 6), so a node that turned urgent would stay urgent until restart, and on a filesystem shared by several nodes every node would cross the threshold together and evict at full speed, recreating scenario A.

**The disk cost of pacing.** Chunks that arrive during a paced eviction cannot always reuse slots that are not yet freed, so the shard files grow by about 4 KB per arriving chunk for as long as the eviction lasts, and the space is reclaimed only by compaction (#627) and sharky's truncation at start. Following the arrival rate keeps this to the arrivals during the eviction.

### 4. What is dropped, and why

**Deferring the deletion while the disk has room** (an idea in #623) is dropped. It would be safe for sampling (Notes below), push-sync (`waitNetworkRFunc`, `pkg/node/node.go` 1450 to 1465) and salud, but it has two costs this design avoids: the sharky growth above for the whole deferral, and that a worker sitting inside `unreserve` cannot lower the radius if the network shrinks meanwhile. Pacing gives most of the benefit without them.

### 5. Expired batches

`evictExpiredBatches` uses the same rounds and the same limiter. Pacing it has a cost to other nodes that a radius increase does not have: an expired batch's chunks stay inside the radius for longer, so peers that pull-sync from this node fetch them and reject them on stamp validation, spending their bandwidth, and the `ReserveSize` this node reports through the status protocol (`pkg/status/status.go` 141) stays inflated for longer. Because an expiry is predictable, a random start delay per node (for example up to 10 minutes) would spread a network-wide expiry without slowing the deletion of chunks inside the radius. Which of the two is used for expired batches is an open question for the operator.

### 6. Counters and log

The evicted counter and the reserve size are updated after each round (today: after each batch). One Info line when an eviction starts says the target, the rate and the number of workers. Full progress reporting is #626.

### 7. What stays the same

- The radius is raised at the same point in the code and, because empty rounds do not wait, at about the same time after the start (section 2). Changing when it rises changes what the node claims to the network.
- What is evicted, and in which order of batches.
- With `reserve-eviction-rate: 0`: no waiting and `runtime.NumCPU()` workers, as today; only the rounds and the arrival-rate floor (which has no effect at full speed) differ.

## Protocol impact

None. Eviction is local to the node. Nothing under `pkg/p2p`, `pkg/swarm` or `pkg/config` changes, and no message format changes. Peers see a node that holds chunks below its radius for longer and can still serve them if asked, and, for paced expired batches, chunks that fail stamp validation for longer (section 5).

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `reserve-eviction-rate` | 0 until the bench measurement exists (see "Decisions for the operator") | The most chunks the node evicts per second, raised to the arrival rate when that is higher, with more workers if needed. 0 means no limit and today's number of workers. |

**Raising `reserve-eviction-rate`:** a radius increase finishes sooner, at the cost of more disk and CPU load during it, closer to today's behaviour that took sw-1 offline. Other nodes pay only if this node becomes unresponsive: retrievals and syncs through it fail or slow down. For expired batches a higher rate shortens the time peers fetch and reject expired chunks.

**Lowering it:** the node stays more responsive, but the eviction takes longer and the shard files grow about 4 KB per chunk arriving meanwhile, reclaimed only by compaction. When arrivals exceed the rate the node deletes at the arrival rate anyway, so a low setting does not let the excess grow, but it does not protect the host during a sync burst either. For expired batches, peers fetch and reject expired chunks for longer, which costs their bandwidth, and the reserve size this node reports stays inflated for longer.

**Expected rates (estimates to replace with the bench measurement):** if the rate today, with ten nodes evicting at once, was about 1,400 chunks per second per node (assumption, see below), the machine as a whole deleted about 14,000 per second while stalling. A per-node rate of 300 would keep ten nodes at about 3,000 per second together, about a fifth of that, and an excess of 14 million chunks would take about 13 hours per node.

The setting takes effect at start; it is not on-disk layout.

## Measurement

**State between runs.** Each run starts from a copy of the data directory taken once before the first run (a stopped node, `cp -a` or a filesystem snapshot), restored before every run; otherwise each run would first need about a day of pull-syncing to refill the reserve. For a node at doubling 1 the copy is about 35 GB of sharky files plus the index store, per node; the runner checks there is room for the copy and one restored run before it starts. After each restore it records the batches that expired since the copy was taken and the catch-up sync rate, because a restored node syncs what it missed, which competes with the eviction.

**Conditions, 3 runs each, interleaved** (1, 2, 3, 4, then again, then again), so that a change in network load affects every condition alike:

1. today's code (`main`);
2. rounds at rate 0 (separates the memory effect of rounds from pacing);
3. rounds at a paced rate, with `evictionWorkers` workers;
4. expired-batch eviction of a large batch, today's code and the paced version. The batch is made to expire on demand on a testnet bench node: buy a batch with a lifetime of a few hours, store about a million chunks under it by local upload (on testnet the node's radius covers them), take the data-directory copy, and let the batch expire; each later run restores the copy and waits for the expiry again.

Recorded every 15 s: radius, reserve size, evicted count, resident memory and Go heap, CPU, disk utilisation, API `/health` latency, whether the libp2p greeting is answered, whether a known chunk is retrieved, the arrival rate and the worker count; plus the duration and failed-chunk counts of a worst-case sample during the eviction, the total eviction time and the growth of the shard files.

**Setup.** One bench node can lower its doubling only from 1 to 0 (`max-reserve-capacity-doubling` defaults to 1, `cmd/bee/cmd/cmd.go` 454): an eviction of about 4 million chunks without contention. Today's code may pass the criteria there, which would show nothing (rule 7). So the first step is to run condition 1 on that setup and check whether it breaks criterion 1 or 2. If it does not, the setup becomes four nodes at doubling 1 on one bench host whose disk holds them, evicting together: enough to put a bench disk under the kind of load sw-1 had with ten, while fitting one bench machine. Acceptance is judged on whichever setup reproduces the problem with condition 1.

**Time.** A paced run of about 4 million chunks at 300 per second takes about 3.9 hours; four conditions times 3 runs, with restores, is about two days of bench time.

**sw-1,** only after the bench result and if rental time allows: scenario A with the paced rate, compared with the run of 2026-10-08. If fewer than 3 runs fit, the result says so.

**Accepted when,** on the setup that reproduces the problem, condition 1 breaks criterion 1 or 2, and in all 3 runs of condition 3:
1. `/health` answers within 1 s and the libp2p greeting within 5 s throughout the eviction;
2. a worst-case sample during the eviction finishes within the budget of the chain the bench node is on, with no failed chunks;
3. peak memory does not grow with the eviction target (conditions 2 and 3 against 1);
4. the eviction completes within the time the rate predicts, plus 20 %, and the radius rises as soon after the start as in condition 1.

**A negative result** looks like this: with rounds and pacing the node is still unresponsive or samples still fail, which would point at something else (compaction, #627, or the per-chunk transactions, #624); or the eviction never finishes because deletion cannot keep up with arrivals even at `runtime.NumCPU()` workers.

## Rollout and rollback

With the default of 0, the only change an operator sees is the bounded memory of rounds. Pacing is turned on by setting a rate. A node interrupted in the middle of an eviction resumes it after a restart, because `reserveWorker` triggers `unreserve` whenever the reserve is over capacity (`pkg/storer/reserve.go` 245 to 247). Rolling back means installing the previous release; no data changes.

## Upstream portability

The core change is in `pkg/storer/internal/reserve`, unchanged from upstream v2.8.2, and in the `unreserve` loop. The wasp-only shutdown checks of #407 would be left out of an upstream port. Upstream would need the setting and the decision on a default.

## Operator extras (optional)

On top of the node-level fix, an operator running several nodes on one machine may also set a memory limit per node (`GOMEMLIMIT`) and lower I/O and CPU weights for the services (`IOWeight=`, `CPUWeight=` in systemd). Neither is needed for the fix to work, and the measurement runs without them.

## Notes

- **A radius-increase eviction never deletes chunks the sampler reads.** `sampleBins` limits the sample to bins at or above the storage radius (`pkg/storer/sample.go` 651 to 659), and the radius rises before the deletion at the new radius (`pkg/storer/reserve.go` 555 to 557), which deletes only bins below it. For a radius increase, eviction and sampling compete only for the disk. The correctness concern in #625 applies to expired-batch eviction, which deletes inside the radius.

## Assumptions to revise with the sw-1 data

The sw-1 logs from 2026-10-08 are not readable yet (#622):

1. **The stall came from eviction load, not only from memory.** If the kernel log shows out-of-memory kills, the rounds (memory) matter more than the rate; both are in this design.
2. **Today's deletion rate of about 1,400 chunks per second per node** is derived, not counted: Pebble flushed about 34 MB per minute per node, about 0.57 MB per second, and the figure assumes about 400 bytes of index writes per deleted chunk (five index entries removed per chunk, `RemoveChunkWithItem`). It could be off by a factor of two either way.
3. **New chunks arrive far slower than any useful rate** on a settled node, so the arrival-rate floor rarely applies. To check against the pull-sync rates during the eviction.
4. **Samples slowed during the eviction.** Expected from disk contention, not yet seen in a sample log.

## Decisions for the operator

1. **Turn pacing on by default after the bench measurement?** Rule 8 keeps the default at 0 until a measurement justifies a value. The measurement will propose one.
2. **Expired batches:** pace them like a radius increase (cost to peers, section 5), or keep them at full speed and spread a network-wide expiry with a random start delay per node instead.

Generated with help of AI.
