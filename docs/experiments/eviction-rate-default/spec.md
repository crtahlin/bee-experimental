# Default eviction rate: 500 chunks per second instead of no limit

Issue: [#651](https://github.com/crtahlin/wasp/issues/651). Related: [#623](https://github.com/crtahlin/wasp/issues/623) (paced eviction, where the setting comes from), [#649](https://github.com/crtahlin/wasp/issues/649) (sit out the lottery while evicting), [#650](https://github.com/crtahlin/wasp/issues/650) (the measurement that confirms or adjusts this default), [#540](https://github.com/crtahlin/wasp/issues/540) (2-second blocks).

## Decision

The operator decided on 2026-10-09 to give `reserve-eviction-rate` a non-zero default now, from the estimate below, and to re-measure later with #650.

This departs from two parts of rule 8 (`AGENTS.md`):
- rule 8 asks for the measurement first;
- rule 8 makes the current value (here 0, no limit, Bee's behaviour apart from the rounds of #623) the default of a new setting.

Both are reversed on purpose, and #650 stays planned. Only the default changes; the setting and its behaviour exist since #623.

## Problem (measured)

Ten nodes with `reserve-capacity-doubling: 3` on one 8-core host (the dense bench host) were switched to half capacity at once on the paced-eviction build, at today's default 0. Each evicted about 14 M chunks (9.3 to 13.9 M by the evicted counter).

| Measure (scenario A2, 2026-10-09) | During the eviction | Before or after |
|---|---|---|
| Eviction rate per node | about 2,600 to 3,150 chunks/s (2.6 M in the first 13 minutes on the fastest nodes; about 14 M in 74 minutes) | - |
| Duration, first restart to last node at capacity | 1 h 14 min | - |
| CPU (host) | 0 % idle, load average 57 to 100 | load 1 to 5 |
| Disk-wait pressure, `some`, 10 s average (host) | 57 to 78 % | under 8 % |
| Memory per node (resident, all ten nodes) | 520 to 640 MB, flat | same |
| Reserve sample on two observer nodes | 8 to 15 minutes, or not done within 25 | 1 min 37 s to 2 min 59 s |

The sample budget is about 570 s at 5-second blocks and about 228 s after #540.

**Estimate (not measured):** about 0.25 to 0.3 ms of CPU per evicted chunk (8 cores divided by about 26,000 to 31,500 chunks/s for the host; an upper bound, because all other work is included). This counts CPU only, while disk wait was also saturated.

**Hypotheses for #650:**
- about 650 chunks/s per node keeps ten evicting nodes to about a quarter of the host's CPU;
- at 500 the node stays responsive and samples finish within budget.

The default rounds down to 500.

## Design

### The default

- `cmd/bee/cmd/cmd.go`: the flag default for `reserve-eviction-rate` becomes 500.
- `c.config.GetInt` (`cmd/bee/cmd/start.go:481`) returns the flag default when the key is unset, and the set value otherwise. So **0 set in the config file, as a flag or in the environment keeps meaning no limit**: `newEvictionPacer` (`pkg/storer/evictionpace.go`) returns nil for 0, as today. No `IsSet` check is needed.
- An empty environment variable counts as unset and gives 500. The help text says so.
- `storer.Options.ReserveEvictionRate` keeps 0 as its zero value (no limit). The default lives only in the command's flag, so tests, the `db` subcommands and other users of `storer.New` keep today's unpaced behaviour. A real node builds `storer.Options` only in `pkg/node/node.go`, from the command's value.
- A negative value stays refused at start (`storer.go:900`).

### What a non-zero rate changes (already true for any explicit rate today)

From `evictionpace.go` and `reserve.go`, at origin/main after #649:
- Each round of at most 1,000 chunks pays for its deletions with rate-limiter tokens after the batch lock is released, and deletes on 2 goroutines instead of `runtime.NumCPU()`.
- **The rate follows arrivals, with a lag.**
  - The effective rate is `max(configured, arrival rate)` (`effective`). Goroutines are added after 3 rounds that miss it, up to one per CPU thread.
  - The arrival rate is averaged against a sample at least 60 s old, and samples survive between episodes. So after an idle period a burst of arrivals at rate A grows the excess by about (A - 500) x 60 s before eviction follows.
  - Above what one goroutine per CPU thread can delete, the excess grows as it does at rate 0.
  - Within those bounds the excess over capacity does not grow while chunks arrive faster than 500/s.
- With #649 a paced eviction pauses before its rate-limiter wait while a sample runs. Time paused is excluded from the episode age and from the pacer's headroom. Nothing in #649 depends on the rate being 0 or not.

### Who it affects

- Only full nodes that are not bootnodes have a reserve (`pkg/node/node.go`, reserve configured only for a full non-boot node), so light nodes, ultra-light nodes and bootnodes are unaffected.
- Operators who set no value get paced eviction after upgrading. Operators who set a value keep it.
- **Bench scripts that change eviction settings must set 0 explicitly when they mean no limit**, because removing the key now gives 500. These are the eviction baseline runner on bench-1 and the radius-change scenario runner on the dense bench host.

### Cost (rule 8)

All durations below assume no arrivals. With arrivals at rate a, the excess drains at about 500 - a chunks/s, and not at all while a >= 500 (the floor then holds it level). Pull-sync can arrive at up to 1,000 chunks/s. **So the durations are lower bounds, and the eviction episode of #649 lasts at least that long.**

1. **Batch-expiry evictions now skip lottery rounds (likely the larger cost; hypothesis).**
   - The #649 episode covers both `unreserve` and `evictExpiredBatches` (`pkg/storer/reserve.go`, both wrapped by `evictionRun`), and the gate fires once an episode has been active for 60 s.
   - At 500/s an episode passes 60 s after about 31,000 chunks (one burst of 1,000 plus 60 s x 500). So **any expired batch with more than about 31,000 chunks on this node makes it sit out the rounds during its eviction.**
   - At rate 0 the threshold is, as a hypothesis, about 160,000 to 190,000 chunks on the dense host (60 s at the measured 2,600 to 3,150 chunks/s) and more on a node that evicts alone (not measured).
   - Expiry is routine, so this probably happens far more often than a radius increase.
   - **Measurement:** the size of each expiry eviction, from `bee_localstore_expired_count` per expiry and the `evict expired batches start` log line, on the bench nodes and a staked node over a week, against the 31,000 threshold.
   - **Open question, not changed here:** whether batch-expiry eviction should count toward the #649 gate at all. It is tracked in [#663](https://github.com/crtahlin/wasp/issues/663); this spec does not change the gate.
2. **Radius-increase evictions take longer.**
   - Half of a default reserve (capacity 4,194,304, about 2 M chunks) takes at least about 70 minutes at 500/s, instead of a few minutes.
   - Half of a reserve at doubling 3 (about 14 to 17 M chunks) takes at least about 8 hours, instead of about 1 h 14 min measured on the dense host.
3. **This node's lottery during a radius increase** (with #649; estimate at committed depth 9, where a neighbourhood is selected about once per 512 rounds of 152 blocks at 5 s):
   - **default node:** about 6 rounds skipped per large eviction instead of under 1, so a missed selection in about 1 % of such evictions;
   - **doubling 3:** about 38 rounds instead of about 5 at rate 0 (1 h 14 min), so **about 33 rounds added**, and a missed selection in about 6 % of such evictions instead of about 1 %.
4. **Disk:** sharky reuses freed slots for new chunks (`pkg/sharky/shard.go`, `slots.go`), so the shard files do not grow per arriving chunk. They stay at their high-water mark for longer, because the excess is deleted more slowly. Pebble frees space only after compaction (#627).
5. **Expired chunks stay longer.**
   - They are safe for the sample result: rejected by the failed stamp load or `validStamp`.
   - But the sampler still reads and hashes them before rejecting them (`pkg/storer/sample.go`).
   - They count in `reserveSizeWithinRadius`.
   - No `unreserve` runs while a long expiry eviction holds the worker.
6. **Other nodes:** for an expired batch, peers that sync from this node fetch and reject its chunks for longer, which costs their bandwidth. In exchange (hypothesis, to be measured in #650), the node stays responsive to them during the eviction.
7. **A single node per host** gains little from pacing and can set 0.

### Help text, template and docs

- **Flag help** (cobra appends "(default 500)"):

  > most chunks per second reserve eviction deletes, raised to the rate chunks arrive at; 0 (set explicitly) means no limit, the behaviour before this default; an empty value counts as unset. Raising it finishes an eviction sooner with more CPU and disk load meanwhile, which on a host with many nodes can make them all slow to serve and miss lottery samples; lowering it keeps the node responsive but keeps evicted and expired chunks on disk longer, makes the node sit out more lottery rounds while it evicts, and makes peers fetch and reject chunks of expired batches for longer

  The wording "0 uses the default" is not used, because here 0 means no limit.
- **Templates:** `packaging/bee.yaml` and its three copies (`homebrew-amd64`, `homebrew-arm64`, `scoop`), and `docs/config-reference.yaml`. The commented line becomes `# reserve-eviction-rate: 500`, with one sentence that 0 removes the limit. The packaging text's "shard files grow about 4 KB per chunk" is replaced by the high-water-mark wording above.
- **`docs/DIFFERENCES.md`:**
  - the "Reserve eviction" behaviour row (around line 60, "at its default of 0 nothing else changes") gets the new default;
  - the `reserve-eviction-rate` settings row gets the new default, the shard wording fix and links to this spec and #650;
  - the paragraph introducing the settings table ("Unless noted, a node that leaves these settings unset behaves as Bee does") gets an explicit exception naming `reserve-eviction-rate`.

## Tests

1. **Real command flags, all three channels.** The read at `start.go:481` moves into a small helper (for example `reserveEvictionRate(c.config)`) that both `start.go` and the test call. The test builds a `command`, registers the real flags with `(*command).setAllFlags` (`cmd/bee/cmd/cmd.go:352`), loads the config through the same viper setup as the start command, and checks:
   - the key unset gives 500;
   - `reserve-eviction-rate: 0` in a config file gives 0;
   - the environment variable set to 0 gives 0;
   - `--reserve-eviction-rate=0` gives 0;
   - an empty environment variable gives 500.

   The value checked is the helper's return value, which is what `start.go` passes to the node options.
2. **Storer:** with 0 no pacer exists (`TestEvictionPacerOffAtZero` already covers `newEvictionPacer`); with 500 a pacer exists.
3. **Eviction keeps up with fast arrivals** (storer level, real time):
   - rate 500, a starting excess of 2,000 chunks over capacity, then arrivals at 2,000/s for 5 s;
   - **fresh samples** (the arrival samples already show about 2,000/s when the burst starts): the excess grows by less than about 3,750 chunks over the burst (half of (2,000 - 500) x 5 s, while the averaged arrival rate catches up);
   - **stale samples** (the arrival samples are older than 60 s when the burst starts): the excess grows by at most (A - 500) x burst duration, 7,500 chunks for 5 s at 2,000/s;
   - after arrivals stop, the reserve reaches capacity within 25 s: the worst case is 2,000 + 7,500 = 9,500 chunks at no less than 500/s, about 19 s, plus margin;
   - the tolerance is stated in the test, which uses the pacer's injectable clock where the arrival window is involved.
4. Existing pacer, eviction and #649 tests pass unchanged.

**Mutation checks** (each must make a test fail):
- the flag default left at 0;
- 0 mapped to 500 in `start.go`;
- a default of 500 applied in `storer.New` or in `node.go` when the value is 0;
- the arrival floor removed from `effective`.

## Measurement

#650 measures, in 60-minute windows on the dense host:
- a **rate-0 arm**;
- 2,000, 1,000 and 500 chunks/s per node.

Each window records sample duration on two observer nodes, local retrieval latency at a modest load, CPU, disk-wait pressure and memory. Pass: samples under 228 s with margin, retrieval p99 under 1 s with no errors.

The default is adjusted if 500 fails, or if a higher rate passes with margin. The expiry-size measurement in cost item 1 runs alongside. Until then the default rests on the estimate above.

## Upstream

Not an upstream defect: upstream has no paced eviction and no such setting. No `affects-upstream` label.
