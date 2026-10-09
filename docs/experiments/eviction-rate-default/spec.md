# Default eviction rate: 500 chunks per second instead of no limit

Issue: [#651](https://github.com/crtahlin/wasp/issues/651). Related: [#623](https://github.com/crtahlin/wasp/issues/623) (paced eviction, where the setting comes from), [#649](https://github.com/crtahlin/wasp/issues/649) (sit out the lottery while evicting), [#650](https://github.com/crtahlin/wasp/issues/650) (the measurement that confirms or adjusts this default), [#540](https://github.com/crtahlin/wasp/issues/540) (2-second blocks).

## Decision

The operator decided on 2026-10-09 to give `reserve-eviction-rate` a non-zero default now, from the estimate below, and to re-measure later with #650. Rule 8 normally asks for the measurement first. This spec records that the order is reversed on purpose and that #650 stays planned. Only the default changes; the setting and its behaviour exist since #623.

## Problem (measured)

Ten nodes with `reserve-capacity-doubling: 3` on one 8-core host were switched to half capacity at once on the paced-eviction build, at today's default 0 (no limit). Each evicted about 14 M chunks.

| Measure | During the eviction | Before or after |
|---|---|---|
| Eviction rate | about 2,600 chunks/s per node, about 26,000/s for the host | - |
| Duration | 1 h 14 min | - |
| CPU | 0 % idle, load average about 100 | load about 1 to 5 |
| Disk-wait pressure (10 s) | 57 to 78 % | under 8 % |
| Memory per node | 520 to 640 MB, flat | same |
| Reserve sample on an observer node | 8 to 15 minutes, or not done within 25 | 1 min 37 s to 2 min 59 s |

The sample budget is about 570 s at 5-second blocks and about 228 s after #540.

**Estimate (not measured):** about 0.3 ms of CPU per evicted chunk (8 cores divided by 26,000 per second; an upper bound, because all other work on the host is included). About 650 chunks/s per node keeps ten evicting nodes to about a quarter of such a host. The default rounds down to 500.

## Design

### The default

- `cmd/bee/cmd/cmd.go`: the flag default for `reserve-eviction-rate` becomes 500.
- `c.config.GetInt` (`cmd/bee/cmd/start.go:481`) returns the flag default when the key is unset, and the configured value when it is set, including an explicit 0. So **0 set in the config file, flag or environment keeps meaning no limit** with no new code: `newEvictionPacer` (`pkg/storer/evictionpace.go`) returns nil for 0, as today. No `IsSet` check is needed.
- `storer.Options.ReserveEvictionRate` keeps 0 as its zero value (no limit). The default lives only in the command's flag, so tests and other users of `storer.New` that do not set the field keep today's unpaced behaviour.
- A negative value stays refused at start (`storer.go:900`).

### What a non-zero rate changes (already true for any explicit rate today)

From `evictionpace.go` and `reserve.go`, at origin/main after #649:
- Each round of at most 1,000 chunks pays for its deletions with rate-limiter tokens after the batch lock is released, and deletes on 2 goroutines instead of `runtime.NumCPU()`.
- **The rate follows arrivals.** The effective rate is `max(configured, arrival rate)` over the last minute (`effective`), with goroutines added after 3 rounds that miss it, up to one per CPU thread. So when chunks arrive faster than 500/s, eviction keeps up and the excess over capacity does not grow. `TestEvictionPacerArrivalFloor` already checks the effective rate; this change adds a test at the storer level (below).
- With #649, a paced eviction pauses before its rate-limiter wait while a sample runs; time paused is excluded from the episode age and from the pacer's headroom. Nothing in #649 depends on the rate being 0 or not.

### Who it affects

- Only full nodes that are not bootnodes have a reserve (`pkg/node/node.go:1180`), so light nodes, ultra-light nodes and bootnodes are unaffected.
- Operators who set no value get paced eviction after upgrading. Operators who set a value keep it.
- **Bench scripts:** any script that removes the key to mean "no limit" must now set 0 explicitly.

### Cost (rule 8)

- **This node:** a large eviction takes longer. Half of a default reserve (capacity 4,194,304, about 2 M chunks) takes about 70 minutes at 500/s instead of a few minutes. Half of a reserve at doubling 3 (about 14 to 17 M chunks) takes about 8 hours instead of about 1.5. Meanwhile the shard files grow about 4 KB per chunk that arrives, reclaimed only by compaction, and the reported reserve size stays above capacity.
- **This node's lottery:** with #649 the node sits out a round while an eviction episode lasts more than a minute. Estimate: on a default node about 6 rounds per large eviction, a missed selection in about 1 % of such evictions; at doubling 3 about 38 rounds and about 7 %.
- **Other nodes:** for an expired batch, peers that sync from this node fetch and reject its chunks for longer, which costs their bandwidth. In exchange, the node stays responsive to them during the eviction.
- **A single node per host** gains little from pacing and can set 0.

### Help text, template and docs

- Flag help: state the default, that it is raised to the arrival rate, that 0 means no limit, and the trade-off above in the rule 8 format.
- `packaging/bee.yaml` and the three copies (`homebrew-amd64`, `homebrew-arm64`, `scoop`): the commented line becomes `# reserve-eviction-rate: 500`, with one sentence that 0 removes the limit.
- `docs/DIFFERENCES.md`: the settings row (`reserve-eviction-rate`) gets the new default and the reason, linking this spec and #650.

## Tests

1. **Flag default:** the command's flag set reports 500 for `reserve-eviction-rate`.
2. **Explicit 0 means no limit:** with the key set to 0 in the config, the value passed to the node is 0 and the storer has no pacer (`evictionPacer == nil`). With the key unset, the value is 500 and a pacer exists.
3. **Eviction keeps up with fast arrivals:** a storer with rate 500 whose reserve receives chunks faster than 500/s while over capacity ends at or below capacity once arrivals stop; the excess does not grow while they continue (bounded by about one round plus the arrivals of one arrival window).
4. Existing pacer, eviction and #649 tests pass unchanged.

**Mutation checks** (each must make a test fail): flag default left at 0; `newEvictionPacer` treating 0 as the default instead of no limit; the arrival floor removed from `effective`.

## Measurement

#650 measures 2,000, 1,000 and 500 chunks/s per node in 60-minute windows on the dense host, with sample duration on two observer nodes, local retrieval latency at a modest load, CPU, disk-wait pressure and memory. Pass: samples under 228 s with margin, retrieval p99 under 1 s with no errors. The default is adjusted if 500 fails or if a higher rate passes with margin. Until then the default rests on the estimate above.

## Upstream

Not an upstream defect: upstream has no paced eviction and no such setting. No `affects-upstream` label.
