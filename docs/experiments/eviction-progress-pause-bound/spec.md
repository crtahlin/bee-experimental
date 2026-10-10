# Eviction progress while it runs, and a bound on how long samples can hold eviction off

Issues: [#626](https://github.com/crtahlin/wasp/issues/626) (report eviction progress while it runs, part of [#621](https://github.com/crtahlin/wasp/issues/621)) and [#659](https://github.com/crtahlin/wasp/issues/659) (the eviction pause cap does not bound back-to-back `/rchash` calls, a follow-up from the review of [#653](https://github.com/crtahlin/wasp/pull/653)).

One spec for both because they touch the same code (`pkg/storer/evictionpace.go`, `pkg/storer/evictionsample.go`, the eviction metrics) and are measured in the same eviction run. Either part can ship alone.

## Terms

- **Eviction episode:** an eviction that spans reserve-worker runs, from #649 (`pkg/storer/evictionsample.go`). `EvictingFor()` reports its active time.
- **Pause:** eviction waiting between rounds while a reserve sample runs (#649, `waitWhileSampling`).
- **Stretch:** a continuous period in which at least one sample runs (`samplingCount > 0`).

## Problem

### #626: progress during a long eviction (partly solved since the issue was filed)

When the issue was filed, `bee_localstore_evicted_count` and `bee_localstore_reserve_size` changed only when a whole batch finished, and stayed flat for 15 minutes and more on sw-1.

**Already solved by #623 (paced eviction):** both are now updated after every round of at most 1,000 chunks (`hooks.After` in `pkg/storer/evictionpace.go:243-249`, `r.size.Add` in `pkg/storer/internal/reserve/reserve.go:564`). In the ten-node rerun (scenario A2, 2026-10-09) the evicted counter moved by about 2.6 M in the first 13 minutes on one node, so progress is visible in metrics.

**Still missing:**
- How much is left. `unreserve` knows its target (`pkg/storer/reserve.go:516-522`) but logs it only at the start and at the end (`:522`, `:579`); nothing exposes the remaining count while it runs.
- A log line while it runs. An operator reading the journal sees "unreserve start" and then nothing for hours (at the default 500 chunks/s, a half-capacity eviction on a node with `reserve-capacity-doubling: 3` takes at least about 8 hours).

### #659: back-to-back samples can hold eviction off indefinitely (from code)

The pause cap (`maxEvictionPause`, 15 minutes) applies to one continuous stretch: `samplingSince` is set when the sample count goes from 0 to 1 (`evictionsample.go:49-53`) and `evictionShouldWait` compares against it (`:73-91`). When the count returns to 0 between two samples, the next sample starts a new stretch with a fresh 15 minutes. Eviction polls every 250 ms (`samplingPausePoll`, `:39`), so a gap shorter than one poll may let no round run at all. Back-to-back `/rchash` calls can therefore hold eviction off for as long as they continue, although the #649 spec says the cap prevents that.

Who can cause it: the storage lottery cannot (its sample is cancelled at the next reveal phase, about 570 s, `pkg/storageincentives/agent.go:157`, and runs at most once per round). `/rchash` is a debug-API call bounded only by its request (`pkg/api/rchash.go:125`); bench scripts and monitoring call it, and an operator script calling it in a loop would block eviction while the reserve stays over capacity.

## Hypothesis

1. A remaining-count gauge and a progress line every minute make a long eviction readable from metrics and from the journal alone, at no measurable cost.
2. Bounding the time eviction waits for non-lottery samples over a sliding window guarantees eviction at least half of the time whatever the `/rchash` pattern, while a lottery sample still pauses eviction for its whole duration.

## Design

### 1. Progress (#626)

- **Gauge `bee_localstore_eviction_remaining`:** the reserve's own `EvictionTarget()` (size minus capacity, `internal/reserve/reserve.go:850-855`), read from atomics, set in `hooks.After` and in the wait loop below. Using the target rather than "the run's target minus evicted so far" keeps it right across an expiry interruption (which ends a run early, `pkg/storer/reserve.go:545-553, 567-570`), counts arrivals during the eviction, and does not overshoot by `minEvictCount`. It is 0 when the reserve is within capacity.
- **Gauge `bee_localstore_eviction_expired_batches_remaining`:** the number of expired batches left in an expired-batch run, set at the start and after each batch.
- **Progress line, info level, at most once a minute while an episode runs:** emitted from `hooks.After` and also from the wait loop in `waitWhileSampling`, so a long pause still produces lines. Fields: kind (`unreserve` or `expired`), evicted in this run, remaining (target, or batches left), rate over the last minute, the configured rate, workers, paused time in this run, `paused` (true when written from the wait loop), and for `unreserve` an estimate of the time left (`remaining / rate over the last minute`, omitted while that rate is 0).
- The existing start and end lines stay.

Cost: one gauge set per round and one time comparison per round or poll; about 60 lines an hour per evicting node.

### 2. Bounded waiting for samples other than the lottery (#659)

- **Two kinds of sample.** The lottery's sample (the storage-incentives agent, `makeSample`) is marked as such when it calls `ReserveSample`; every other caller (`/rchash`, tests) is not. The flag is set where the sample registers (`pkg/storer/sample.go:199`, `sampleStarted`), and the storer keeps two counts.
- **A lottery sample always pauses eviction for its whole duration** (it lasts at most about 570 s, cancelled at the next reveal, `pkg/storageincentives/agent.go:157`), exactly as since #649. Without this, earlier `/rchash` samples could use up the budget, and eviction would run during a lottery sample, which #649 exists to prevent. The existing per-stretch cap (15 minutes) stays as the guard against a stuck lottery sample.
- **Other samples share a sliding-window budget.** The quantity bounded is **waited time**: the time eviction actually spent in `waitWhileSampling` because only non-lottery samples were running (`evictionsample.go:98-104` measures it today). The list keeps the closed intervals (start, end) younger than 30 minutes, pruned on every insert, plus the interval in progress, counted up to now. `evictionShouldWait` returns false for non-lottery-only sampling when waited time in the last 30 minutes, including the interval in progress, has reached 15 minutes. An interval that crosses the window's start counts only its part inside the window. Because waited time is what is bounded, eviction gets at least 15 minutes of every 30 to run, whatever the `/rchash` pattern.
- **While the budget is used up,** rounds run between non-lottery samples; once old waiting leaves the window, those samples pause eviction again.
- **Counter and warning:** `bee_localstore_eviction_pause_capped_total` increases on each change from under the budget to at the budget (or past the lottery stretch cap). The Warning is logged at most once per 30 minutes, with the waited time in the window.
- **Expected sample duration on the dense host:** 1 min 37 s to 2 min 59 s with no eviction running, 8 to 15 minutes during the ten-node eviction (scenario A2), and one `/rchash` that ran past 25 minutes. So one long `/rchash` during an eviction can use the whole budget by itself; that is the intended behaviour (eviction then proceeds), and a bench measurement that times a sample during an eviction should note it.

**Alternative considered:** let only the lottery pause eviction, and never `/rchash`. Rejected: bench measurements use `/rchash` during evictions, and #649's barrier also keeps those samples consistent. A bounded budget keeps that and guarantees progress.

**Note for bench scripts:** since #653 an `/rchash` sample during an eviction pauses it, up to the budget. The radius-change runner samples one observer every 10 minutes, so with samples of 8 to 15 minutes during an eviction it can reach the budget; its sample timings must say whether the eviction was paused.

### Configuration

No new setting. `maxEvictionPause` stays a package variable for tests.

## Tests

1. **Remaining gauge:** a reserve above capacity evicting over several rounds (small round size via `reserve.SetEvictionRound`); after each round the gauge equals `EvictionTarget()`, and 0 once within capacity. With an expiry interruption midway, the gauge stays at the true remainder.
2. **Expired batches remaining:** three expired batches; the gauge goes 3, 2, 1, 0.
3. **Progress line:** with the line interval shortened, a run of several rounds logs at least one line with the fields above and no more than one per interval; during a long pause, lines still appear with `paused=true`.
4. **Sliding window, back-to-back `/rchash` samples (the #659 case), deterministic:** the poll interval and the clock are injected (package variables, set by the test), so the test does not depend on 250 ms timing. Non-lottery samples started and finished back to back with gaps shorter than one poll; once waited time in the window reaches the bound, rounds run. On unmodified code no round runs while the samples continue (the reproduction).
5. **Window frees:** after the budget is used and samples stop, old waiting leaves the window and a later non-lottery sample pauses eviction again.
6. **Lottery exempt:** with the budget used up by non-lottery samples, a lottery sample still pauses eviction for its whole duration.
7. **Stuck lottery sample:** a lottery sample longer than the stretch cap; eviction resumes at the cap (no regression from #653).
8. **Counter and warning:** the capped counter rises once per change to the bound; the Warning appears at most once per 30 minutes.
9. **Interval in progress counts:** a single non-lottery sample longer than the bound; eviction resumes when its own waiting reaches the bound, without another interval closing first.

**Mutation checks** (each must make a test fail): gauge from the run's target minus evicted instead of `EvictionTarget()` (test 1); expired-batch gauge not decremented (test 2); line only from `hooks.After` (test 3); bound applied per stretch instead of over the window (test 4); old intervals never leave the window (test 5); lottery samples counted in the budget (test 6); interval in progress not counted (test 9); counter increased on every check (test 8).

## Measurement

In the next eviction run on the dense host (the planned A3, ten nodes at the default 500 chunks/s):
- the remaining gauge falls steadily on every node and reaches 0 when the node is at capacity; the progress line appears about once a minute per node;
- eviction duration within the spread of the previous runs at the same rate (no measurable overhead);
- `eviction_pause_capped_total` and the waited time with the runner's sampling (one `/rchash` per 10 minutes) are recorded; reaching the budget is expected only if those samples run for many minutes each.

## Protocol impact

None.

## Rollout and rollback

No setting; revert of the merge commit.

## Upstream portability

Upstream has neither paced eviction nor the pause, so neither part applies upstream. No `affects-upstream` label.

Generated with help of AI.
