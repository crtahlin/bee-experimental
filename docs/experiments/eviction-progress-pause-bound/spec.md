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

**Already solved by #623 (paced eviction):** both are now updated after every round of at most 1,000 chunks (`hooks.After` in `pkg/storer/evictionpace.go:243-249`, `r.size.Add` in `pkg/storer/internal/reserve/reserve.go:561`). In the ten-node rerun (scenario A2, 2026-10-09) the evicted counter moved by about 2.6 M in the first 13 minutes on one node, so progress is visible in metrics.

**Still missing:**
- How much is left. `unreserve` knows its target (`pkg/storer/reserve.go:516-522`) but logs it only at the start and at the end (`:522`, `:579`); nothing exposes the remaining count while it runs.
- A log line while it runs. An operator reading the journal sees "unreserve start" and then nothing for hours (at the default 500 chunks/s, a half-capacity eviction on a node with `reserve-capacity-doubling: 3` takes at least about 8 hours).

### #659: back-to-back samples can hold eviction off indefinitely (from code)

The pause cap (`maxEvictionPause`, 15 minutes) applies to one continuous stretch: `samplingSince` is set when the sample count goes from 0 to 1 (`evictionsample.go:49-53`) and `evictionShouldWait` compares against it (`:73-91`). When the count returns to 0 between two samples, the next sample starts a new stretch with a fresh 15 minutes. Eviction polls every 250 ms (`samplingPausePoll`, `:39`), so a gap shorter than one poll may let no round run at all. Back-to-back `/rchash` calls can therefore hold eviction off for as long as they continue, although the #649 spec says the cap prevents that.

Who can cause it: the storage lottery cannot (its sample is cancelled at the next reveal phase, about 570 s, `pkg/storageincentives/agent.go:157`, and runs at most once per round). `/rchash` is a debug-API call bounded only by its request (`pkg/api/rchash.go:125`); bench scripts and monitoring call it, and an operator script calling it in a loop would block eviction while the reserve stays over capacity.

## Hypothesis

1. A remaining-count gauge and a progress line every minute make a long eviction readable from metrics and from the journal alone, at no measurable cost.
2. Bounding the pause over a sliding window guarantees eviction at least half of the time whatever the sampling pattern, and changes nothing for the lottery.

## Design

### 1. Progress (#626)

- **Gauge `bee_localstore_eviction_remaining`:** for `unreserve`, `target - evicted so far` in the current run, updated in `hooks.After` (per round) and set to 0 when the run ends. For an expired-batch eviction the total is not known in advance (counting it would need a scan), so the gauge is not used there; a second gauge `bee_localstore_eviction_expired_batches_remaining` gives the number of expired batches left in the run, set at the start and after each batch.
- **Progress line, info level, at most once a minute while an episode runs**, from `hooks.After` (no extra goroutine): kind (`unreserve` or `expired`), evicted in this run, remaining (or batches left), the rate over the last minute, the configured rate, workers, paused time in this run, and for `unreserve` an estimate of the time left (`remaining / rate over the last minute`, omitted while that rate is 0).
- The existing start and end lines stay.

Cost: one gauge set per round and one time comparison per round; the line at most once a minute.

### 2. Pause bound over a sliding window (#659)

Replace the per-stretch cap with a bound on paused time in a sliding window:

- Eviction may be held off by samples for at most `maxEvictionPause` (15 minutes) within any window of `2 × maxEvictionPause` (30 minutes). A short list of pause intervals (start, end) younger than the window is kept under `samplingMu`; `evictionShouldWait` returns false when the sum of paused time in the window has reached the bound, and true again once old pauses fall out of the window.
- A single long stretch is covered by the same rule (it reaches 15 minutes of pause), so the existing per-stretch cap is subsumed; `samplingSince` stays for the log line.
- When the bound is reached: the existing `bee_localstore_eviction_pause_capped_total` counter increases once per window in which the bound was reached, and the existing Warning is logged once per window, with the paused time in the window.
- While the bound is reached, rounds run between samples as they do after the per-stretch cap today; the barrier (`evictionBarrier`) still keeps a round from starting while a sample is reading the radius only up to the bound.
- **The lottery is unaffected:** a lottery sample lasts at most about 570 s, and a node that is evicting sits out the lottery (#649), so it never reaches the bound through the lottery.

**Alternative considered:** let only lottery samples pause eviction, and not `/rchash`. Rejected: the bench measurements use `/rchash` to time samples during an eviction, and #649's barrier also protects the consistency of those samples. The sliding window keeps that and still guarantees progress.

**Note for bench scripts:** since #653, an `/rchash` sample during an eviction pauses that eviction (up to the bound). A sample time measured during an eviction now measures a sample with the eviction paused, not one competing with it. The radius-change runner samples one observer every 10 minutes, which stays far below the bound.

### Configuration

No new setting. `maxEvictionPause` stays a package variable for tests.

## Tests

1. **Remaining gauge:** an `unreserve` with a target of several rounds (small round size via `reserve.SetEvictionRound`); after each round the gauge equals target minus evicted, and 0 at the end.
2. **Expired batches remaining:** three expired batches; the gauge goes 3, 2, 1, 0.
3. **Progress line:** with the line interval shortened, a run of several rounds logs at least one progress line with the fields above, and no more than one per interval.
4. **Sliding window, back-to-back samples (the #659 case):** with a shortened bound, samples started and finished back to back with gaps shorter than one poll; eviction makes progress (rounds run) once the paused time in the window reaches the bound. On unmodified code no round runs while the samples continue (the reproduction).
5. **Window frees:** after the bound is reached and samples stop, old pauses leave the window and a later sample pauses eviction again.
6. **Single long stretch:** one sample longer than the bound; eviction resumes at the bound, as with today's cap (no regression).
7. **Counter and Warning once per window:** the capped counter rises by exactly 1 per window in which the bound is reached.

**Mutation checks** (each must make a test fail): gauge not updated per round (test 1); expired-batch gauge not decremented (test 2); progress line not rate-limited (test 3); bound applied per stretch instead of over the window (test 4); old pauses never leave the window (test 5); counter increased on every check (test 7).

## Measurement

In the next eviction run on the dense host (the planned A3, ten nodes at the default 500 chunks/s):
- the remaining gauge falls steadily on every node and reaches 0 when the node is at capacity; the progress line appears about once a minute per node;
- eviction duration within the spread of the previous runs at the same rate (no measurable overhead);
- `eviction_pause_capped_total` stays 0 with the runner's sampling (one sample per 10 minutes).

## Protocol impact

None.

## Rollout and rollback

No setting; revert of the merge commit.

## Upstream portability

Upstream has neither paced eviction nor the pause, so neither part applies upstream. No `affects-upstream` label.

Generated with help of AI.
