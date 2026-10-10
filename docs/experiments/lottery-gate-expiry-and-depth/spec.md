# Storage lottery: no skip for batch-expiry eviction, and no skip for a radius decrease during a sample

Issues: #663 (batch-expiry eviction should not make the node sit out the lottery) and #658 (the post-sample depth check also skips on a radius decrease). Both refine the lottery gate from #649 (spec `docs/experiments/lottery-skip-while-evicting/spec.md`, merged as 8f5ee64f). Related: #651 (default `reserve-eviction-rate` 500), #696 (gating radius decreases on the puller, implemented in parallel; this spec does not touch the decrease logic in `pkg/storer/reserve.go`).

**One spec for both.** Both change when a selected node sits out a round, both are stake-risk, and both rest on the same reasoning about which reserve changes alter a sample's content. One review and one measurement cover them. The code changes are separate (the episode in `pkg/storer/evictionsample.go` for #663, the post-sample check in `pkg/storageincentives/agent.go` for #658), so the implementation PR keeps them in separate commits.

## Terms

- **Episode, evicting, round, sample:** as in the #649 spec. `EvictingFor()` (`pkg/storer/evictionsample.go`) returns the episode's active time; the agent sits out a round when it is at least `EvictingMinAge` (60 s).
- **Unreserve run:** a reserve-worker run of `unreserve` (radius increase, `pkg/storer/reserve.go:502`).
- **Expiry run:** a reserve-worker run of `evictExpiredBatches` (`reserve.go:323`).
- **Depth read:** the committed depth the agent reads at the top of `handleSample` (`agent.go:434`) and uses for selection and the sample.

## Problem

### #663: an expiry run counts as evicting

`evictionRun` (`evictionsample.go`) wraps both kinds of run (`reserve.go:264, 283`), so an expiry run opens and extends the episode exactly as an unreserve run does. With the default rate of 500 chunks/s (#651) one minute of eviction is about 31,000 chunks (the limiter burst of 1,000 plus 60 s × 500), so any expired batch with more than about 31,000 chunks on the node makes it sit out the round. At rate 0 that threshold was 160,000 to 190,000 chunks on the dense bench host (measured rates of 2,600 to 3,150 chunks/s). Batches expire routinely on mainnet, so this is likely the larger lottery cost of #649 and #651 together (hypothesis; the measurement below counts it).

### #658: a radius decrease skips the round

After the sample, the agent compares the committed depth with the depth read and skips the round if they differ (`agent.go:495-498`, added in #649). A decrease is not behind the eviction barrier and can land at any time, including during a sample: the puller is paused while a sample runs (`pkg/puller/puller.go:420`), so its sync rate can fall to 0 and the reserve worker's decrease condition (`reserve.go:311`) can hold. The agent then skips a round it would have played before #649. The log line "changed during the sample" is also wrong for a step that landed before the sample started. The skip has no metric.

## What changes a sample's content (reasoned from code; test 1 verifies the expiry part)

A played round has `proximity(overlay, anchor) >= depth read` (the contract's `IsPlaying` selects by that), so `sampleBins` (`pkg/storer/sample.go:654-662`) returns the range `[max(depth read, storage radius), MaxPO]`, and the iteration also requires `proximity(chunk, anchor) >= depth read` (`sample.go:263`).

1. **Expiry eviction does not change the content.** `batchstore.cleanup` (`pkg/postage/batchstore/store.go:300-345`) first calls `evictFn`, which only records an `expiredBatchItem` and wakes the reserve worker (`reserve.go:412-426`), and then deletes the batch from the batch store. From then on every remaining chunk of that batch fails `validStamp` in phase 3 of the sample (`sample.go:493`, `postage.ValidStamp` returns not found, counted as `InvalidStamp`), whether the expiry run has deleted it yet or not. Before `cleanup` runs, the batch's value is at or below the cumulative payout, which is below the agent's `minBatchBalance` (`agent.go:568-575`, total payout plus the price for the blocks up to the next round), so `batchesBelowValue` (`sample.go:542`) already excludes it. Edge: with a current price of 0 the two values are equal and `batchesBelowValue` (strict `<`) keeps the batch; it is then still in the batch store, its chunks pass `validStamp`, and they are sampled consistently until `cleanup` deletes it, so this edge is also independent of eviction. Expiry eviction costs the sample time (the chunks are read and hashed before the stamp check), which the pause covers.
2. **An increase can change the content on a default node.** With no doubling, the depth read equals the radius r, so the range starts at bin r. A step to r+1 lets unreserve delete in bin r (the #649 barrier stops that during the sample, not before it), and a sample that starts after the step reads radius r+1 and starts at bin r+1. On a doubled node the deleted bin is below the range.
3. **A decrease does not change the content.** It deletes nothing. The puller then pulls chunks in bin r−1 and below; such a chunk shares fewer than r bits with the overlay, while every chunk in range shares at least the depth read (≥ r) bits with it, so none of them is in range. The sample at the depth read is the one the node was selected for.

So: expiry runs should not count toward the gate; a decrease should not skip; an increase should, including an increase followed by a decrease that brings the depth back to the depth read (bin r chunks are gone, and the current comparison would miss it).

## Hypothesis

1. Counting only unreserve runs toward the episode removes every expiry-driven skip and keeps every skip for a radius increase.
2. Skipping only when the radius rose between the depth read and the end of the sample keeps the round for a decrease and still skips every sample whose content an increase could have changed, including increase-then-decrease.

## Design

### 1. Expiry runs do not count toward the episode (#663)

- `evictionRun` gains a kind: `evictionRun(kindUnreserve, ...)` and `evictionRun(kindExpiry, ...)` at the two call sites (`reserve.go:283` and `:264`). Nothing else in `reserve.go` changes.
- **Unreserve runs** behave as today: open an episode if none is open, add active time, close it when no work is left.
- **Expiry runs** never open an episode and add no active time. If an episode is open (an unreserve run left work, for example because a batch expiry interrupted it, `reserve.go:551, 568, 589`), the expiry run keeps it open: it sets `lastRunEnd` at its end so the idle grace counts from there, and it does not close it.
- **"Work left" for the episode** becomes `EvictionTarget() > 0` only (the reserve is over capacity). Pending expired batches no longer keep an episode open. `evictionWorkLeft` keeps its current meaning for anything else that uses it; the episode uses a new helper.
- **The pause during a sample** (`waitWhileSampling`, the `Before` hook, the barrier) applies to expiry runs unchanged, so an expiry run still does not compete with a running sample.
- **Shutdown** closes the episode as today.
- **Metrics:** `bee_localstore_expiry_eviction_seconds_total` (counter, active seconds of expiry runs, paused time excluded) and `bee_localstore_expiry_eviction_running_seconds` (`GaugeFunc`, active seconds of the expiry run in progress, 0 when none), so expiry eviction stays visible now that it no longer shows in `bee_localstore_eviction_running_seconds`.

### 2. Skip after a sample only on a radius increase (#658)

- The storer counts radius increases: `pkg/storer/internal/reserve` `SetRadius` compares the new radius with the stored one and increments an atomic counter when it is higher. This covers the unreserve step (`reserve.go:587-597`) without touching the decrease branch. The start-up `SetRadius` (`startReserveWorkers`) runs before the agent and is not relevant.
- `storer.Reserve` gains `RadiusIncreases() uint64`; the mock reserve implements it (its `SetStorageRadius` counts a higher radius the same way).
- `handleSample` reads `RadiusIncreases()` together with the depth at the top. After the sample:
  - if `RadiusIncreases()` changed: skip, log `skipping round because the storage radius increased since the round's depth was read` with the depth read and the current depth, and count `bee_storageincentives_skipped_depth_changed_total{direction="increase"}`;
  - else if the committed depth is lower than the depth read: play, log at info `radius decreased since the round's depth was read; playing at the depth read`, and count `{direction="decrease"}` in a separate counter `bee_storageincentives_played_after_radius_decrease_total`, so a skip and a play are not mixed in one series;
  - otherwise play as today.
- Both counters are created at start so they export 0.
- The comparison is on the increase counter, not on the depth, so increase-then-decrease is caught.

## Tests

Storer (`pkg/storer`):

1. **Expired-batch chunks give the same sample whether evicted or not.** A real storer (`memStorer`, the real reserve) with chunks of a valid batch A and a batch E in range. Take sample S1 with both batches valid. Then expire E the way `cleanup` does: record it with `EvictBatch` and delete it from the batch store, without letting the reserve worker run. Take S2. Then run the expiry eviction of E. Take S3. Assert S2 == S3 (same hash and items) and that neither contains an item of E, and that S1 contains at least one item of E (so the test exercises the case). A second case uses a batch whose value is below `minBatchBalance` while still in the batch store: excluded before and after eviction.
2. **An expiry run alone never makes the node evicting.** An expiry run that lasts longer than `EvictingMinAge` (fake clock through the episode's `now` arguments, as the #649 tests do): `EvictingFor()` stays 0 throughout.
3. **An expiry run inside an open episode keeps it open without adding time.** Unreserve run (30 s active, work left), expiry run (90 s), unreserve run (40 s): the episode is open throughout, `EvictingFor()` is 70 s after the last run, and it never exceeds the unreserve time.
4. **Pending expired batches do not keep an episode open.** Unreserve run ends with the reserve within capacity while an `expiredBatchItem` is recorded: the episode closes.
5. **Expiry eviction still pauses for a sample.** The existing #649 pause test, run with an expiry run.
6. **Increase counter:** `SetRadius` to a higher radius increments, to the same or a lower radius does not.

Agent (`pkg/storageincentives`):

7. **Decrease during the sample: the round is played.** The sample hook lowers the radius by one; the agent commits, the played-after-decrease counter is 1, the skip counter is 0.
8. **Increase during the sample: skipped** (the existing `TestAgentSkipsRoundWhenDepthChangesInSample`), now also asserting `skipped_depth_changed_total{direction="increase"}` is 1.
9. **Increase then decrease during the sample: skipped,** although the committed depth is back to the depth read.
10. **Series exported at 0** before any skip or decrease.

**Mutation checks** (each must make a test fail):

- expiry runs counted toward the episode (test 2);
- expiry runs adding active time inside an open episode (test 3);
- expiry run closing an open episode (test 3);
- episode "work left" still counting pending expired batches (test 4);
- pause skipped for expiry runs (test 5);
- skip on any depth change, decreases included (test 7);
- comparison on the committed depth instead of the increase counter (test 9);
- `SetRadius` counting every call (test 6);
- counters not incremented, or not created at start (tests 8 and 10).

Test 1 is a verification, not a mutation target: if it fails, the reasoning behind #663 is wrong and the change must not ship.

## Measurement

On the ten-node bench host, with the build that carries this change, during the planned eviction-and-refill cycle (A3/B3):

- **Expiry:** count expiry runs and their size (`evict expired batches start` log line, `bee_localstore_expired_count` deltas) and `expiry_eviction_seconds_total`. Pass: `EvictingFor()` and `bee_localstore_eviction_running_seconds` stay 0 during expiry runs that are not inside an unreserve episode.
- **Increase:** during A3 (all nodes evict at the default 500), the gate still fires: `bee_storageincentives_skipped_while_evicting` counts any selection that falls inside the unreserve episode (the bench nodes are unstaked and normally not selected, so the gate is checked through `EvictingFor()` and the episode gauge rather than through skips).
- **Decrease:** during B3 (refill), any sample that overlaps a decrease is counted in the played-after-decrease counter. The bench observers take `/rchash` samples, which do not go through the agent, so on the bench this counter is expected to stay 0; it is a production-node metric.
- Rule 7: report each series per node with the window it covers.

## Upstream

Neither issue applies to upstream as such: the episode and the post-sample check exist only in wasp (#649). The verification in test 1 (expired-batch chunks are excluded whether or not evicted) is upstream behaviour and needs no change there. No `affects-upstream` label.

## Scope

- No wire change (rule 6), no new setting.
- Not in scope: the radius-decrease condition itself (#696), back-to-back `/rchash` calls and the pause cap (#659), the eviction rate default (#651, #650).

## Files

- `pkg/storer/evictionsample.go` (run kind, episode work-left helper, expiry metrics)
- `pkg/storer/reserve.go` (the two `evictionRun` call sites only)
- `pkg/storer/internal/reserve/reserve.go` (`SetRadius` increase counter)
- `pkg/storer/storer.go`, `pkg/storer/metrics.go` (interface method, metrics)
- `pkg/storer/mock/mockreserve.go` (`RadiusIncreases`)
- `pkg/storageincentives/agent.go`, `pkg/storageincentives/metrics.go`
- tests in `pkg/storer` and `pkg/storageincentives`
- `docs/DIFFERENCES.md` (the #649 row updated)

Generated with help of AI.
