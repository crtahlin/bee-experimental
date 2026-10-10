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

1. **Expiry eviction does not change the content.** `batchstore.cleanup` (`pkg/postage/batchstore/store.go:298-343`, under `s.mtx`) first calls `evictFn`, which only records an `expiredBatchItem` and wakes the reserve worker (`reserve.go:412-426`), and then deletes the batch from the batch store. From then on every remaining chunk of that batch fails `validStamp` in phase 3 of the sample (`sample.go:493`, `postage.ValidStamp` returns not found, counted as `InvalidStamp`), whether the expiry run has deleted it yet or not. Before `cleanup` runs, the batch's value is at or below the cumulative payout, which is below the agent's `minBatchBalance` (`agent.go:568-575`, total payout plus the price for the blocks up to the next round), so `batchesBelowValue` (`sample.go:542`) already excludes it. Edge: with a current price of 0 the two values are equal and `batchesBelowValue` (strict `<`) keeps the batch; it is then still in the batch store and its chunks are sampled consistently until `cleanup` deletes it, so the edge does not depend on eviction (and with price 0 nothing expires). Expiry eviction costs the sample time (the chunks are read and hashed before the stamp check), which the pause covers.
2. **An increase can change the content.** Let D be the depth read, d the doubling, r = D − d the radius, and p the proximity of the overlay to the round's anchor. `sampleBins` (`sample.go:240-246, 654-662`) gives bins D..MaxPO when p ≥ D, and only bin p when p < D; either range is then clipped to bins at or above the current radius. An increase to r+1 lets unreserve delete in bin r. With no doubling (D = r) the range starts at bin r, so the deletion is inside it. With doubling and p = r the range is bin r alone, so after the increase it is clipped away and the sample is empty. So an increase can change the sample in both configurations; the skip on any increase stays.
3. **A decrease cannot reach the range.** It deletes nothing, and the puller then adds chunks only in bins below the old radius r. The range lies at or above bin p (p < D) or bin D (p ≥ D). For a decrease to add a chunk in range would need p < r. **Assumption (hypothesis, from the contract's selection rule as wasp uses it in `IsPlaying(ctx, D)`, not verified against the contract source here):** a selected node has p ≥ D − d = r, so every chunk the decrease brings in is outside the range, and the sample at the depth read is the one the node was selected for.
4. **Neighbours agree on an expiry whatever their eviction state.** A neighbour whose batch store has not yet run `cleanup` still excludes the batch through `minBatchBalance`, which projects to the next round (`agent.go:568-575`), so its sample matches. Neighbours can still disagree for reasons unrelated to eviction (a lagging batch store, other content). So sitting out a round for an expiry buys nothing.

So: expiry runs should not count toward the gate; a decrease should not skip; an increase should, including an increase followed by a decrease that brings the depth back to the depth read (bin r chunks are gone, and the current comparison would miss it).

## Hypothesis

1. Counting only unreserve runs toward the episode removes every expiry-driven skip and keeps every skip for a radius increase.
2. Skipping only when the radius rose (the increase count changed) between the depth read and the end of the sample keeps the round for a decrease and still skips every sample whose content an increase could have changed, including increase-then-decrease.

## Design

### 1. Expiry runs do not count toward the episode (#663)

- `evictionRun` gains a kind: `evictionRun(kindUnreserve, ...)` at the unreserve call site (`reserve.go:283`) and `evictionRun(kindExpiry, ...)` at the expiry call site (`:264`). Nothing else in `reserve.go` changes.
- **Unreserve runs** behave as today: `runStarted` opens an episode if none is open, active time accrues, `runEnded` closes it when no work is left.
- **Expiry runs** never open an episode and add no active time. They use a **hold** instead of a run:
  - `holdStarted(now)`: if an episode is open, mark it held (`holdStart = now`); if none is open, do nothing.
  - `holdEnded(now)`: clear the hold and set `lastRunEnd = now`, so the idle grace counts from the end of the expiry run; the episode stays open whatever the outcome (an expiry run never closes it).
  - `expiredLocked` treats a held episode as busy (like a run in progress): it does not close it, however long the hold.
  - **`EvictingFor()` during a hold** returns the episode's accumulated active time, frozen: the hold adds nothing. So an episode interrupted by an expiry neither closes nor advances toward `EvictingMinAge` while the expiry runs.
  - A shutdown during a hold closes the episode, as for a run.
- **"Work left" for the episode** becomes `EvictionTarget() > 0` only (the reserve is over capacity). `evictionWorkLeft` has no other caller (`evictionsample.go`), so it is replaced by `episodeWorkLeft`, not kept beside it. Pending expired batches no longer keep an episode open.
- **The pause during a sample** (`waitWhileSampling`, the `Before` hook, the barrier) applies to expiry runs unchanged.
- **Metrics,** in the existing `bee_localstore_eviction_*` family, with the `_total` suffix on counters as in the sibling metrics:
  - `bee_localstore_eviction_expiry_seconds_total`: active seconds of expiry runs. Expiry runs keep their own run start and paused time (the same accounting as `runPaused`/`pauseStart`, `evictionsample.go:207-232`, kept in separate fields), so time paused for a sample is excluded;
  - `bee_localstore_eviction_expiry_running_seconds` (`GaugeFunc`): active seconds of the expiry run in progress, 0 when none.
- **Progress lines (#705):** spec #705 logs progress "while an episode runs", with kind `expired` for expiry runs. After this change an expiry run outside an unreserve episode has no episode, so #705's progress line must key on "an eviction run is in progress" (unreserve or expiry), not on the episode. This is recorded here and in a comment on #705; whichever lands second adapts.

### 2. Skip after a sample only on a radius increase (#658)

- The storer counts radius increases. In `pkg/storer/internal/reserve` `SetRadius`, the radius and the counter are kept in **one atomic `uint64`**: radius in the low 8 bits, the increase count in the rest. `SetRadius` does a compare-and-swap loop that stores the new radius and, when it is higher than the old, increments the count in the same word. `Radius()` reads the low 8 bits. One word removes any interleaving between "radius changed" and "counter changed". The unreserve step (`reserve.go:587-597`) goes through `SetRadius`; no path changes the radius without it. The start-up `SetRadius` runs before the agent.
- `storer.Reserve` gains `RadiusState() (radius uint8, increases uint64)`, read from that one word; `CommittedDepth()` is unchanged. The mock reserve implements it the same way.
- `handleSample` reads `RadiusState()` first, at the top, and computes the depth read from that radius plus the doubling, in place of the separate `CommittedDepth()` call (`agent.go:434`). After the sample it reads `RadiusState()` again:
  - if the increase count changed: skip, log `skipping round because the storage radius increased since the round's depth was read` with the depth read and the current depth, and count `bee_storageincentives_skipped_radius_increase_total`;
  - else if the radius is lower than at the read: play, log at info `radius decreased since the round's depth was read; playing at the depth read`, and count `bee_storageincentives_played_after_radius_decrease_total`;
  - otherwise play as today.
- No label on either counter (each has one meaning). Both are created at start so they export 0.

## Tests

Storer (`pkg/storer`):

1. **Expired-batch chunks give the same sample whether evicted or not (verification).** A real storer (`memStorer`) with the **real batch store**: `batchstore.New` over a test state store, its `evictFn` wired to `db.EvictBatch` as in `pkg/node/node.go:713-719, 1200`, and the default `postage.ValidStamp(batchStore)` (no accept-all). Two batches, A and E, each with a real owner key, saved through the batch store, and chunks stamped with real signatures (`postagetesting` signer). E's chunks are placed in the anchor's range deterministically: the anchor is the overlay with the sampled bits fixed, and E's chunks are mined to share at least the committed depth with it, so S1 is guaranteed to contain an E item (asserted). Steps:
   - S1 with both batches valid;
   - expire E through `PutChainState` with a total amount above E's value, so `cleanup` runs its real order (evict at `store.go:328`, delete at `:336`); the reserve worker is not started, so nothing is deleted yet; S2;
   - start the reserve worker and wait for the expiry eviction of E to finish; S3.

   Assert S2 == S3 (hash and items), that neither contains an E item, and that S1 does. A second case: a batch whose value is below `minBatchBalance` but above the total amount (still in the batch store) is excluded before and after its eviction. If test 1 fails, the reasoning behind #663 is wrong and the change must not ship.
2. **An expiry run alone never makes the node evicting.** An expiry hold longer than `EvictingMinAge` with no open episode (fake clock through the episode's `now` arguments, as the #649 tests do): `EvictingFor()` stays 0.
3. **A hold keeps an open episode open without adding time.** Unreserve run (30 s active, work left), then an expiry hold of 90 s, then an unreserve run of 40 s. `EvictingFor()` is called **during the hold** at 15 s and at 80 s (both past the 10 s idle grace) and returns 30 s both times (open, frozen); after the last run it returns 70 s.
4. **Pending expired batches do not keep an episode open.** An unreserve run ends with the reserve within capacity while an `expiredBatchItem` is recorded: the episode closes.
5. **Expiry eviction still pauses for a sample.** The existing #649 pause test, run with an expiry run.
6. **Radius word:** `SetRadius` to a higher radius increments the count and stores the radius in the same word; to the same or a lower radius it stores the radius and leaves the count; concurrent `SetRadius` and `RadiusState` under `-race` never return a radius and count from different writes (a reader sees either the old pair or the new pair).
7. **Worker level: an expiry eviction leaves `EvictingFor()` at 0.** As `TestEvictingForFollowsTheWorker` (`evictionsample_storer_test.go:338`), with a pacing rate so the eviction takes seconds: a batch expiry through `EvictBatch` on a reserve within capacity; while the expiry eviction runs (the expiry running gauge above 0), `EvictingFor()` is 0 throughout.

Agent (`pkg/storageincentives`):

8. **Decrease during the sample: the round is played.** A one-shot sample hook lowers the radius by one on the first sample only (the mock contract's `Reveal` fails from the second round, `agent_test.go:360-361`, so the test stops after the first commit). The agent commits; the played-after-decrease counter is 1; the skip counter is 0.
9. **Increase during the sample: skipped** (the existing `TestAgentSkipsRoundWhenDepthChangesInSample`), now also asserting `skipped_radius_increase_total` is 1.
10. **Increase then decrease during the sample: skipped,** although the radius is back where it was at the read.
11. **Series exported at 0** before any skip or decrease.

**Mutation checks** (each must make a test fail):

- kinds swapped at the two call sites in `reserve.go` (tests 7 and the #649 worker test);
- expiry runs opening the episode (test 2);
- a hold adding active time (test 3);
- `expiredLocked` ignoring a hold (test 3, the call at 80 s);
- a hold closing an open episode (test 3);
- `episodeWorkLeft` still counting pending expired batches (test 4);
- pause skipped for expiry runs (test 5);
- skip on any depth change, decreases included (test 8);
- comparison on the radius instead of the increase count (test 10);
- `SetRadius` counting every call, or storing radius and count in two words (test 6);
- counters not incremented, or not created at start (tests 9 and 11).

## Measurement

Rule 7 asks for three runs per condition; where a condition happens once per cycle, the cycle is the run and three cycles are needed before the default is judged.

- **Expiry, on the bench:** the planned A3/B3 cycle does not by itself expire a batch. Plan one: buy a short-lived batch on the bench's chain backend (a TTL of a few hours), upload enough chunks that its share on a node exceeds 31,000, and let it expire during a quiet period. Three such expiries. Record the `evict expired batches start` line, `bee_localstore_expired_count` deltas, `eviction_expiry_seconds_total`, and `EvictingFor()` / `bee_localstore_eviction_running_seconds`. Pass: the two episode series stay 0 during every expiry run outside an unreserve episode.
- **Increase, on the bench:** during A3 (all nodes evict at the default 500) the episode opens as before (`EvictingFor()` and the episode gauge). The bench nodes are unstaked and not selected, so skips are not observed there.
- **Hypothesis 2 is not observable on unstaked nodes:** the agent only samples when selected. It is covered by tests 8 to 10; in production, by the two counters.
- **Mainnet canary (from #663):** on a staked production node, count expiry evictions over a week and how many pass the one-minute mark at 500/s, before and after this change, together with `skipped_while_evicting`. Pass: after the change, no `skipped_while_evicting` increment coincides with an expiry run outside an unreserve episode.

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
- `pkg/storer/mock/mockreserve.go` (`RadiusState`)
- `pkg/storageincentives/agent.go`, `pkg/storageincentives/metrics.go`
- tests in `pkg/storer` and `pkg/storageincentives`
- `docs/DIFFERENCES.md`: the #649 behaviour row and the metrics row (`:226`, which names `skipping round because the committed depth changed during the sample`), and the `reserve-eviction-rate` settings row (`:153`, whose expiry-threshold note changes)

Generated with help of AI.
