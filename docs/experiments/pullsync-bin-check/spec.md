# Pull-sync: refuse a request for a bin that cannot exist

Issue: #643. Found in the review of the spec for #640. Related: #640 (end an older waiting request from the same peer), #641 (end a waiting request when the requester is gone), #638 (stream limits per peer, the backstop).

## Terms

- **Request:** one inbound pull-sync stream (`pullsync/1.4.0`, stream `pullsync`). The requester sends a `Get` with a bin and a start BinID (`pkg/pullsync/pb/pullsync.proto`: `int32 Bin = 1; uint64 Start = 2`).
- **Valid bin:** a bin the reserve can hold, `0 <= Bin < swarm.MaxBins` (32). Reserve entries are keyed by `swarm.Proximity` (`pkg/storer/internal/reserve/reserve.go:127`), which returns at most `swarm.MaxPO` (31, `pkg/swarm/proximity.go`), so bins 32 and up are always empty.

## Problem

The handler never checks the requested bin. It converts it with `uint8(rn.Bin)` in two places (`pkg/pullsync/pullsync.go:189`, the #640 registration, and `:520`, `makeOffer`). Two things follow.

1. **A bin from 32 to 255 waits for ever.** `SubscribeBin` (`pkg/storer/reserve.go:673`) iterates the bin, finds nothing, and waits for a trigger on that bin, which only fires for bins 0 to 31. The request holds an inbound stream until the peer disconnects or the node stops. #640 limits this to two waiting requests per peer and bin, but there are 224 such bins, so one peer can still hold up to 448 extra waiting requests; with the 64 for valid bins, 512 in all.
2. **Any other value wraps.** `int32` to `uint8` keeps the low 8 bits, so 278 and -234 both become 22. Such a request is served as bin 22: it joins the shared collection for bin 22 (`sfKey`, `:723`) and counts toward the peer's own cap for bin 22 in #640. A negative value or a value above 255 can never be a real bin, and serving it as another bin hides a broken or hostile requester.

**No correct requester sends an invalid bin.** The wasp puller and upstream's both build `Get{Bin: int32(bin)}` (`pkg/pullsync/pullsync.go:294`) from loops over `p.bins`, which defaults to `swarm.MaxBins` (`pkg/puller/puller.go:149`; upstream `:124`), or from a peer's proximity order, at most `swarm.MaxPO`.

### Reproduced (2026-10-09)

`TestBinOutOfRangeRefused` (below, test 1) sends a `Get` for bin 40 through the normal `Sync` client and expects it refused. It uses only signals upstream has. It fails the same way on unmodified wasp `main` (9474b2545) and on unmodified upstream `master` (cc5c4706e):

```
request for bin 40: still waiting after 1m0s, want it refused at once
```

The test's reserve is the storer mock, so it shows that the handler accepts the request and waits (on the mock a valid bin with no chunks waits too). That the real reserve then waits for ever is read from `SubscribeBin` above, not measured; test 6 measures it.

## Change

1. **Check first.** In `handler`, straight after the `Get` is read and before the #640 registration, the #641 watcher and `makeOffer`: if `rn.Bin < 0 || rn.Bin >= int32(swarm.MaxBins)`, return an error wrapping a new `errBinOutOfRange`. The handler's existing defer resets the stream. No registration, no watcher, no subscription.
2. **Plain error, no penalty.** No disconnect and no blocklist: the peer is not punished, its request is just not served. This matches how other malformed requests are handled in this handler, and #638 is the place for per-peer limits. The handler error is logged at debug level by libp2p as today.
3. **Metric:** `bee_pullsync_requests_refused_total{reason="bin_out_of_range"}`, a counter. The `bin_out_of_range` label is created in `New`, so the series exports 0 from the start and "absent" can be told apart from "0". On a network of correct clients it stays at 0, so any increase points at a broken or hostile requester.
4. **Cursor handler:** not affected. It reads a `Syn` with no bin and answers with all cursors (`cursorHandler`, `pkg/pullsync/pullsync.go:660`, `ReserveLastBinIDs`).
5. **Conversions after the check** stay `uint8(rn.Bin)`, now safe.

No wire change (rule 6): the messages are unchanged, and a refused request ends the way any failed request ends today, with a stream reset. No setting.

## Tests

1. `TestBinOutOfRangeRefused` (portable, above), committed with the code: bin 40 through `Sync` is refused at once. The server mock is built with `WithSubscribeResp(nil, nil)`, because upstream's mock panics on a subscription with no prepared response. Fails on unmodified code, passes with the change.
2. **Raw stream values.** A `Get` written directly on a recorder stream for 32, 255, 278, -1 and -234 is refused at once; for each, `WaitingTracked()` stays 0 and the reserve mock records no `SubscribeBin` call. The mock gains a test-only counter for that, `SubscribeBinCalls()` in `pkg/storer/mock` (it has none today, and `OpenSubscriptions()` reads 0 after any return). 278 and -234 show that the wrap to bin 22 no longer happens.
3. **Edges still served:** bins 0 and 31 are not refused.
4. **Metric:** the counter rises by one per refused request.
5. **Ordering: a refused request never registers.** Checked by two observable results, not by a hook (`afterMakeOffer` runs only after `makeOffer`, so a check placed after `register` but before `makeOffer` would also skip it):
   - `WaitingTracked()` is 0 after the refusal;
   - a waiting request from the same peer for bin 22 with start S survives a following request with `Bin = -234` and start S (which wraps to bin 22), and `RequestsReplaced` stays 0. With the check moved after the #640 registration, the -234 request would register as (peer, 22, S), end the valid request as a duplicate and never unregister.
6. **Real reserve (storer level, optional but planned):** with a real DB, subscribe to bin 40, put chunks in, and assert nothing is delivered within a deadline. This turns "the real reserve waits for ever" from a reading of `SubscribeBin` into a measurement.

**Mutation checks** (each must make a test fail): check removed; upper bound `>` instead of `>=` (bin 32 accepted); lower bound removed (negative values accepted); check moved after the #640 registration; check done on `uint8(rn.Bin)` instead of `rn.Bin` (278 accepted as 22); metric not incremented.

## Upstream

Affects upstream: the handler path is unchanged from v2.8.2 to upstream `master`, and test 1 fails on unmodified upstream `master` (cc5c4706e). It is upstream's because the handler trusts the requester's bin and converts it without a range check. The fix there is the same check. The issue gets the `affects-upstream` label and a row in `docs/UPSTREAM.md` (added by this spec PR as `open`, as rule 14 asks and #583 and #607 did). When the code merges, the row is amended to `done` with the branch and merge commit; if this spec PR were closed without merging, the row would land separately.

## Measurement

On the production nodes (the ten-node dense host and the staked node, by role), after deployment: `bee_pullsync_requests_refused_total{reason="bin_out_of_range"}` over 7 days. Expected 0; any increase is recorded with the peer count, as evidence of a broken or hostile client. No performance effect is expected: the check is two comparisons per request.
