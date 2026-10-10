# Puller: pause after an empty offer too, and keep the epoch check until it succeeds

Issues: #576 (a peer that keeps answering with an empty offer is still retried with no pause) and #590 (a failed epoch read or write after `GetCursors` skips the peer's epoch reset for good). Both are in `pkg/puller/puller.go`, both concern how a sync worker or sync peer recovers from a peer that does not deliver, and both are small; one spec, one pull request, as #645, #646 and #648 shared one (`pullsync-waiting-followups`). Related: #573 (the retry pause), #695 (the cursors deadline, in review).

## Terms

- **No progress:** a `Sync` call whose returned topmost is below the requested start (`top < start`), so the interval does not advance (`puller.go:478`, `:517-526` on the `main` this spec was written against).
- **Empty offer:** `pullsync.Sync` returns `offer.Topmost, 0, nil` when the offer lists no chunks (`pkg/pullsync/pullsync.go:331`).
- **Epoch check:** in `syncPeer`, after `GetCursors`, the stored epoch of the peer is read and, when it differs from the peer's, the peer's intervals are reset and the new epoch stored (`puller.go:342-361`).

## Problem

### #576: no pause after an empty offer

#573 added a growing pause before retrying a call that made no progress, but only on the error path (`puller.go:486-505`: `if err != nil { ... if top < start { failures++; waitRetry } }`). A call that returns no error and no progress, an empty offer with `Topmost < start`, loops again at once, and neither increases nor resets the failure count.

When does a peer send that? The serving side builds the offer in `collectAddrs` (`pkg/pullsync/pullsync.go`), which waits for the first chunk at or above `start` and returns topmost 0 with no error when its subscription channel closes (`:590`). `SubscribeBin` closes the channel when the reserve stops (`pkg/storer/reserve.go`, `defer close(out)`); its release (`unsub`) runs only after `collectAddrs` has stopped reading (`pkg/pullsync/pullsync.go:576-581`), so a release never ends a wait. A healthy peer can therefore send one such offer only while it shuts down, and then disconnects. (Corrected in the code review; the first version said a release also closes the channel under a waiting request.) A healthy peer that is running never sends an empty offer below `start`: it either waits for a chunk or delivers one at or above `start`. **The invariant: an empty offer means the server has nothing for this range and will not have anything soon.** A peer that keeps sending them is faulty or hostile, and today it gets the busy loop #573 measured for errors (about 2,000 attempts per second and 1.4 cores from one peer).

**Evidence:** reasoned from the code; not observed on a node. Upstream retries every failed call at once (it has no #573 pause), so the busy loop for empty offers is the same defect there as the one #573 measured and labelled `affects-upstream`; the reproduction below settles the label for #576.

### #590: the epoch check is skipped for good after it fails once

`syncPeer` sets `peer.cursors` (`puller.go:340`) before the epoch check (`:342-361`). If reading the stored epoch, resetting the intervals or storing the new epoch fails, `syncPeer` returns an error, but `peer.cursors` is set. The next recalculation sees `peer.cursors != nil` (`:335`) and skips the epoch check until the peer disconnects. Meanwhile the node syncs the peer with intervals from its previous epoch: the peer's BinIDs restarted, so the stored intervals mark ranges as done that the node never received from this epoch, and those chunks are skipped.

**Evidence:** reasoned from the code; it needs a statestore error to happen at all. Upstream has the same order (`peer.cursors = cursors` at `puller.go:265` on `master`, before the epoch check).

## Change

1. **#576, one rule for no progress.** In the sync worker, after the error handling and the `ErrPeerNotFound` return, any call with `top < start` counts as a failure and waits `retryBackoff(failures)` before the next call, whether `err` is nil or not. A call with `top >= start` resets the count, as today. Concretely, the `top < start` block moves out of `if err != nil`.
2. **#576, visibility:** `bee_puller_no_progress_total`, a counter of calls that returned no error and no progress: an empty offer, or an offer whose topmost is below the start (`pullsync.go:500`). On a network of correct peers it rises only when a neighbour shuts down. (Named `empty_offers_total` in the first version of this spec; renamed in review, because it also counts the second kind.)
3. **#590, set the cursors last.** In `syncPeer`, the cursors are kept in a local variable through the epoch check; `peer.cursors` is set only when the epoch read, and any reset and store, have succeeded. On a failure `peer.cursors` stays nil, so the next recalculation asks for the cursors again and repeats the epoch check. With #695 merged that is one more cursors request per failure, bounded by its deadline.
4. **Correction carried with this change:** the #573 spec (`docs/experiments/puller-retry-backoff/spec.md`, line 49) says a recovered peer "is synced again within a minute". The longest wait is the 1-minute cap plus up to 20 % random, 72 s; the sentence is corrected to that.
5. **No wire change** (rule 6) and no setting.

### Cost

A healthy neighbour that shuts down costs its worker at most one pause of 1 s (the first failure) before the worker notices the disconnect, instead of an immediate retry. It sends an empty offer only some of the time: a subscription release never ends a waiting `collectAddrs`, because `unsub` runs only after reading stopped (`pkg/pullsync/pullsync.go:576-581`); on a reserve stop `SubscribeBin` both sends `ErrDBQuit` and closes its channel, and the server's `select` picks either (`pkg/storer/reserve.go:681, 691-692, 708-710`). A peer that keeps sending empty offers is slowed exactly as one that keeps failing (#573: at most about 26 attempts per minute once the waits reach their maximum).

## Tests

1. **Reproduction for #576, `TestEmptyOfferRetryPaused`, portable to upstream.** `Bins: 1`, cursors all 0 (so only the live worker runs), one neighbour from the kademlia mock, and a test-local `pullsync.Interface` stub that answers every `Sync` for bin 0 with `topmost 0, count 0, err nil` and blocks on its context for any other bin, counting calls with an atomic. Without that set-up correct code makes up to 64 calls (one live and one historical worker per bin). After 500 ms the test asserts at most 20 calls (with #573's 1 s base the fixed code makes about 1; the test lowers nothing). On unmodified wasp `main` the worker loops at once and makes thousands. On upstream the same count shows the busy loop.
2. **Pause grows and resets:** with the backoff lowered through the existing setter, empty offers make the waits grow; a following call with `top >= start` resets the count.
3. **Counter:** `no_progress_total` rises per empty offer, and not for an error or for a call with progress.
4. **No change for errors:** the #573 tests still pass unchanged.
5. **Reproduction for #590, `TestEpochCheckRetriedAfterFailure`, portable to upstream.** A test-local statestore wrapper fails the first read of the peer's epoch key (`sync_interval_epoch_` plus the peer address, built in the test) and passes everything else. The stored epoch is 1 and an interval for one bin is stored; the peer's cursors carry epoch 2. After the first recalculation fails, a second one is triggered (kademlia mock `Trigger()`). The test asserts that the stored epoch becomes 2 and the stored interval is gone. On unmodified code the stored epoch stays 1, because the second recalculation skips the check.
6. **Cursors asked again:** after a failed epoch check, `GetCursors` is called again at the next recalculation.

**Mutation checks** (each must make a test fail): the `top < start` pause left inside `if err != nil` (test 1); failure count not reset after progress (test 2); empty offer not counted, or an error counted as one (test 3); `peer.cursors` set before the epoch check (test 5); `peer.cursors` set after the check but also on its failure (tests 5, 6).

Race detector on `pkg/puller`; check the exit status directly.

## Upstream

- **#576:** run test 1 on an unmodified `upstream/master` worktree. If the call count shows the busy loop, add `affects-upstream` with what was checked and a row in `docs/UPSTREAM.md`.
- **#590:** run test 5 on an unmodified `upstream/master` worktree. If the stored epoch stays 1, add `affects-upstream` likewise.

## Measurement

On the dense host (ten nodes, by role), 24 hours with the build carrying this change: `bee_puller_empty_offers_total` per node (expected: small steps when neighbours restart, no sustained rate), and `bee_puller_worker_errors` for comparison. A sustained rate of empty offers from one peer would be the first observation of #576 on a node and is recorded in the issue. #590 is not expected to show on healthy disks; no measurement beyond the tests.

Generated with help of AI.
