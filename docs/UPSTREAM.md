# Bee-origin issues

Every wasp issue that also applies to upstream Bee, that is, one carrying the
`affects-upstream` label, which means the defect or gap was confirmed in
unmodified Bee code. This is the set a maintainer could one day offer upstream.

By the fork rules this file does not link to the Bee repository, and nothing here
authorises contacting Bee or opening anything there; the `affects-upstream`
label is a marker for a later human decision and nothing more. The issues and
commits below are wasp's own.

- **Status** is `done` when the change is merged on `main`, with the ledger's
  outcome added where it has one (`validated` measured on a real node,
  `neutral` no measurable effect). A `done` row carrying neither word is merged
  but not yet measured, so the issue itself can still be open. `open` means no
  wasp change has merged for it yet. `not planned` means the issue was closed
  without a wasp change to node behaviour. `(via #N)` or `(bundled with #N)` means the fix
  landed inside another issue's pull request.
- **Branch** is the fork branch the work was built on. Fork branches are never
  deleted, so each one still exists. A dash means the issue has no branch of its
  own: the fix shared another issue's branch, the issue is still open, or no
  wasp change was made.
- **Commit** links the merge commit on `main`. A dash means no wasp change has
  merged. A `not planned` row can still carry one: what landed there is
  whatever the fork did keep, which may be only the tests and the record of
  why no fix was made.
- **Title** follows the issue's own title, with punctuation normalized to ASCII
  under the writing rules. Some older issue titles contain em-dashes; those read
  as commas here. This reverses an earlier instruction not to edit titles in
  this column at all, which was written to stop a title being reworded into
  something its author did not write; replacing a punctuation mark with its
  ASCII equivalent is not rewording. The wording is otherwise unchanged, so a
  title still identifies its issue. Do not reword a title to improve it: the
  column's job is to find the issue.

Derived from the experiment ledger ([`experiments/INDEX.md`](experiments/INDEX.md))
and the history, last revised 2026-10-07, against the upstream base
`v2.8.2`. Three issues (#73, #76, #430) were closed not planned. Wasp issue
numbers can collide with upstream Bee pull-request numbers in the shared
history, so the commits here were resolved from fork-only merges, not by issue
number alone.

**#430 keeps its label although wasp landed no behaviour change, and the reason is worth
stating**, because "not planned" would otherwise read as "not a real problem".
`MigratePeer` is byte for byte the same upstream, and a failure between its
writes does leave two peers mapped to one beneficiary there as here. What
differs is the consequence. Wasp serializes the cumulative payout with a
per-beneficiary lock, merged for
[#317](https://github.com/crtahlin/wasp/issues/317), so two overlays resolving
to one chequebook settle correctly. Upstream's `chequebook.Issue` has no such
lock.

**The difference in consequence is reasoned, not reproduced**, and rule 11 asks
for that to be said. What is checked is the code on both sides: wasp's
`Issue` takes `beneficiaryLocks` around the read of the last cheque and the
write of the new one (`pkg/settlement/swap/chequebook/chequebook.go:229-231`,
`:240`, `:278`), and `git show upstream/v2.8.2:pkg/settlement/swap/chequebook/chequebook.go`
has no equivalent. What is **not** demonstrated is a lost update: that needs
two concurrent `Issue` calls for one beneficiary from two overlays, and no
measurement here produced one.

Wasp closed the issue because the fix it proposed, reordering the writes, was
shown to be worse than the defect. That finding is about the fix, not about
whether upstream has the problem. See
[`experiments/migrate-peer/spec.md`](experiments/migrate-peer/spec.md).

**Table: wasp issues that also apply to upstream Bee, with how each was handled**

| Issue | Title | Status | Branch | Commit |
|---|---|---|---|---|
| [#21](https://github.com/crtahlin/wasp/issues/21) | Improve peer discovery and reduce aggressive pruning | done (via #148) | `feat/148-configurable-saturation` | [`af9f5fe6`](https://github.com/crtahlin/wasp/commit/af9f5fe6) |
| [#22](https://github.com/crtahlin/wasp/issues/22) | Scale the isFullySynced threshold by reserve capacity doubling | done (via #223) | `feat/223-sync-rate-threshold` | [`b9e572aa`](https://github.com/crtahlin/wasp/commit/b9e572aa) |
| [#24](https://github.com/crtahlin/wasp/issues/24) | Revert CompactionL0Trigger to the LevelDB default | done, neutral | `exp/24-l0-depth-bench` | [`8db48cb9`](https://github.com/crtahlin/wasp/commit/8db48cb9) |
| [#28](https://github.com/crtahlin/wasp/issues/28) | countWithinRadius performs a full O(n) reserve scan every wake-up | done, validated | `fix/28-batch-exists-cache` | [`3d1d3d21`](https://github.com/crtahlin/wasp/commit/3d1d3d21) |
| [#34](https://github.com/crtahlin/wasp/issues/34) | Reserve size within radius is computed two different ways | done | `fix/34-reserve-size-two-ways` | [`7f1da3d3`](https://github.com/crtahlin/wasp/commit/7f1da3d3) |
| [#35](https://github.com/crtahlin/wasp/issues/35) | Add descriptions to OpenAPI schema properties | done (via #192) | - | [`e999eaae`](https://github.com/crtahlin/wasp/commit/e999eaae) |
| [#36](https://github.com/crtahlin/wasp/issues/36) | Fix typos and parameter naming in the OpenAPI spec | done (via #192) | - | [`e999eaae`](https://github.com/crtahlin/wasp/commit/e999eaae) |
| [#69](https://github.com/crtahlin/wasp/issues/69) | fix(build): Green Tea GC in Go 1.26 corrupts the heap, build with GOEXPERIMENT=nogreenteagc | done, validated | `fix/69-disable-greenteagc` | [`9702319c`](https://github.com/crtahlin/wasp/commit/9702319c) |
| [#73](https://github.com/crtahlin/wasp/issues/73) | Node segfaults in goleveldb's block-cache buffer pool after the Green Tea fix | not planned | - | - |
| [#74](https://github.com/crtahlin/wasp/issues/74) | Node reports ready with zero peers and never recovers, p2p dial breaker stays tripped | done, validated | `fix/74-breaker-backoff-reset` | [`22dfbdd5`](https://github.com/crtahlin/wasp/commit/22dfbdd5) |
| [#76](https://github.com/crtahlin/wasp/issues/76) | Node dies with concurrent map writes in peerRegistry.addStream under load | not planned | - | - |
| [#77](https://github.com/crtahlin/wasp/issues/77) | SIMD BMT hasher segfaults in hashLeavesBatch, nil node dereference | done | `fix/77-warn-on-simd` | [`f18520b2`](https://github.com/crtahlin/wasp/commit/f18520b2) |
| [#79](https://github.com/crtahlin/wasp/issues/79) | Flaky CI: pkg/api hangs on macOS, TestDebugInfo/disk fails on Windows | done | `fix/79-async-test-deadlines` | [`bb6bbb73`](https://github.com/crtahlin/wasp/commit/bb6bbb73) |
| [#85](https://github.com/crtahlin/wasp/issues/85) | Node with zero peers cannot self-recover: breaker blocks the bootnode fallback | done, validated | `fix/74-breaker-never-isolates` | [`2e3016b7`](https://github.com/crtahlin/wasp/commit/2e3016b7) |
| [#89](https://github.com/crtahlin/wasp/issues/89) | XKCP generator emits a stack frame smaller than the code it generates needs | done | `fix/89-generated-frame-too-small` | [`4b57e508`](https://github.com/crtahlin/wasp/commit/4b57e508) |
| [#90](https://github.com/crtahlin/wasp/issues/90) | keccak .syso blobs cannot be rebuilt from source; build fails on modern gcc | done, validated | `fix/90-rebuildable-keccak-blobs` | [`c950d7b6`](https://github.com/crtahlin/wasp/commit/c950d7b6) |
| [#91](https://github.com/crtahlin/wasp/issues/91) | XKCP go_wrapper writes out of bounds when a lane is shorter than the longest | done, validated | `fix/91-xkcp-oob-write` | [`b5691b68`](https://github.com/crtahlin/wasp/commit/b5691b68) |
| [#92](https://github.com/crtahlin/wasp/issues/92) | SIMD assembly stub runs foreign code on the goroutine stack and passes Go pointers to it | done, validated | `fix/92-scratch-stack` | [`8fe665dd`](https://github.com/crtahlin/wasp/commit/8fe665dd) |
| [#99](https://github.com/crtahlin/wasp/issues/99) | TestGetter cancellation waits are bounded at scheduling-latency timescale | done, validated | `fix/replicas-getter-timing-flake` | [`1e389b93`](https://github.com/crtahlin/wasp/commit/1e389b93) |
| [#109](https://github.com/crtahlin/wasp/issues/109) | blockchain-rpc-endpoint accepts only one endpoint, and a 10-minute outage shuts the node down | done, validated | `feat/109-rpc-failover` | [`d5d36cb1`](https://github.com/crtahlin/wasp/commit/d5d36cb1) |
| [#114](https://github.com/crtahlin/wasp/issues/114) | goleveldb races on close when a writer is paused at the L0 trigger | done (bundled with #115) | `fix/115-close-without-writing` | [`ac7f7365`](https://github.com/crtahlin/wasp/commit/ac7f7365) |
| [#115](https://github.com/crtahlin/wasp/issues/115) | Store.Close writes, so a stalled index store cannot shut down cleanly | done | `fix/115-close-without-writing` | [`ac7f7365`](https://github.com/crtahlin/wasp/commit/ac7f7365) |
| [#122](https://github.com/crtahlin/wasp/issues/122) | leveldbstore cannot open a database read-only | done | `fix/readonly-open` | [`1e89e4b6`](https://github.com/crtahlin/wasp/commit/1e89e4b6) |
| [#128](https://github.com/crtahlin/wasp/issues/128) | TestPostageAccessHandler deadlocks when its 100ms sleep is not enough | done | `fix/128-postage-test-deadlock` | [`fdcd84a6`](https://github.com/crtahlin/wasp/commit/fdcd84a6) |
| [#138](https://github.com/crtahlin/wasp/issues/138) | Four hive ping metrics are declared but never observed | done | `fix/138-dead-hive-metrics` | [`261e452f`](https://github.com/crtahlin/wasp/commit/261e452f) |
| [#146](https://github.com/crtahlin/wasp/issues/146) | Seven storage benchmarks fail on Go 1.26, and the rest depend on which selection is run | done | `fix/146-storage-benchmark-harness` | [`e915041e`](https://github.com/crtahlin/wasp/commit/e915041e) |
| [#156](https://github.com/crtahlin/wasp/issues/156) | A blocked log sink deadlocks peer connectivity: node goes deaf and never recovers | done | `fix/156-log-sink-backpressure` | [`de4cfdca`](https://github.com/crtahlin/wasp/commit/de4cfdca) |
| [#158](https://github.com/crtahlin/wasp/issues/158) | kademlia's manage loop waits on dials without a bound | done | `fix/158-bounded-connector-wait` | [`14ae5e67`](https://github.com/crtahlin/wasp/commit/14ae5e67) |
| [#160](https://github.com/crtahlin/wasp/issues/160) | The chunkstore benchmarks read addresses that could never exist | done | `fix/160-chunkstore-benchmark-addresses` | [`b792d653`](https://github.com/crtahlin/wasp/commit/b792d653) |
| [#162](https://github.com/crtahlin/wasp/issues/162) | ReadRandomMissing measures range rejection, not a missing-key lookup | done | `fix/162-missing-key-interleaved` | [`f810581a`](https://github.com/crtahlin/wasp/commit/f810581a) |
| [#167](https://github.com/crtahlin/wasp/issues/167) | kademlia's bounded dial wait ignores shutdown | done | `fix/167-shutdown-aware-connector-wait` | [`c5feed0b`](https://github.com/crtahlin/wasp/commit/c5feed0b) |
| [#173](https://github.com/crtahlin/wasp/issues/173) | The store read benchmarks charge harness overhead to the engine | done | `fix/173-store-read-benchmark-overhead` | [`fcb110f0`](https://github.com/crtahlin/wasp/commit/fcb110f0) |
| [#176](https://github.com/crtahlin/wasp/issues/176) | A write-stalled index store has no in-process recovery | done | `fix/176-write-pause-log-line` | [`150d644c`](https://github.com/crtahlin/wasp/commit/150d644c) |
| [#282](https://github.com/crtahlin/wasp/issues/282) | Research: can a light node serve its cache to offload retrieval bandwidth from full nodes? | open | - | - |
| [#291](https://github.com/crtahlin/wasp/issues/291) | Kademlia pruning can disconnect a static peer it is meant to protect | done | `fix/291-static-peer-pruning` | [`c2294a19`](https://github.com/crtahlin/wasp/commit/c2294a19) |
| [#300](https://github.com/crtahlin/wasp/issues/300) | swap: three chain calls per received cheque cap how fast one peer can serve another | open | - | - |
| [#301](https://github.com/crtahlin/wasp/issues/301) | chequebook: read the chequebook issuer once instead of on every cheque | done, neutral | `fix/301-cheque-acceptance-cost` | [`a89a3a83`](https://github.com/crtahlin/wasp/commit/a89a3a83) |
| [#302](https://github.com/crtahlin/wasp/issues/302) | chequebook: do not make cheque acceptance wait for the liquidity check | done, neutral (bundled with #301) | `fix/301-cheque-acceptance-cost` | [`a89a3a83`](https://github.com/crtahlin/wasp/commit/a89a3a83) |
| [#316](https://github.com/crtahlin/wasp/issues/316) | accounting: the refresh allowance in settle is not clamped, so a backwards clock step raises the payment (the issue's title claims a missing cap, which is refuted; see below) | done | `fix/316-refreshdue-cap` | [`2bd2d08c`](https://github.com/crtahlin/wasp/commit/2bd2d08c) |
| [#333](https://github.com/crtahlin/wasp/issues/333) | accounting: Connect rewinds the threshold-growth checkpoint but not the counter it is compared against | done | `fix/333-threshold-growth-reconnect` | [`84897232`](https://github.com/crtahlin/wasp/commit/84897232) |
| [#337](https://github.com/crtahlin/wasp/issues/337) | file: hashtrie.Sum formats the dispersed-replica failure with %s against err.Error() rather than %w, so errors.Is cannot see the cause | done | `fix/337-replica-error-chain` | [`e2e2c623`](https://github.com/crtahlin/wasp/commit/e2e2c623) |
| [#359](https://github.com/crtahlin/wasp/issues/359) | accounting: the refresh allowance is granted as a step, and nearly all refusals fall in the discarded window | open | - | - |
| [#366](https://github.com/crtahlin/wasp/issues/366) | api: a malformed Swarm-Index-Document header returns 500 instead of 400 | done | `fix/366-index-document-status` | [`7d072d97`](https://github.com/crtahlin/wasp/commit/7d072d97) |
| [#382](https://github.com/crtahlin/wasp/issues/382) | p2p: already-connected is decided per address, so a connect over an open connection runs a second handshake and reports as a dial | done (via #522), validated | `fix/522-connect-already-connected` | [`1063b2bf`](https://github.com/crtahlin/wasp/commit/1063b2bf) |
| [#387](https://github.com/crtahlin/wasp/issues/387) | postage: TestCrashRecovery assumes two random addresses land in different buckets, so it fails about 1 run in 256 | done | `fix/387-crash-recovery-buckets` | [`3a83199c`](https://github.com/crtahlin/wasp/commit/3a83199c) |
| [#399](https://github.com/crtahlin/wasp/issues/399) | Shutdown race: reserve worker iterates the store while it is being closed, SIGSEGV under pebble | done | `fix/399-shutdown-race` | [`dc9f69c6`](https://github.com/crtahlin/wasp/commit/dc9f69c6) |
| [#407](https://github.com/crtahlin/wasp/issues/407) | Reserve worker's evict and unreserve store operations are not interrupted on shutdown | done | `fix/407-shutdown-interrupt` | [`735f8613`](https://github.com/crtahlin/wasp/commit/735f8613) |
| [#409](https://github.com/crtahlin/wasp/issues/409) | api: a directory upload with a non-tar body returns 500 instead of 400 | done | `fix/409-non-tar-body-status` | [`167a8d7a`](https://github.com/crtahlin/wasp/commit/167a8d7a) |
| [#424](https://github.com/crtahlin/wasp/issues/424) | api: two malformed multipart directory uploads return 500 instead of 400 | done | `fix/424-multipart-status` | [`05672a98`](https://github.com/crtahlin/wasp/commit/05672a98) |
| [#455](https://github.com/crtahlin/wasp/issues/455) | api: two more malformed multipart bodies answer 500, which #424 did not cover | done | `spec/455-multipart-part-errors`, `fix/455-multipart-part-errors` | [`ca413858`](https://github.com/crtahlin/wasp/commit/ca413858) |
| [#438](https://github.com/crtahlin/wasp/issues/438) | retrieval: a flight ends on the error budget while a request is still in flight, so the delivery is thrown away | done | `fix/438-flight-exit` | [`4f88343c`](https://github.com/crtahlin/wasp/commit/4f88343c) |
| [#440](https://github.com/crtahlin/wasp/issues/440) | api: GET /chunks answers 500 when the peer walk is exhausted, where /bzz answers 404 | done | `fix/440-chunk-notfound` | [`15a2d874`](https://github.com/crtahlin/wasp/commit/15a2d874) |
| [#449](https://github.com/crtahlin/wasp/issues/449) | api: POST /pins answers 500 when the peer walk is exhausted, the same defect as #440 on a second endpoint | done | `fix/449-pin-notfound` | [`726c5b35`](https://github.com/crtahlin/wasp/commit/726c5b35) |
| [#430](https://github.com/crtahlin/wasp/issues/430) | swap: MigratePeer is not atomic, so a failed delete leaves two peers sharing one beneficiary | not planned | `fix/430-migratepeer` | [`b0fb7d82`](https://github.com/crtahlin/wasp/commit/b0fb7d82) |
| [#511](https://github.com/crtahlin/wasp/issues/511) | A peer behind NAT waits 10 s for its addresses before the handshake | done, validated | `fix/511-identify-wait` | [`a20f4812`](https://github.com/crtahlin/wasp/commit/a20f4812) |
| [#522](https://github.com/crtahlin/wasp/issues/522) | api: POST /connect to an already connected peer answers 500, and on the reverse direction disconnects the peer | done, validated | `spec/522-connect-already-connected`, `fix/522-connect-already-connected` | [`1063b2bf`](https://github.com/crtahlin/wasp/commit/1063b2bf) |
| [#541](https://github.com/crtahlin/wasp/issues/541) | swap: fixed gas limits fail under Glamsterdam's state-gas costs; chequebook deploy breaks on Sepolia on 2026-10-06 | fixed, validation pending | `spec/541-swap-gas-estimation`, `fix/541-swap-gas-estimation` | [`97442a76`](https://github.com/crtahlin/wasp/commit/97442a76) |
| [#548](https://github.com/crtahlin/wasp/issues/548) | swap: a reverted initial deposit into a new chequebook is never retried | open | - | - |
| [#573](https://github.com/crtahlin/wasp/issues/573) | Puller: a peer that keeps failing pull-sync is retried with no pause (2,000 attempts/s, 1.4 cores) | done | `fix/573-puller-retry-backoff` | [`ab7ed195`](https://github.com/crtahlin/wasp/commit/ab7ed195) |
| [#578](https://github.com/crtahlin/wasp/issues/578) | Postage listener: a failed log query during catch-up is retried with no pause | done | `fix/578-listener-retry-backoff` | [`cd539e76`](https://github.com/crtahlin/wasp/commit/cd539e76) |
| [#579](https://github.com/crtahlin/wasp/issues/579) | Swap: each invalid first cheque costs a chain call under a global lock, and the peer can repeat it at once | open | - | - |
| [#583](https://github.com/crtahlin/wasp/issues/583) | Postage listener: when postage sync stalls, a running node stops and the process exits with status 0, so a service manager that restarts only on failure leaves it down | open | - | - |
| [#607](https://github.com/crtahlin/wasp/issues/607) | kademlia: a peer that drops us right after connecting is re-dialled forever, because each brief success resets the failed-attempt count | open | - | - |
| [#640](https://github.com/crtahlin/wasp/issues/640) | pull-sync: a peer that asks again for a bin leaves its older waiting request open, and these fill the inbound stream cap | done | `feat/640-pullsync-duplicate-requests` | [`bb05949a`](https://github.com/crtahlin/wasp/commit/bb05949a) |
| [#643](https://github.com/crtahlin/wasp/issues/643) | pull-sync: the requested bin is not checked against the number of bins, so a request for a bin that cannot exist waits forever | done | `feat/643-pullsync-bin-check` | [`e379e428`](https://github.com/crtahlin/wasp/commit/e379e428) |
| [#647](https://github.com/crtahlin/wasp/issues/647) | pull-sync: data race inside the singleflight library when a caller leaves while another joins the same key | done | `feat/647-singleflight-v041` | [`a7c471b0`](https://github.com/crtahlin/wasp/commit/a7c471b0) |
| [#695](https://github.com/crtahlin/wasp/issues/695) | puller: one peer that does not answer the cursors request blocks the whole recalculation, so a radius change is not acted on | done | `feat/695-puller-cursors-deadline` | [`03261874`](https://github.com/crtahlin/wasp/commit/03261874) |
| [#696](https://github.com/crtahlin/wasp/issues/696) | storer: the radius drops a second step while the puller has not synced at the current radius, because a sync rate of 0 counts as synced | done | `feat/696-radius-decrease-after-sync` | [`a724fe45`](https://github.com/crtahlin/wasp/commit/a724fe45) |
| [#576](https://github.com/crtahlin/wasp/issues/576) | Puller: a peer that keeps answering with an empty offer is still retried with no pause | done | `feat/576-puller-no-progress-and-epoch` | [`366f0a53`](https://github.com/crtahlin/wasp/commit/366f0a53) |
| [#590](https://github.com/crtahlin/wasp/issues/590) | puller: a failed epoch read or write after GetCursors skips the peer's epoch reset for good | done | `feat/576-puller-no-progress-and-epoch` | [`366f0a53`](https://github.com/crtahlin/wasp/commit/366f0a53) |
| [#725](https://github.com/crtahlin/wasp/issues/725) | storage incentives: a stop between commit and reveal freezes the node for twice as long; wait for the reveal before stopping | done | `feat/725-reveal-before-stop` | [`63c43149`](https://github.com/crtahlin/wasp/commit/63c43149) |

#301 and #302 are done, and they settle only half of what #300 says. Its per
peer half stands: the three chain calls were confirmed directly, and the rate at
which one peer can serve another was measured in #290. Its node-wide half, the
cost across all of a node's peers at once, was never exercised, so #300 stays
open and the work needed to answer it is
[#312](https://github.com/crtahlin/wasp/issues/312). A reader should not take
the two closed rows as evidence for that half.

#303 and #304 had rows here and no longer do. Both lost the
`affects-upstream` label when their spec was written
(`docs/experiments/accounting-gates/spec.md`, Upstream portability). They are
optimizations this fork wants, not things upstream got wrong, and rule 11 says
to tag defects rather than preferences. The same reading of that code did turn
up two genuine problems, and they were split off rather than left attached to
the changes:

- [#316](https://github.com/crtahlin/wasp/issues/316) is tagged and has its row
  above, but **not for the reason first recorded here, which was wrong and is
  corrected per rule 11.**

  This entry said the defect was the missing cap in `settle`, and called it
  rule 11's own example, one quantity computed two different ways. It is not.
  `peerAllowance` grants elapsed seconds times the refresh rate, and
  `NotifyRefreshmentSent` blocklists a peer that forgives less than
  `interval x refreshRate`, so both sides of the protocol agree a refreshment
  is worth the elapsed product. Capping `settle`'s prediction at one rate would
  make this node pay for debt its peer is obliged to forgive. The capped sites
  bound a credit limit until the next refreshment; two of them are not gates at
  all but the read-only reporter in `PeerAccounting`. They share a name with
  this one and nothing else.

  What is upstream's defect, and what keeps the label, is that the elapsed time
  is a signed subtraction with no clamp, so a clock stepping back a second or
  more makes the term negative and raises the payment. Upstream line 484 is
  unclamped. That is the same fault #359 fixed on the credit-limit side, and
  the fix is the same change in both trees.
- [#333](https://github.com/crtahlin/wasp/issues/333), the growth checkpoint
  rewound on connect while `totalDebtRepay` is not, is tagged and has its row
  above. `Connect` sets `thresholdGrowAt` back to 450,000,000 but nothing ever
  re-zeroes the cumulative figure it is tested against, which is written once at
  record creation, and the per-peer record is never deleted from the map. So a
  returning peer with a long history fires the threshold upgrade on every
  settlement until the checkpoint climbs back over the stale total, sending an
  announce under the peer lock each time.

  **Two statements here were wrong and are corrected, per rule 11.** The first
  said the run raises the granted threshold "past a limit the node refuses to
  start with". It does not, at the shipped default: a peer at 10,000,000,000
  collects 18 upgrades worth 81,000,000, and 13,500,000 + 81,000,000 =
  94,500,000, below the 108,000,000 `maxPaymentThreshold`. It passes that limit
  only for a configured base above 27,000,000. The second said the area was
  "verified byte-identical to `upstream/v2.8.2` across `pkg/accounting`,
  `pkg/pricing` and `pkg/settlement/pseudosettle`". That is no longer true:
  `git diff --stat upstream/v2.8.2 origin/main` over those three paths reports
  11 files changed and 2,203 lines inserted. The defect is still upstream's, but
  that had to be checked in upstream's own copy rather than by diffing the
  files: `totalDebtRepay` is written zero once at line 613 there and every other
  write is an addition, and upstream's `Connect` carries the same reset block,
  lines 1425 to 1433, with the same omission.

  It was read from the code and not measured on a node.
- [#317](https://github.com/crtahlin/wasp/issues/317), the unlocked allocation
  in `chequebook.Issue`, is **not** tagged and has no row. The code is the same
  upstream, but no caller there or here can reach it: `Issue` has a single
  caller, gated per peer by `paymentOngoing`, and two peers cannot share a
  beneficiary. It is a latent hazard that #303 itself would make reachable, and
  rule 11 says to leave untagged what is reasoned from reading the code rather
  than reproduced. That issue records what evidence would justify the tag later.

Generated with help of AI.
