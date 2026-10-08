# Keep a node running when postage sync stalls, and make a stop visible to the service manager

Issue: [#583](https://github.com/crtahlin/wasp/issues/583)

## Terms

- **Postage listener:** the loop in `pkg/postage/listener/listener.go` that reads postage contract events (`BatchCreated`, `BatchTopUp`, `BatchDepthIncrease`, `PriceUpdate`, `Paused`) from the chain through the RPC endpoint and applies them to the batch store.
- **Batch store:** the node's local copy of every postage batch and the chain state (block, total amount, current price) that those events produce.
- **Page:** one `eth_getLogs` query over a block range: 5,000 blocks with the RPC backend, 50,000 with the snapshot backend (`blockPage`, `blockPageSnapshot`, `listener.go:32-33`).
- **Caught up:** the listener has applied a page in the non-paged branch, that is, a page whose range reached the confirmed head because the remaining range was smaller than one page (`to-from < pageSize`, `listener.go:343-348`). Taking the branch is not enough; the page must also be applied.
- **Last progress:** the time the listener last applied a page (`lastProgress`, `listener.go:267`, `:371`).
- **Stale:** no applied page for `stallingTimeout`, 10 minutes (`postageSyncingStallingTimeout`, `pkg/node/node.go:256`), **or** not caught up since the node last became stale. Stale ends only when the listener is caught up again (section 2).
- **Degraded:** the node's state while stale: it stays on the network, **accepts and serves chunks** (section 3), does not play the storage lottery, does not lower its storage radius, and keeps working to recover.
- **Held chunk:** a chunk whose stamp names a batch the stale node does not know yet. It is kept in the chunk store with its stamp in a separate held index, served by retrieval, and validated when the node is caught up (section 3).

## Problem

### What happens today (verified in code on `origin/main` at 9cf512ee)

**After startup (a caught-up node falls behind):**
1. The loop checks `time.Since(lastProgress) >= l.stallingTimeout` at the top of every pass and returns `ErrPostageSyncingStalled` (`listener.go:277-279`).
2. The goroutine that runs the loop logs `failed syncing event listener; shutting down node error` and calls `l.syncingStopped.Signal()` (`listener.go:385-390`).
3. `start` waits on `SyncingStopped()`, logs `syncing has stopped` at debug level and `shutting down...`, and runs the normal shutdown (`cmd/bee/cmd/start.go:114-121`).
4. `RunE` returns `nil` (`start.go:189`), because only a failed build is kept as an error (`start.go:185-187`, from #490). `main` exits with a status only when `Execute` returns an error (`cmd/bee/main.go:40-43`), so the process exits 0.

So a node that falls behind and stalls stops **and reports success**. A light node whose background batch sync fails takes the same signal (`node.go:1276-1283`), and so does the snapshot backend, which receives the same signaller and timeout (`node.go:1196`).

**At startup (a full node catching up):** `batchSvc.Start` blocks building the node until the listener is caught up or fails (`node.go:1267-1273`; `batchservice.go:334-345` waits on the listener's `synced` channel). A stall there returns `unable to start batch service: postage syncing stalled` as a build error, which in wasp exits 1 (#490), and kademlia has not started yet (`node.go:1807`), so the node never joined the network. Under `Restart=always` it starts again and meets the same stall.

**The listener's chain calls have no deadline.** The HTTP transport sets dial, TLS and idle timeouts only (`pkg/node/chain.go:74-84`); the RPC client is built with an `http.Client` that has no `Timeout` (`chain.go:265`); the failover backend adds no per-call deadline (`pkg/transaction/failover/failover.go:154-171`; its only `WithTimeout` is the 10 s endpoint probe at line 371); and the listener passes its long-lived context to `BlockNumber` and `FilterLogs` (`listener.go:311`, `:351`). A call that never answers therefore blocks the loop indefinitely: the stall check at the top of the loop is never reached, so the node neither stops nor recovers until the connection itself fails.

### Two ways into it

**A page that always fails (the original report, from code, not observed).** During catch-up, a provider that limits the block range or the result size of `eth_getLogs` fails the same 5,000-block page every time. #578 (merge cd539e76) only makes the retries wait `backoffTime` (5 s); it does not change the page, so after 10 minutes the node stops (or, at startup, fails to build), restarts and meets the same page.

**An overloaded host (observed on 2026-10-08, sw-1, scenario A of #621, data on #622).** Ten nodes evicting about 14 M chunks each at once drove the host to a load average of about 120 on 8 threads, with memory pressure stalls 21 % of the time and I/O pressure stalls 33 % of the time (5-minute "full" averages from `/proc/pressure`). Node 1's own listener log shows (times converted from the node's UTC log to CEST):
- one Warning, `could not get blockchain log ... 408 Request Timeout: stream timeout`, at 15:23:42;
- then nothing from the listener for 89 minutes;
- then `failed syncing event listener; shutting down node error ... postage syncing stalled` at 16:52:40;
- the unit `inactive (dead)`, `code=exited, status=0/SUCCESS`.

The `getting block number ... context deadline exceeded` lines in the same log come from the storage-incentives agent, whose call carries a deadline of one round (`pkg/storageincentives/agent.go:191-199`), not from the listener. The 89 silent minutes fit a listener call that hung with no deadline until the connection failed; that is a hypothesis consistent with the log, since nothing recorded the call itself. That host's units use `Restart=on-failure`, so after the clean exit the node stayed down.

### Why a stop is the wrong answer

- A restart does not fix a page the provider always refuses: the node meets the same page again.
- A restart does not fix an overloaded host. It adds load: the node reopens its store, re-dials its peers and restarts any eviction or sync it was in. On sw-1 the nine nodes that stayed up recovered without help when the host recovered; node 1, the one the stall check stopped, did not.
- Exit status 0 hides the stop from every service manager that restarts only on failure (systemd `Restart=on-failure`, Docker `restart: on-failure`).

## Hypothesis

A stale batch store is a reason to stop **playing the storage lottery** and to **hold chunks of unknown batches until they can be validated**, not a reason to stop **the node**, and not a reason to stop accepting or serving chunks: a node that refuses chunks is, to its peers, as good as not on the network. If the node instead stays on the network in a degraded state, keeps actively working to recover (per-call deadlines, a wait that grows while the stall lasts, a smaller page when the log query fails), alerts the operator, and leaves the degraded state only when caught up, then:

- the overloaded-host case ends by itself when the host recovers, without the node ever leaving the network;
- the always-failing-page case makes progress at a smaller page instead of looping through restarts;
- a hanging RPC call can no longer freeze the listener;
- the node never gets truly stuck; it exits only on a fault it cannot recover from, and then with a non-zero status.

### What a stale batch store affects (from code)

| Function | Effect of a stale batch store | Handled by |
|---|---|---|
| Pull-sync of chunks of batches created during the stall | The peer transfers the chunk first (`pullsync.go:299-311` checks only `ReserveHas`); `ValidStamp` then returns not found (`pkg/postage/stamp.go:193-196`), the chunk is dropped (`pullsync.go:358-362`), and the puller still records the interval as synced (`puller.go:518-525`), so the chunk is **never pulled again**. A reserve that lacks those chunks gives a sample that can disagree with the neighbourhood after recovery | **The chunk is held instead of dropped** (section 3): it goes into the chunk store with its stamp in the held index, retrieval serves it, and it is validated once caught up. Intervals advance only past chunks that were stored or held, so nothing is lost. When the held area is full, pulling pauses (live and historical) so no interval advances past a chunk that could not be held |
| Push-sync of chunks of new batches | The storer path validates the stamp before storing and signing (`pkg/pushsync/pushsync.go:270-283`); the forwarding path does not validate (`pushsync.go:322-344`), so a stale forwarder already forwards as today | **The storer holds the chunk and answers with an error, never a receipt** (section 3): the uploader's push goes on to other storers, which sign; retrieval serves the held chunk meanwhile |
| Expiring batches | `cleanup` runs from new chain state (`batchstore/store.go:296-325`), so expired batches stay valid locally while stale | Not playing the lottery while stale. After recovery the catch-up expires many batches at once, an eviction burst like #621 (see Not in scope) |
| Storage lottery sample | `minBatchBalance` uses the stale chain state (`agent.go:549-555`), and the reserve may hold expired or lack new chunks | **Must not play while stale** (section 4). A disagreeing reveal freezes a staked node |
| Radius changes | A paused puller reports a sync rate of 0 (`puller.go:512`), and the reserve worker lowers the radius when the reserve is below its threshold and the sync rate is 0 (`pkg/storer/reserve.go:297`), every 15 minutes (`node.go:266`), down to the minimum radius. After recovery the node would pull a much larger area, overfill, raise the radius and evict: the #621 load. A stopped node today does not change its radius | **The radius decrease is skipped while stale** (section 3). A radius increase stays possible: it only evicts, and the lottery gate keeps any wrong reserve out of a sample until catch-up |
| Retrieval of stored chunks | Does not read the batch store | Keeps working |
| `POST /stamps` | Still sends the purchase transaction; the new batch is unknown locally, so uploads with it return 422 `batch not usable yet or does not exist` (`api/bzz.go:122`) until caught up | Documented; `/status` reports the degraded state (section 5) |
| `/chainstate` | Reports the stale block, total amount and price | Documented; the new gauges report how far behind |

## Design

### 1. Per-call deadlines on the listener's chain calls

`BlockNumber` gets a deadline of 30 s and `FilterLogs` a deadline of 60 s, each from a context derived for that call only (compiled in; see Configuration). A call that runs out its deadline returns `context.DeadlineExceeded`, is counted as a backend error and goes through the existing error branches (`listener.go:312-323`, `:352-364`).

**The failover gives each attempt its own deadline.** Today `isTransportFailure` treats `context.DeadlineExceeded` as a transport failure (`pkg/transaction/failover/classify.go:51`), and `call` tries the next endpoint with the same context (`failover.go:154-171`). With a per-call deadline that context has already expired, so every following endpoint fails at once and the shared active endpoint (used by transactions and the storage-incentives agent too) jumps to the last one without any of the others being asked; `Recover` moves it back only after a probe, at 30 s or more (`failover.go:325-327`). On an overloaded host that would switch endpoints for every caller for nothing.

So `call` gives each endpoint attempt its own deadline (the caller's remaining time divided by the **connected** endpoints still to try, those whose `live` connection is set, as `advance` already checks at `failover.go:139`, with a floor of 10 s), derived from the caller's context, and **does not advance** when the caller's own context has ended (`ctx.Err() != nil`), returning that error instead. This was chosen over only stopping on `ctx.Err() != nil`: that alternative lets the first endpoint's hang use the whole deadline, so a second, healthy endpoint would never be tried within the call. With per-attempt deadlines a hanging endpoint costs one share and the healthy one is still reached in the same call. Callers without a deadline (the transaction service today) keep today's behaviour, since there is no remaining time to divide.

This does not stop all switching on a busy host: an attempt that runs out its own deadline still counts as a transport failure, so the shared active endpoint still advances, but only after the next endpoint has actually been tried. It also shares out other callers' deadlines: with two connected endpoints, the storage-incentives agent's per-attempt time is half its one-round deadline (`agent.go:192`). The comments that describe the failover outcome (`failover.go:8-11`, `chain.go:86-88`) are updated, since a stall no longer stops the node.

### 2. A stall marks the node stale instead of stopping it

The listener stores `lastProgress` and the caught-up state in atomics, and computes `Stale()` when it is read, not inside the loop:

- **stale begins** when `time.Since(lastProgress) >= stallingTimeout`;
- **stale ends** only when a page in the non-paged branch (`to-from < pageSize`) has been applied, that is, when the listener is caught up again. One applied page while thousands of blocks behind does not end it, and an endpoint that lets one page through every 9 minutes cannot keep the node out of the stale state.

Because `Stale()` is computed on read, a call that hangs (until its deadline in section 1) cannot keep the flag from being set. The stall check that returns `ErrPostageSyncingStalled` is removed from the loop.

**Other errors keep stopping the node, as faults that cannot be recovered by waiting:** a failure to apply events to the batch store (`processEvents` returning an error), `ErrParseSnapshot`, and `ErrPostagePaused` (the postage contract is paused). Section 7 gives their exit status.

The same rule applies to the light-node background path (`node.go:1276-1283`) and the snapshot backend (`node.go:1196`): a stall marks stale and does not signal `syncingStopped`.

### 3. A stale node keeps accepting and serving chunks

**Operator requirement (2026-10-08):** whatever the node's state, chunks must still be accepted and served; a node that does not is, to the network, as if it were not there.

**Chunks of known batches** are validated and stored as today on every path: push-sync (`pushsync.go:270-283`), pull-sync (`pullsync.go:358-381`, then `ReservePutter().Put` at `:393`), and retrieval serves them (`retrieval.go:756`). A batch the stale node knows may have been topped up or diluted during the stall; a chunk whose stamp index exceeds the depth the node knows (`ErrInvalidIndex`, `stamp.go:216-218`) is treated like a chunk of an unknown batch, since the dilution may simply not have been seen yet.

**Chunks of unknown batches are held, not dropped.** While stale, when `ValidStamp` fails with `postage.ErrNotFound` (`stamp.go:193-196`) or with `ErrInvalidIndex`, the chunk has already passed its content check (`cac.Valid` or `soc.FromChunk`, `pullsync.go:364-379`, `pushsync.go:247-259`), so its data is genuine and only its stamp cannot be checked yet. Instead of dropping it, the node **holds** it:

- **Where:** the chunk is written to the chunk store (`ChunkStore().Put`, which counts references, `pkg/storer/internal/chunkstore/chunkstore.go:92-133`, so a chunk that is also in the reserve or cache is stored once), and a held-index entry records its address, batch ID, stamp hash, the full stamp and the time it arrived. **The held entry and the chunk-store `Put` commit in one transaction**, so a crash cannot leave a reference without an entry or an entry without its chunk. It is **not** put in the cache's order index, so cache eviction (`cache.go:185`, bounded by `cache-capacity`) cannot remove it.
- **Served:** retrieval reads `Lookup()`, whose getter takes the chunk from the chunk store first and treats the cache entry as best effort (`pkg/storer/cachestore.go:63-80`, `cache.go:136-150`), so a held chunk is served like any stored chunk. Retrieval sends the chunk data only: the delivery message has a stamp field, but the handler does not fill it (`retrieval.go` writes `pb.Delivery{Data: ...}`; `pkg/retrieval/pb/retrieval.proto:15-19`), and the requester checks the content address, not a stamp. A held chunk therefore needs no stamp to be served.
- **Bounded:** at most `heldChunksMax` chunks (compiled in, 100,000, about 410 MB; see Configuration). A fake batch ID costs the sender nothing while the node is stale, so the bound is what limits spam. The bound is enforced as a **claim, not a check**, as `LocalIngestSession.Reserve` does (`pkg/storer/localingest.go:56-68`): concurrent push-sync and pull-sync handlers each claim room before holding, so they cannot together pass the bound. The check whether an address is already held and the claim for it run under a **per-address lock**, a `multex.Multex` keyed by the chunk address (the pattern the reserve uses, `pkg/storer/internal/reserve/reserve.go:43`, `:60`), so two handlers holding the same address at once claim room once. The room is released when the **last** held entry for that address leaves the held index. It is counted **once per address**: a held chunk offered again before validation is wanted again (the want check reads the reserve only, `pullsync.go:299-311`), and a second copy of the same address takes no further room.
- **When full:** new unknown-batch chunks are refused as today (push-sync answers with an error) and the held area is not churned, since there is no way to tell spam from real chunks before validation. To keep pull-sync from losing chunks when it can no longer hold them, a pull-sync page in which a wanted chunk could not be held returns no progress for that page (`topmost` below `start`), so the puller records nothing and retries the page after its back-off (`puller.go:497-506`, #573) instead of advancing past the chunk, and **pulling pauses (live and historical) while the held area is full**, through the existing `waitWhileSampling` wait (`puller.go:472`, which sits in the loop shared by both workers); it resumes when the area has room again or the node is caught up. A Warning reports the full area once per stale period.
- **Validated after catch-up, and at startup:** a validation worker walks the held index when the stale state ends, **and also at every startup when the held index is not empty**, so a node that restarts and catches up without becoming stale again does not keep held chunks for good. The startup pass waits for the same condition that ends the stale state: it starts only once the listener is caught up (a page in the non-paged branch applied), after storer recovery has finished and with the reserve worker running. Started earlier, it would validate against the stored chain state and drop the held chunks of batches created after that state was written, which are exactly the chunks it exists to keep. For each entry it runs `ValidStamp` with the stored stamp: a valid chunk inside the storage radius is put into the reserve, a valid chunk outside it is dropped (it was only ever held, never promised), and an invalid one (still unknown batch, wrong owner, bucket mismatch, expired) is dropped. **Promotion is idempotent:** `ReservePutter().Put` first (a repeated put of the same stamp is accepted by the reserve), then the held entry and its chunk-store reference are deleted in one transaction, so a crash between the two only repeats the promotion. A held entry whose chunk is gone from the chunk store is dropped. A metric counts held, promoted and dropped chunks.
- **Paced, and its cost stated.** Validation itself is cheap: one batch-store read and one owner recovery per entry (`stamp.go:213-215`), about 10 to 20 s of CPU for 100,000 entries (estimate). The dominant cost is up to 100,000 reserve `Put` transactions right after catch-up, together with the burst of batch expiries a long-stale node meets when it catches up. The worker therefore promotes in rounds with a pause between them, and records its total duration (`bee_postage_held_validation_seconds`). A reserve that was already full goes over capacity and evicts; while the radius decrease is blocked (section 3, the radius rule), that eviction can only raise the radius. Eviction itself is the subject of #629 (paced eviction) and #621.

**Pull-sync with held chunks.** `Sync` returns the offer's `topmost` whatever happened to single chunks (`pullsync.go:358-362` drops and continues; the return at `:403`), and the puller records `start..top` as synced (`puller.go:518-525`). With held chunks this is now safe for every chunk that could become valid once caught up: such a chunk is either stored or held, and every held chunk is validated later, so advancing the interval loses nothing that could be kept. Pull-sync still drops, and advances past, chunks that no batch-store state can make valid, as today: unmarshal failures and unsolicited chunks (`pullsync.go:339-354`), invalid content (`:375-379`), `ErrOverwriteNewerChunk` (`:394-397`), and owner, signature or bucket mismatches. The one case where a chunk could still be lost, a full held area, is the case in which pulling pauses (above). A held chunk offered again before validation is wanted again, since the want check reads the reserve only (`pullsync.go:299-311`); the same stamp is not held a second time (the held index is keyed by address and stamp). Each held entry owns exactly one chunk-store reference: a second stamp for the same address creates a second entry and takes a second chunk-store reference, but no extra room under the bound, which counts addresses. Pulling therefore **continues** while stale, which keeps the node replicating the neighbourhood's data and keeps the sync rate real.

**Push-sync with held chunks.** A storer that holds a chunk pending validation **must not sign a receipt for it**: a receipt promises storage, and the chunk may turn out to be invalid. So the storer path holds the chunk and returns an error, `pushsync: batch not known yet, chunk held`, without writing a receipt. Decided over the alternative of forwarding to another storer:
- an error is what the uploader's push already handles (it tries the next closest peer), and the neighbourhood's other storers validate and sign;
- forwarding from a storer would need a new "closest peer excluding self" path and makes the stale node pay for a forward it cannot verify;
- the cost of the error: the stream resets with no receipt; the origin skiplists the stale node for 5 minutes (`pushsync.go:50`, `:524`) within its 32-error budget (`:56`, `:393`), and a forwarder has a budget of 1 plus its parallel forwards in the neighbourhood (`:383`, `:466-471`), so the uploader (or the forwarder before it) spends one transfer and one retry on the stale node. Stamp bucket counting is untouched until promotion. Meanwhile the held chunk is still served by retrieval, and after catch-up it is promoted into this node's reserve, so the neighbourhood ends up with the chunk on this node too.

**The radius does not drop while stale.** Pulling continues, but the reserve size is not reliable while chunks are held outside it, and the reserve worker lowers the radius when the reserve is below its threshold and the sync rate is 0 (`pkg/storer/reserve.go:297`), every 15 minutes (`node.go:266`). A paused puller (full held area) reports a sync rate of 0 (`puller.go:512`). **While the listener is stale, and for one full sync-rate window (15 minutes) after stale ends, the reserve worker skips the radius decrease.** The stale function reaches the reserve worker where it is started, `localStore.StartReserveWorker(ctx, pullerService, waitNetworkRFunc, nil)` (`node.go:1604`), from the listener built at `node.go:1064`. A radius increase is not blocked: it only evicts.

### 4. The storage lottery does not play while stale

The `isFullySynced` function given to the storage incentives agent (`node.go:1651-1655`) gains one condition: **the listener is not stale and the held index has been fully processed**. Stale ends at catch-up, and validation of held chunks follows, possibly for minutes; until it is done the reserve still lacks chunks the neighbourhood holds, so a sample taken then could disagree and freeze a staked node. The agent already skips a round when that function returns false, with `skipping round because node is not fully synced` (`agent.go:454-457`), and `/redistributionstate` reports `isFullySynced`. The agent caches the value on entering each phase (`SetFullySynced`, `agent.go:229`), so the gate and `/redistributionstate` follow the stale state with a lag of up to one phase; that is acceptable, since a phase is shorter than the 10 minutes before a node becomes stale, and a stall that starts inside a phase would have stopped the node at the same point today.

### 5. The operator is told, for as long as it lasts

- Gauges: `bee_postage_listener_stale` (0 or 1), `bee_postage_listener_seconds_since_progress`, `bee_postage_listener_blocks_behind` (the last known head minus the confirmation depth, minus the listener's next block; reported as unknown, -1, while no `BlockNumber` call has succeeded since the node became stale), and `bee_postage_listener_page_blocks` (the current page).
- A Warning when the node becomes stale: `postage sync stalled; the node stays up and serves content but does not play the storage lottery until the batch store catches up; chunks of batches not seen yet are held and served, and validated once caught up`, with the time since last progress, the blocks behind (if known) and the last error. **The Warning repeats every 30 minutes while stale**, so a node whose RPC endpoint is permanently wrong does not stay degraded with a single line in its log. Each entry into the stale state logs its own Warning: near the head, an endpoint that answers just over every 10 minutes makes the node go stale and recover in a cycle, and each cycle is logged.
- An Info line when stale ends: `postage sync caught up`, with how long the node was stale.
- `/status` reports the degraded state: a field such as `postageSyncStale` with the seconds since progress. `/status` is built from the status protocol's message type (`api/status.go:72`), which `/status/peers` shares, so the field is added **to the API response only, never to the status protocol message** (that would change the wire surface), and is filled only for the local node.
- **`/readiness` is unchanged (decided, see below).** It keeps answering on the startup probe and connected peers (`api/readiness.go:40-62`). A load balancer that saw not-ready would stop sending requests to a node that can still serve content, the opposite of the goal. The stale state is reported in `/status` and the metrics only.

### 6. The node keeps working to recover

- **Growing wait, only while stale.** #578 kept a fixed `backoffTime` (5 s) because `stallingTimeout` bounded how long the node kept trying. Without that bound, a fixed 5 s retry would keep calling an overloaded endpoint for as long as the stall lasts. So, once stale only, the wait after a failed `BlockNumber` or `FilterLogs` doubles from `backoffTime` up to 60 s, and the first applied page resets it to `backoffTime`. Before the node is stale, the wait is exactly as today, so a short outage recovers as fast as now.
- **A smaller page when the log query fails.** When, in one pass, `BlockNumber` succeeds and `FilterLogs` fails (an error answer or the deadline), the next attempt uses half the page, down to a minimum of 100 blocks. After 3 consecutive applied pages at a reduced size, the page doubles again, up to the standard size. A failed `BlockNumber` does not change the page. This applies whether or not the node is stale, which is a change from today before the node is stale: a single failed log query halves the next page. Against a provider that refuses ranges above some size, the page settles into a cycle around that size (for example 625 to 1,250 blocks against a 1,000-block limit, one failed query every 4 pages). The minimum of 100 blocks is 500 s of chain at 5 s blocks and 200 s at 2 s blocks, well above the 5-block batch factor (`defaultBatchFactor`, `listener.go:34`).

### 7. Startup

At startup a full node still waits for the listener before it builds the rest of the node (`batchSvc.Start`), but no longer forever and no longer by failing:

- the wait for `synced` ends either when the listener is caught up, or when the listener becomes stale. The startup wait reads an unbuffered `synced` channel (`batchservice.go:343-345`) whose only senders are the loop (`listener.go:347`, `:387`). A loop blocked in a call cannot send, so a separate watcher goroutine, started with the loop, sends on `synced` under the same `closeOnce` when the listener becomes stale;
- if the listener becomes stale at startup, the node logs the stale Warning and **continues building in the degraded state**: it joins the network and serves what it holds, it accepts and holds chunks as in section 3, the radius decrease is skipped, and the lottery gate is closed (section 4). The listener keeps running and ends the stale state when it catches up.
- `syncStatus.Store(true)` (`node.go:1269`) still runs after the wait, so the `/stamps` endpoints, which `postageSyncStatusCheckHandler` keeps closed until then (`api/postage.go:42-44`), open on stale data. That is documented: buying or topping up a batch still works, and its effect appears once the listener catches up.

So the original #583 scenario, a full node whose catch-up page always fails at startup, no longer loops through restarts off the network: the node comes up degraded and keeps retrying with a smaller page.

### 8. Exit status: non-zero, and only for faults that cannot be recovered

The node no longer stops for a stall. It stops for the unrecoverable listener errors in section 2, and when an operator sets `postage-stall-shutdown` (Configuration). `syncingStopped` carries no error today. The cause is stored in an atomic error on `Bee`, set before the signal; `start.go` reads it after `stop` has run and returns an error wrapping it instead of `nil`, as it already does for a stored build error (`start.go:185-187`). `main` maps:

| Cause | Status | Reason |
|---|---|---|
| `postage-stall-shutdown` reached | **75** (`EX_TEMPFAIL` from `sysexits.h`) | Temporary: a restart may succeed |
| A paused postage contract, seen at runtime (`ErrPostagePaused`) or at startup (`node.go:1217-1223`, today a plain `errors.New("postage contract is paused")` that exits 1) | **75** in both cases | Temporary: the contract can be unpaused; the node restarts and checks again. The startup check returns the same sentinel, so both paths map to one status |
| `processEvents` failure, `ErrParseSnapshot` | **1** | A fault in the node or its data, like any other failure |
| Configuration error (#490) | 78, unchanged | `RestartPreventExitStatus=78` keeps a bad configuration from looping |

75 and 1 are both non-zero, so `Restart=on-failure` and Docker's `on-failure` restart the node; 75 lets an operator tell a chain-side cause from a crash in `systemctl status`. The packaged unit (`packaging/bee.service`) needs no change: `Restart=always` already restarts on any status. The node does not rely on it.

**Windows service:** `p.start` runs in a goroutine (`start.go:490-494`) and nothing stops the service when the node stops on its own, so an exit status cannot be reported to the Windows service manager. This is unchanged by the spec and noted for completeness; with the node no longer stopping for a stall, it matters less.

### 9. What does not change

- The stall timeout of 10 minutes still decides when the node becomes stale.
- `postage-confirmation-depth` and `MaxConfirmationDepth` (`listener.go:48`) are unchanged; their comment, which says the node stops after the stall timeout, is updated to say it becomes stale.
- The startup checks that refuse a batch store from another chain (`node.go:1237-1263`) are unchanged.

## Overload: how the design behaves when the node's own host is the problem

This is the sw-1 case.

1. A listener call that hangs ends at its deadline (30 s or 60 s) instead of blocking for an hour and a half. The error is counted and retried after `backoffTime`, as today, and a failed log query halves the next page.
2. After 10 minutes without an applied page the node becomes stale. It stays on the network, keeps accepting and serving chunks (holding those of unknown batches), keeps its storage radius, skips any lottery round it is selected for, reports the stale state in `/status`, and warns every 30 minutes. Retries slow down to one per minute, which removes most of the listener's own share of the load.
3. When the host recovers, the next attempt succeeds. The node catches up in pages, leaves the stale state when caught up, logs that it recovered, validates the held chunks, and resumes playing.

What it does not do: it does not reduce the load that caused the overload. On sw-1 that load was the reserve eviction (#621, #623). This change only stops the listener from turning a temporary overload into a node that stays down.

## Protocol impact

None. Nothing under `pkg/p2p`, `pkg/swarm` or `pkg/config` changes, and no message, protocol ID or wire constant changes. A stale node holding a push-synced chunk answers with an error and no receipt, the same outcome a stock node gives for an unknown batch; nothing on the wire tells a peer the chunk was held. Retrieval delivers held chunks with the data-only delivery message used today.

**Cost to other nodes:**
- **Pull-sync:** a stale node keeps pulling, so neighbours serve it as today. A held chunk offered again before validation is transferred again (the want check reads the reserve only), at most once per offer; and while the held area is full the node pauses pulling, so neighbours transfer those chunks after recovery instead.
- **Push-sync:** a stale storer holds a chunk of an unknown batch and answers with an error, so the uploader spends one transfer and one retry on it before another storer signs. Today the peers would instead find the node gone after 10 minutes and spend dial attempts on it until they drop it.
- **Held area:** costs the stale node up to about 410 MB of disk while stale, freed after validation.
- **Expired batches:** a stale node may keep chunks of batches that expired during the stall. It does not offer them in pull-sync more than today, since pull-sync serves from the reserve as before; peers that are up to date reject them on stamp validation if they pull them. The amount is bounded by what expired during the stall.

## Measurement

### Setup

One bench node (role `bench-1` or `bench-2`, whichever is not running another experiment). Its `blockchain-rpc-endpoint` points at a small local proxy in front of the real endpoint, with four modes switched at run time:
- **pass:** forwards everything;
- **block:** accepts the connection and never answers;
- **slow:** delays every answer by 120 s, longer than both per-call deadlines;
- **range limit:** answers `eth_getLogs` with a JSON-RPC error when `toBlock - fromBlock` is greater than 1,000, and forwards everything else.

The proxy and the data-directory copies live in the private bench tooling, not in the repository. The node is unstaked; the lottery gate and the puller pause are covered by unit tests, since a bench node cannot be made to be selected on demand.

Conditions are split by when the stall happens, because startup and a running node take different code paths.

### Startup set (data directory restored a few hours behind the chain head before each run)

| Condition | Proxy | Duration |
|---|---|---|
| S1 endpoint unreachable at startup | block for 20 min, then pass | until caught up |
| S2 provider range limit at startup | range limit throughout | 60 min |

### Caught-up set (node started and caught up first, then the proxy is switched)

| Condition | Proxy | Duration |
|---|---|---|
| C1 endpoint unreachable | block for 20 min, then pass | until caught up |
| C2 endpoint hangs | slow for 20 min, then pass | until caught up |
| C3 short outage | block for 3 min, then pass | until caught up |

Each condition runs three times on today's build (`main` before this change) and three times on the new build, alternating builds, with the data directory restored before each startup run.

### Serving and accepting while stale (all stale conditions)

A second node (the other bench role, or a throwaway node on the same host) is connected to the stale node only, and a third node, caught up and in the same neighbourhood, is connected to both. During the stale period the second node:
- **retrieves** 100 chunks the stale node held in its reserve before the stall (known batches), every 5 minutes;
- **pushes** 50 new chunks stamped with a batch the stale node knows, and 50 with a batch bought during the stall (so unknown to the stale node), each into the stale node's neighbourhood;
- **retrieves** the pushed chunks back from the stale node.

With only the stale node connected, the second node's push would end in `ErrWantSelf` and be stored by the second node itself (`pushsync.go:438-447`); the third node is what shows that another storer signs a receipt for the chunks the stale node holds.

Further arms, three runs each:
- **Held area full:** the cap lowered for the run (a test build), filled by pushing unknown-batch chunks; pulling pauses, and resumes when catch-up frees the area.
- **Restart with held entries:** the node is restarted while stale with held chunks, then caught up without becoming stale again; validation still runs at startup and the held gauge reaches 0.
- **Validation:** its duration and the reserve size before and after, for a held area of at least 10,000 chunks.
- **Lottery gate:** `isFullySynced` stays false from catch-up until the held index is empty.

### Recorded per run

- whether the process is still running, and its exit status if not;
- whether the node answers on its API, on `/readiness`, and to the libp2p greeting on its p2p port, every minute;
- the four listener gauges and the listener's backend call and error counters, every 15 s;
- RPC calls per minute seen by the proxy, and the duration of each call;
- the puller's synced counter for both workers, labelled `live` and `historical` (`puller.go:507-512`), and the held, promoted and dropped counters;
- the storage radius, to confirm it does not drop while stale;
- time from the proxy returning to pass until the listener is caught up;
- `isFullySynced` from `/redistributionstate`.

### Acceptance

The new build is accepted when, in all three runs of each condition:
1. **S1, C1, C2:** the node never exits; it answers its API and the libp2p greeting throughout; it keeps reporting ready on `/readiness`, reports the stale state in `/status`, reports `isFullySynced` false, and the storage radius does not drop while stale; and it is caught up within 10 minutes of the proxy returning to pass.
2. **Serving and accepting (S1, C1, C2):** every retrieval of a known chunk succeeds while stale; every pushed chunk of a known batch gets a receipt from the stale node; every pushed chunk of an unknown batch gets no receipt from the stale node, a receipt from the third node, is held, and is retrievable from the stale node while stale; after catch-up the held chunks of the real batch are in the reserve and none remains held; `isFullySynced` stays false until the held index is empty; after a restart with held entries, validation runs and the held gauge reaches 0. Today's build is expected to stop after 10 minutes, so none of this is possible with it.
3. **C2:** no listener call lasts longer than its deadline (30 s or 60 s) as seen by the proxy, and the node becomes stale 10 minutes after the switch, where today's build is expected to hang in the call without becoming stale or stopping.
4. **S1, C1, C2:** RPC calls from the listener while stale are at most about 2 per minute after the wait reaches its 60 s cap, against about 24 per minute today before the stop.
5. **S2:** the node joins the network in the degraded state and reaches the chain head within the 60 minutes, with the page settling at or below 1,250 blocks, where today's build fails to build and restarts in every run.
6. **C3:** time to catch up is within 10 % of today's build, so a short outage is not slowed down.

Unit tests cover what the bench cannot show: the agent skips a round while the listener is stale; a stale node accepts and serves chunks of known batches on push-sync, pull-sync and retrieval; it holds chunks of unknown batches (and of a known batch with an index beyond the known depth) and serves them through retrieval; it signs no receipt for a held chunk and returns an error; pull-sync advances an interval only past chunks stored or held, for live and historical pulling; pulling pauses when the held area is full and resumes when it has room; a full held area refuses new unknown-batch chunks without evicting held ones; cache eviction does not remove a held chunk; after catch-up a valid held chunk inside the radius is promoted to the reserve, one outside it is dropped, and an invalid one is dropped, each releasing its chunk-store reference; a held entry and its chunk-store put commit together; promotion repeated after a crash between its two steps leaves one reserve entry and no held entry; a held entry whose chunk is gone is dropped; validation runs at startup when the held index is not empty, and only once the listener is caught up; held entries of a batch created after the stored chain state survive a restart and are promoted after catch-up; two handlers holding the same address at once claim room once, and the room is released when the last entry for the address leaves; the bound is a claim counted once per address; `isFullySynced` is false while the held index has unprocessed entries; the reserve worker does not lower the radius while stale or for 15 minutes after; a stall marks stale and does not signal `syncingStopped`; stale ends only on the non-paged branch, not on one applied page while behind; `Stale()` becomes true while a call is still in progress; the listener's calls carry deadlines; processing, parse and pause errors still stop the node with the statuses in section 7; the page halves only when `BlockNumber` succeeded and `FilterLogs` failed, never below 100, and doubles back after 3 applied pages; the wait doubles to 60 s only while stale and resets on progress; the Warning repeats every 30 minutes while stale and is logged again on each new entry; the failover gives each attempt its own deadline and does not advance on an expired caller context; startup continues in the degraded state when the listener becomes stale; `exitCode` maps the new causes to 75 and 1 and leaves 78 unchanged.

**A negative result** is any of: the new build exits or stops answering in S1, C1 or C2; a retrieval of a known or held chunk fails while stale; a push of a known-batch chunk gets no receipt, or a push of an unknown-batch chunk gets one; a held chunk of a real batch is missing from the reserve after catch-up; a listener call outlives its deadline; it does not catch up after the proxy returns (for example because the wait cap or the smaller page leaves it permanently behind); it makes no progress in S2; or C3 recovers more slowly. Any of these is recorded in `results.md` and the change is not shipped as is.

### On a host under real overload

The sw-1 radius scenarios (#621) are the natural repeat of the observed case. If a re-run of scenario A happens after this change has merged, the run records for every node whether it stopped, its exit status, its stale periods and the duration of its listener calls. This is reported as supporting evidence only: the bench conditions above decide acceptance, because sw-1 cannot reproduce the same overload three times on demand.

## Rollout and rollback

- **Rollout:** on by default in the release that carries it.
- **Rollback:** `postage-stall-shutdown: 10m` brings back a stop 10 minutes after the node becomes stale, that is, 20 minutes after the last applied page (with exit status 75 instead of 0). Today's timing, a stop 10 minutes after the last applied page, cannot be reproduced exactly, since the setting counts from becoming stale. The per-call deadlines, the held area, the pause of pulling while the held area is full and the lottery gate stay, since they only prevent harm. A full rollback is the previous release, after the held gauge reads 0 (above).
- **Data format:** the batch store and the listener's stored block number are unchanged. The held index is a new namespace in the index store. **No migration is needed:** like the local-ingest records (#326, `pkg/storer/localingest.go`), it is a namespace that starts empty, and the migration steps (`pkg/storer/migration/all_steps.go`) are not involved. **Downgrade only when `bee_postage_held_chunks` reads 0:** an older wasp, or stock Bee, ignores the held keys but never releases the chunk-store references they hold, so up to about 410 MB of disk would be lost for good. Start-up recovery (`validateAndAddLocations`, `pkg/storer/recover.go:67-77`) checks chunk-store entries against their data; the implementation should also teach it about the held index, so a held entry left by a crash is dropped together with its reference.
- The implementation pull request updates `docs/DIFFERENCES.md` (rule 13): the stall behaviour, the degraded state, the exit statuses, the setting, the `/status` field and the new metrics.

## Upstream portability

Verified on `upstream/v2.8.2` (the tag in `.upstream-base`) and `upstream/master`:
- `listener.go` returns `ErrPostageSyncingStalled` after `stallingTimeout` (v2.8.2 lines 260-261, master 269-270) and signals `syncingStopped` (v2.8.2 line 368, master 379);
- `start.go` waits on `SyncingStopped()` (line 103 in both) and then returns `nil` (v2.8.2 line 159);
- upstream `main.go` exits 1 only when `Execute` returns an error (v2.8.2 line 17), so a stall of a running node exits 0;
- `postageSyncingStallingTimeout` is 10 minutes (v2.8.2 `node.go:215`, master `node.go:216`);
- the packaged unit uses `Restart=always` in both refs.

So the stop of a running node on a stall, and its exit status 0, are unmodified upstream behaviour. At startup, upstream also fails the build on a stall, but upstream's `start` does not keep a build error (the #490 change is wasp's), so it exits 0 there too. wasp's changes to these files (#490, #540, #545, #578) do not touch the stall path itself. The design ports as: the per-call deadlines, the stale flag, the growing wait and the page reduction in `listener.go`; the puller wait in `puller.go` and the skipped radius decrease in `pkg/storer/reserve.go`; the per-attempt deadline in the failover; one condition in the `isFullySynced` function and the startup wait in `node.go`; the `/status` field; and the exit status in `start.go` and `main.go`. The exit status part does not depend on wasp's `exitCode` function from #490: upstream would map the causes in its own `main`.

## Configuration

**`postage-stall-shutdown`** (duration). A behaviour switch, not a tuning value: whether the node stops, and if so how long after it **became stale** (the reference point is the moment the stale state begins, that is, 10 minutes after the last applied page).
- **Default: `0`, never stop. Decided by the operator (2026-10-08):** the node should never get truly stuck; while degraded it keeps serving content, does not play the lottery, keeps working to recover, and alerts the operator; it exits non-zero only for faults that cannot be recovered. This departs from rule 8's default of today's behaviour (a stop 10 minutes after the last applied page) by the operator's decision.
- **Raising it, or leaving it at 0,** keeps a node with a stale batch store on the network longer. It accepts and serves chunks, holds those of new batches up to the held-area bound (signing no receipt for them, so uploaders also reach another storer), and skips lottery rounds.
- **Lowering it** to a duration brings back a stop. A short value turns a temporary overload or endpoint outage into a node that leaves the network, and the restart adds load to a host that may already be overloaded. A negative value is refused at start as a configuration error (`node.ErrConfig`, exit 78).

**Not settings (compiled in):** `heldChunksMax` (100,000 chunks, about 410 MB; raising it holds more of a long stall's new data at the cost of disk and a larger spam allowance, lowering it pauses pulling sooner), the per-call deadlines (30 s for `BlockNumber`, 60 s for `FilterLogs`), the 60 s wait cap, the 30-minute repeat of the Warning, the 100-block minimum page and the 3-page growth step. None has a measurement showing it matters yet (rule 8: measure first, expose second). The measurement above records call durations, the RPC call rate and catch-up time that would justify exposing them.

**Operator extra (optional, not relied on):** service units restart by default and start at boot, for example systemd `Restart=always` with the unit enabled. wasp's packaged unit already does this. The node's behaviour above does not depend on it; it only covers the faults that do stop the node.

## Decided by the operator (2026-10-08)

1. **The node never stops for a recoverable stall:** `postage-stall-shutdown` defaults to `0`. While degraded it serves content, does not play the lottery, keeps working to recover (deadlines, backoff, smaller pages) and alerts the operator (gauges, a Warning that repeats while stale, an Info line on recovery, `/status`). A non-zero exit is reserved for faults that cannot be recovered.
2. **A stale node keeps accepting and serving chunks.** Known-batch chunks as today; unknown-batch chunks held, served and validated after catch-up.
3. **Push-sync returns an error for a held chunk, never a receipt**, so the uploader reaches another storer that can validate and sign.
4. **The held-area bound is compiled in until measured** (rule 8).
5. **When the held area is full, new unknown-batch chunks are refused and held ones are kept**, since spam cannot be told from real chunks before validation.
6. **`/readiness` stays unchanged.** A load balancer must keep sending traffic to a node that can still serve content, so the stale state is reported in `/status` and the metrics only.
7. **Service units restart by default** (`Restart=always`, enabled at boot) as an operator extra on top of the node's own behaviour; wasp's packaged unit already does.

## Not in scope

- The load that caused the sw-1 overload: reserve eviction (#621, #623, #628).
- The eviction burst when a long-stale node catches up and many batches expire at once (`batchstore/store.go:298-325`): the same mechanism as #621, handled there.
- A second RPC endpoint or failover policy: `blockchain-rpc-endpoint` already accepts several endpoints.
- The Windows service not stopping when the node stops on its own (`start.go:490-494`).
