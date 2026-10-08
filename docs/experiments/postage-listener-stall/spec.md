# Keep a node running when postage sync stalls, and make a stop visible to the service manager

Issue: [#583](https://github.com/crtahlin/wasp/issues/583)

## Terms

- **Postage listener:** the loop in `pkg/postage/listener/listener.go` that reads postage contract events (`BatchCreated`, `BatchTopUp`, `BatchDepthIncrease`, `PriceUpdate`, `Paused`) from the chain through the RPC endpoint and applies them to the batch store.
- **Batch store:** the node's local copy of every postage batch and the chain state (block, total amount, current price) that those events produce.
- **Page:** one `eth_getLogs` query over a block range: 5,000 blocks with the RPC backend, 50,000 with the snapshot backend (`blockPage`, `blockPageSnapshot`, `listener.go:32-33`).
- **Caught up:** the listener has applied a page in the non-paged branch, that is, a page whose range reached the confirmed head because the remaining range was smaller than one page (`to-from < pageSize`, `listener.go:343-348`). Taking the branch is not enough; the page must also be applied.
- **Last progress:** the time the listener last applied a page (`lastProgress`, `listener.go:267`, `:371`).
- **Stale:** no applied page for `stallingTimeout`, 10 minutes (`postageSyncingStallingTimeout`, `pkg/node/node.go:256`), **or** not caught up since the node last became stale. Stale ends only when the listener is caught up again (section 2).
- **Degraded:** the node's state while stale: it stays on the network and serves content, does not play the storage lottery, does not pull chunks (neither live nor historical pull-sync), does not lower its storage radius, and keeps working to recover.

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

A stale batch store is a reason to stop **playing the storage lottery** and **pulling chunks**, not a reason to stop **the node**. If the node instead stays on the network in a degraded state, keeps actively working to recover (per-call deadlines, a wait that grows while the stall lasts, a smaller page when the log query fails), alerts the operator, and leaves the degraded state only when caught up, then:

- the overloaded-host case ends by itself when the host recovers, without the node ever leaving the network;
- the always-failing-page case makes progress at a smaller page instead of looping through restarts;
- a hanging RPC call can no longer freeze the listener;
- the node never gets truly stuck; it exits only on a fault it cannot recover from, and then with a non-zero status.

### What a stale batch store affects (from code)

| Function | Effect of a stale batch store | Handled by |
|---|---|---|
| Pull-sync of chunks of batches created during the stall | The peer transfers the chunk first (`pullsync.go:299-311` checks only `ReserveHas`); `ValidStamp` then returns not found (`pkg/postage/stamp.go:193-196`), the chunk is dropped (`pullsync.go:358-362`), and the puller still records the interval as synced (`puller.go:518-525`), so the chunk is **never pulled again**. A reserve that lacks those chunks gives a sample that can disagree with the neighbourhood after recovery | **The puller pauses all pulling, live and historical, while stale** (section 3): no interval advances, nothing is lost, and the pull resumes from the same point after recovery. Live pulling must pause too: it is what carries the chunks of batches created during the stall |
| Push-sync of chunks of new batches | Refused on the same stamp check; the pusher retries elsewhere | Nothing needed: refused, not stored wrongly (cost to peers, below) |
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

So `call` gives each endpoint attempt its own deadline (the caller's remaining time divided by the endpoints still to try, with a floor of 10 s), derived from the caller's context, and **does not advance** when the caller's own context has ended (`ctx.Err() != nil`), returning that error instead. This was chosen over only stopping on `ctx.Err() != nil`: that alternative lets the first endpoint's hang use the whole deadline, so a second, healthy endpoint would never be tried within the call. With per-attempt deadlines a hanging endpoint costs one share and the healthy one is still reached in the same call. Callers without a deadline (the transaction service today) keep today's behaviour, since there is no remaining time to divide. The comments that describe the failover outcome (`failover.go:8-11`, `chain.go:86-88`) are updated, since a stall no longer stops the node.

### 2. A stall marks the node stale instead of stopping it

The listener stores `lastProgress` and the caught-up state in atomics, and computes `Stale()` when it is read, not inside the loop:

- **stale begins** when `time.Since(lastProgress) >= stallingTimeout`;
- **stale ends** only when a page in the non-paged branch (`to-from < pageSize`) has been applied, that is, when the listener is caught up again. One applied page while thousands of blocks behind does not end it, and an endpoint that lets one page through every 9 minutes cannot keep the node out of the stale state.

Because `Stale()` is computed on read, a call that hangs (until its deadline in section 1) cannot keep the flag from being set. The stall check that returns `ErrPostageSyncingStalled` is removed from the loop.

**Other errors keep stopping the node, as faults that cannot be recovered by waiting:** a failure to apply events to the batch store (`processEvents` returning an error), `ErrParseSnapshot`, and `ErrPostagePaused` (the postage contract is paused). Section 7 gives their exit status.

The same rule applies to the light-node background path (`node.go:1276-1283`) and the snapshot backend (`node.go:1196`): a stall marks stale and does not signal `syncingStopped`.

### 3. The puller pauses all pulling while stale, and the radius does not drop

The puller already pauses while a reserve sample runs, through `waitWhileSampling` (`puller.go:472`). That call sits in the sync loop shared by live and historical workers, with no `isHistorical` check, so it pauses **both**. The wait is extended to also wait while the postage listener is stale, and it must keep pausing both kinds: live pull-sync is what carries the chunks of batches created during the stall, so an `isHistorical` check here would bring the loss back (`puller.go:518-525`). The comment at `puller.go:468-471`, which says only historical pulling pauses, is corrected in the implementation.

While paused, no interval is advanced, so no chunk of a batch the node has not yet seen is dropped and marked synced; after recovery, pulling resumes from the same intervals and fetches those chunks with their batches known. The reserve therefore does not grow while stale. Push-sync and retrieval are separate protocols and keep working.

A paused puller reports a sync rate of 0, which the reserve worker reads as "nothing left to pull" and answers by lowering the radius (`reserve.go:297`). **While the listener is stale, the reserve worker skips the radius decrease.** It resumes its normal check once the node is caught up and the puller has run again. A radius increase is not blocked: it only evicts.

### 4. The storage lottery does not play while stale

The `isFullySynced` function given to the storage incentives agent (`node.go:1651-1655`) gains one condition: the listener is not stale. The agent already skips a round when that function returns false, with `skipping round because node is not fully synced` (`agent.go:454-457`), and `/redistributionstate` reports `isFullySynced`. The agent caches the value on entering each phase (`SetFullySynced`, `agent.go:229`), so the gate and `/redistributionstate` follow the stale state with a lag of up to one phase; that is acceptable, since a phase is shorter than the 10 minutes before a node becomes stale, and a stall that starts inside a phase would have stopped the node at the same point today.

### 5. The operator is told, for as long as it lasts

- Gauges: `bee_postage_listener_stale` (0 or 1), `bee_postage_listener_seconds_since_progress`, `bee_postage_listener_blocks_behind` (the last known head minus the confirmation depth, minus the listener's next block; reported as unknown, -1, while no `BlockNumber` call has succeeded since the node became stale), and `bee_postage_listener_page_blocks` (the current page).
- A Warning when the node becomes stale: `postage sync stalled; the node stays up and serves content but does not play the storage lottery or pull chunks until the batch store catches up`, with the time since last progress, the blocks behind (if known) and the last error. **The Warning repeats every 30 minutes while stale**, so a node whose RPC endpoint is permanently wrong does not stay degraded with a single line in its log. Each entry into the stale state logs its own Warning: near the head, an endpoint that answers just over every 10 minutes makes the node go stale and recover in a cycle, and each cycle is logged.
- An Info line when stale ends: `postage sync caught up`, with how long the node was stale.
- `/status` reports the degraded state: a field such as `postageSyncStale` with the seconds since progress. `/status` is built from the status protocol's message type (`api/status.go:72`), which `/status/peers` shares, so the field is added **to the API response only, never to the status protocol message** (that would change the wire surface), and is filled only for the local node.
- **`/readiness` is unchanged (decided, see below).** It keeps answering on the startup probe and connected peers (`api/readiness.go:40-62`). A load balancer that saw not-ready would stop sending requests to a node that can still serve content, the opposite of the goal. The stale state is reported in `/status` and the metrics only.

### 6. The node keeps working to recover

- **Growing wait, only while stale.** #578 kept a fixed `backoffTime` (5 s) because `stallingTimeout` bounded how long the node kept trying. Without that bound, a fixed 5 s retry would keep calling an overloaded endpoint for as long as the stall lasts. So, once stale only, the wait after a failed `BlockNumber` or `FilterLogs` doubles from `backoffTime` up to 60 s, and the first applied page resets it to `backoffTime`. Before the node is stale, the wait is exactly as today, so a short outage recovers as fast as now.
- **A smaller page when the log query fails.** When, in one pass, `BlockNumber` succeeds and `FilterLogs` fails (an error answer or the deadline), the next attempt uses half the page, down to a minimum of 100 blocks. After 3 consecutive applied pages at a reduced size, the page doubles again, up to the standard size. A failed `BlockNumber` does not change the page. This applies whether or not the node is stale, which is a change from today before the node is stale: a single failed log query halves the next page. Against a provider that refuses ranges above some size, the page settles into a cycle around that size (for example 625 to 1,250 blocks against a 1,000-block limit, one failed query every 4 pages). The minimum of 100 blocks is 500 s of chain at 5 s blocks and 200 s at 2 s blocks, well above the 5-block batch factor (`defaultBatchFactor`, `listener.go:34`).

### 7. Startup

At startup a full node still waits for the listener before it builds the rest of the node (`batchSvc.Start`), but no longer forever and no longer by failing:

- the wait for `synced` ends either when the listener is caught up, or when the listener becomes stale. The startup wait reads an unbuffered `synced` channel (`batchservice.go:343-345`) whose only senders are the loop (`listener.go:347`, `:387`). A loop blocked in a call cannot send, so a separate watcher goroutine, started with the loop, sends on `synced` under the same `closeOnce` when the listener becomes stale;
- if the listener becomes stale at startup, the node logs the stale Warning and **continues building in the degraded state**: it joins the network and serves what it holds, the puller starts paused and the radius decrease is skipped (section 3), and the lottery gate is closed (section 4). The listener keeps running and ends the stale state when it catches up.
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
2. After 10 minutes without an applied page the node becomes stale. It stays on the network and keeps serving retrieval. It pauses pulling, keeps its storage radius, skips any lottery round it is selected for, reports the stale state in `/status`, and warns every 30 minutes. Retries slow down to one per minute, which removes most of the listener's own share of the load.
3. When the host recovers, the next attempt succeeds. The node catches up in pages, leaves the stale state when caught up, logs that it recovered, and resumes pulling and playing.

What it does not do: it does not reduce the load that caused the overload. On sw-1 that load was the reserve eviction (#621, #623). This change only stops the listener from turning a temporary overload into a node that stays down.

## Protocol impact

None. Nothing under `pkg/p2p`, `pkg/swarm` or `pkg/config` changes, and no message, protocol ID or wire constant changes. A stale node refuses push-synced chunks of batches it does not know with the same refusal a stock node gives for an unknown batch.

**Cost to other nodes:**
- **Pull-sync:** while stale, the node does not pull chunks, so it asks its neighbours for nothing during that time and catches up afterwards, which costs them the same transfer later. Without the pause (today, during the 10 minutes before a stop) a peer transfers a chunk of an unknown batch that the node then drops; the pause removes that waste.
- **Push-sync:** chunks of batches created during the stall are refused, so the pusher retries them elsewhere. Today the peers would instead find the node gone after 10 minutes and spend dial attempts on it until they drop it.
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

### Recorded per run

- whether the process is still running, and its exit status if not;
- whether the node answers on its API, on `/readiness`, and to the libp2p greeting on its p2p port, every minute;
- the four listener gauges and the listener's backend call and error counters, every 15 s;
- RPC calls per minute seen by the proxy, and the duration of each call;
- the puller's synced counter for both workers, labelled `live` and `historical` (`puller.go:507-512`), to confirm that both pause;
- the storage radius, to confirm it does not drop while stale;
- time from the proxy returning to pass until the listener is caught up;
- `isFullySynced` from `/redistributionstate`.

### Acceptance

The new build is accepted when, in all three runs of each condition:
1. **S1, C1, C2:** the node never exits; it answers its API and the libp2p greeting throughout; it keeps reporting ready on `/readiness`, reports the stale state in `/status`, reports `isFullySynced` false, both puller counters stay flat and the storage radius does not drop while stale; and it is caught up within 10 minutes of the proxy returning to pass.
2. **C2:** no listener call lasts longer than its deadline (30 s or 60 s) as seen by the proxy, and the node becomes stale 10 minutes after the switch, where today's build is expected to hang in the call without becoming stale or stopping.
3. **S1, C1, C2:** RPC calls from the listener while stale are at most about 2 per minute after the wait reaches its 60 s cap, against about 24 per minute today before the stop.
4. **S2:** the node joins the network in the degraded state and reaches the chain head within the 60 minutes, with the page settling at or below 1,250 blocks, where today's build fails to build and restarts in every run.
5. **C3:** time to catch up is within 10 % of today's build, so a short outage is not slowed down.

Unit tests cover what the bench cannot show: the agent skips a round while the listener is stale; the puller does not advance an interval while stale, for **live** pulling as well as historical (a chunk of an unknown batch offered by live pull-sync is not dropped and marked synced); the reserve worker does not lower the radius while stale; a stall marks stale and does not signal `syncingStopped`; stale ends only on the non-paged branch, not on one applied page while behind; `Stale()` becomes true while a call is still in progress; the listener's calls carry deadlines; processing, parse and pause errors still stop the node with the statuses in section 7; the page halves only when `BlockNumber` succeeded and `FilterLogs` failed, never below 100, and doubles back after 3 applied pages; the wait doubles to 60 s only while stale and resets on progress; the Warning repeats every 30 minutes while stale and is logged again on each new entry; the failover gives each attempt its own deadline and does not advance on an expired caller context; startup continues in the degraded state when the listener becomes stale; `exitCode` maps the new causes to 75 and 1 and leaves 78 unchanged.

**A negative result** is any of: the new build exits or stops answering in S1, C1 or C2; a listener call outlives its deadline; it does not catch up after the proxy returns (for example because the wait cap or the smaller page leaves it permanently behind); it makes no progress in S2; or C3 recovers more slowly. Any of these is recorded in `results.md` and the change is not shipped as is.

### On a host under real overload

The sw-1 radius scenarios (#621) are the natural repeat of the observed case. If a re-run of scenario A happens after this change has merged, the run records for every node whether it stopped, its exit status, its stale periods and the duration of its listener calls. This is reported as supporting evidence only: the bench conditions above decide acceptance, because sw-1 cannot reproduce the same overload three times on demand.

## Rollout and rollback

- **Rollout:** on by default in the release that carries it.
- **Rollback:** `postage-stall-shutdown: 10m` brings back a stop 10 minutes after the node becomes stale, that is, 20 minutes after the last applied page (with exit status 75 instead of 0). Today's timing, a stop 10 minutes after the last applied page, cannot be reproduced exactly, since the setting counts from becoming stale; the per-call deadlines, the puller pause while stale and the lottery gate stay, since they only prevent harm. A full rollback is the previous release.
- No data format changes: the batch store and the listener's stored block number are unchanged, so the node can move between versions freely.
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
- **Raising it, or leaving it at 0,** keeps a node with a stale batch store on the network longer. It serves retrieval, refuses chunks of new batches (peers push them elsewhere), pauses pulling (its neighbours transfer those chunks later instead of now) and skips lottery rounds.
- **Lowering it** to a duration brings back a stop. A short value turns a temporary overload or endpoint outage into a node that leaves the network, and the restart adds load to a host that may already be overloaded. A negative value is refused at start as a configuration error (`node.ErrConfig`, exit 78).

**Not settings (compiled in):** the per-call deadlines (30 s for `BlockNumber`, 60 s for `FilterLogs`), the 60 s wait cap, the 30-minute repeat of the Warning, the 100-block minimum page and the 3-page growth step. None has a measurement showing it matters yet (rule 8: measure first, expose second). The measurement above records call durations, the RPC call rate and catch-up time that would justify exposing them.

**Operator extra (optional, not relied on):** service units restart by default and start at boot, for example systemd `Restart=always` with the unit enabled. wasp's packaged unit already does this. The node's behaviour above does not depend on it; it only covers the faults that do stop the node.

## Decided by the operator (2026-10-08)

1. **The node never stops for a recoverable stall:** `postage-stall-shutdown` defaults to `0`. While degraded it serves content, does not play the lottery, keeps working to recover (deadlines, backoff, smaller pages) and alerts the operator (gauges, a Warning that repeats while stale, an Info line on recovery, `/status`). A non-zero exit is reserved for faults that cannot be recovered.
2. **`/readiness` stays unchanged.** A load balancer must keep sending traffic to a node that can still serve content, so the stale state is reported in `/status` and the metrics only.
3. **Service units restart by default** (`Restart=always`, enabled at boot) as an operator extra on top of the node's own behaviour; wasp's packaged unit already does.

## Not in scope

- The load that caused the sw-1 overload: reserve eviction (#621, #623, #628).
- The eviction burst when a long-stale node catches up and many batches expire at once (`batchstore/store.go:298-325`): the same mechanism as #621, handled there.
- A second RPC endpoint or failover policy: `blockchain-rpc-endpoint` already accepts several endpoints.
- The Windows service not stopping when the node stops on its own (`start.go:490-494`).
