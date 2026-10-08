# Keep a node running when postage sync stalls, and make a stop visible to the service manager

Issue: [#583](https://github.com/crtahlin/wasp/issues/583)

## Terms

- **Postage listener:** the loop in `pkg/postage/listener/listener.go` that reads postage contract events (`BatchCreated`, `BatchTopUp`, `BatchDepthIncrease`, `PriceUpdate`, `Paused`) from the chain through the RPC endpoint and applies them to the batch store.
- **Batch store:** the node's local copy of every postage batch and the chain state (block, total amount, current price) that those events produce.
- **Stall:** no successful page of events for `stallingTimeout`, 10 minutes (`postageSyncingStallingTimeout`, `pkg/node/node.go:256`).
- **Page:** one `eth_getLogs` query over a block range: 5,000 blocks with the RPC backend, 50,000 with the snapshot backend (`blockPage`, `blockPageSnapshot`, `listener.go:32-33`).
- **Stale:** the batch store has not been brought up to date for longer than `stallingTimeout`.

## Problem

### What happens today (verified in code on `origin/main` at 9cf512ee)

1. The loop checks `time.Since(lastProgress) >= l.stallingTimeout` at the top of every pass and returns `ErrPostageSyncingStalled` (`listener.go:277-279`). `lastProgress` moves only after a page has been applied (`listener.go:371`).
2. The goroutine that runs the loop logs `failed syncing event listener; shutting down node error` and calls `l.syncingStopped.Signal()` (`listener.go:385-390`).
3. `start` waits on `SyncingStopped()`, logs `syncing has stopped` at debug level and `shutting down...`, and runs the normal shutdown (`cmd/bee/cmd/start.go:114-120`).
4. `RunE` then returns `nil` (`start.go:189`), because only a failed build is kept as an error (`start.go:185-187`, from #490). `main` maps `nil` to exit status 0 (`cmd/bee/main.go:28-31`).

So a stalled postage sync stops the node **and reports success**. The same path is taken by a light node whose background batch sync fails (`pkg/node/node.go:1282`), and by the snapshot backend, which receives the same `syncingStopped` signaller and timeout (`node.go:1196`).

### Two ways into it

**A page that always fails (the original report, from code, not observed).** During catch-up, a provider that limits the block range or the result size of `eth_getLogs` fails the same 5,000-block page every time. The fix in #578 (merge cd539e76) only makes the retries wait `backoffTime` (5 s); it does not change the page, so after 10 minutes the node stops, restarts under `Restart=always`, and meets the same page.

**An overloaded host (observed on 2026-10-08, sw-1, scenario A of #621, data on #622).** Ten nodes evicting about 14 M chunks each at once drove the host to a load average of about 120 on 8 threads, with memory pressure stalls 21 % of the time and I/O pressure stalls 33 % of the time (5-minute "full" averages from `/proc/pressure`). On node 1 the log shows, in this order:
- `getting block number ... failover: all 2 endpoints failed ... context deadline exceeded` (14:45 CEST and again at 16:52; the node logs in UTC, these are converted);
- `failed syncing event listener; shutting down node error ... postage syncing stalled` (16:52:40);
- the unit `inactive (dead)`, `code=exited, status=0/SUCCESS`.

That host's units use `Restart=on-failure`, so the node stayed down. Nothing points at the endpoints themselves: the timeouts fall in the hours when the host was overloaded, and none of the other nine nodes on the host hit the stall limit. The node stopped because its own host was too busy to complete an RPC call in time (a hypothesis consistent with the timing; the endpoints were not monitored separately).

### Why a stop is the wrong answer to both

- A restart does not fix a page the provider always refuses: the node meets the same page again.
- A restart does not fix an overloaded host either. It adds load: the node reopens its store, re-dials its peers and restarts any eviction or sync it was in. On sw-1 the nodes that stayed up recovered without help at about 17:00; node 1, the one the stall check stopped, did not.
- Exit status 0 hides the stop from every service manager that restarts only on failure (systemd `Restart=on-failure`, Docker `restart: on-failure`). wasp's own packaged unit uses `Restart=always` (`packaging/bee.service:12`) and would restart it, but an operator with any other setting gets a node that is silently gone.

## Hypothesis

A stale batch store is a reason to stop **playing the storage lottery**, not a reason to stop **the node**. If the node instead (1) stays on the network, (2) refuses to play while stale, (3) keeps retrying the chain with a wait that grows while the stall lasts, and (4) shrinks the page when only the log query fails, then:

- the overloaded-host case ends by itself when the host recovers, with the node never leaving the network;
- the always-failing-page case makes progress at a smaller page instead of looping through restarts;
- a node that does stop, by the operator's choice, exits non-zero, so every common restart policy brings it back.

### What a stale batch store breaks, and what it does not (from code)

| Function | Effect of a stale batch store | Handled by |
|---|---|---|
| Storing chunks of batches created during the stall | `ValidStamp` returns `batchstore get: not found` (`pkg/postage/stamp.go:193-196`), so push-sync and pull-sync to this node refuse them | Nothing needed: the chunks are refused, not stored wrongly. Peers retry elsewhere (cost to peers, below) |
| Expiring batches | `cleanup` in the batch store runs from new chain state (`batchstore/store.go:296-321`), so expired batches stay valid locally | Not playing the lottery while stale (next row); the node may keep and offer expired chunks for longer |
| Storage lottery sample | `minBatchBalance` uses the stale chain state (`pkg/storageincentives/agent.go:549-555`), and the reserve may hold expired or lack new chunks, so the sample can differ from the neighbourhood's | **Must not play while stale.** A disagreeing reveal freezes a staked node |
| Retrieval of stored chunks | Does not read the batch store | Keeps working |
| Kademlia, hive, pull-sync of existing batches | Do not depend on chain freshness | Keep working |
| The node's own uploads | Stamping reads its own batches from the batch store; a batch topped up or diluted during the stall is seen with its old values | Same as today during the 10 minutes before the stop; acceptable |

## Design

### 1. A stall no longer stops the node by default

`Listen` stops returning `ErrPostageSyncingStalled` when `stallingTimeout` passes. Instead the loop keeps running and the listener records that it is **stale**:

- it sets a stale flag, readable through a method on the listener (for example `Stale() bool`), and a gauge `bee_postage_listener_stale` (0 or 1);
- it logs one Warning when it becomes stale: `postage sync stalled; the node stays up but does not play the storage lottery until the batch store catches up`, with the time since the last applied page and the last error;
- it logs one Info when a page is applied again and the flag clears;
- a gauge `bee_postage_listener_seconds_since_progress` reports the time since the last applied page.

The light-node background path (`node.go:1276-1283`) and the snapshot backend (`node.go:1196`) follow the same rule: a stall marks stale and does not signal `syncingStopped`.

Other errors keep stopping the node as today: a failure to apply events to the batch store (`processEvents` returning an error), `ErrParseSnapshot`, and the postage contract being paused. Those are not caused by a slow or unreachable endpoint, and continuing would apply nothing.

### 2. The storage lottery does not play while stale

The `isFullySynced` function given to the storage incentives agent (`node.go:1650-1654`) gains one condition: the postage listener is not stale. The agent already skips a round when that function returns false, with `skipping round because node is not fully synced` (`agent.go:454-457`), and `/redistributionstate` already reports `isFullySynced`. No change in the agent itself is needed, and the existing skip is the one a staked node already uses during a reserve fill.

A node that is selected while stale therefore skips the round without committing, which costs it that round's reward but never a freeze.

### 3. The wait between attempts grows while the stall lasts

#578 deliberately kept a fixed `backoffTime` (5 s) because `stallingTimeout` bounded how long the node kept trying. Without that bound, a fixed 5 s retry would keep calling an overloaded endpoint, or an endpoint on an overloaded host, every 5 s for as long as the stall lasts. So, once stale only:

- the wait after a failed `BlockNumber` or `FilterLogs` doubles from `backoffTime` up to a cap of 60 s (compiled in; see Configuration for why it is not a setting);
- the first applied page resets it to `backoffTime`.

Before the node is stale, behaviour is exactly as today, so a short outage recovers as fast as it does now.

### 4. A failing log query shrinks the page

This is the fix #583 proposed. When, in one pass, `BlockNumber` succeeds and `FilterLogs` fails, the endpoint is reachable but did not answer that page. The next attempt uses half the page, down to a minimum of 100 blocks. After 3 consecutive applied pages at a reduced size, the page doubles again, up to the standard size. A failed `BlockNumber` does not change the page: that failure says nothing about the page.

Both reasons a page can fail are helped by this: a provider limit on range or result size, and a slow endpoint (or a busy host) that cannot finish a large query within its timeout. A gauge `bee_postage_listener_page_blocks` reports the current page.

The minimum of 100 blocks keeps catch-up from turning into one call per block: 100 blocks is 500 s of chain at 5 s blocks and 200 s at 2 s blocks, well above the 5-block batch factor (`defaultBatchFactor`, `listener.go:34`).

### 5. When the node does stop, it exits non-zero

A new setting `postage-stall-shutdown` (a duration, see Configuration) lets an operator keep the old behaviour of stopping. When it fires, or when any other error from section 1 stops the node, `start` returns an error that wraps a new sentinel, for example `node.ErrPostageSyncStopped`, instead of `nil`, and `main` maps it to exit status **75** (`EX_TEMPFAIL` from `sysexits.h`, "temporary failure, try again later").

- 75 is non-zero, so `Restart=on-failure` and Docker's `on-failure` restart the node.
- It is not 78 (`EX_CONFIG`), which the packaged unit's `RestartPreventExitStatus=78` uses to stop restarting on a configuration error (#490, `docs/experiments/config-errors/spec.md`); a postage stall is not a configuration error and must keep restarting.
- It is distinct from 1, so an operator can tell this cause from a crash in `systemctl status`.

The packaged unit (`packaging/bee.service`) needs no change: `Restart=always` already restarts on any status. The node does not rely on it.

### 6. What does not change

- The stall timeout of 10 minutes still decides when the node counts as stale.
- `postage-confirmation-depth` and its limit (`MaxConfirmationDepth`, `listener.go:48`) are unchanged; their comment, which says the node stops after the stall timeout, is updated to say it becomes stale.
- The startup checks that refuse a batch store from another chain (`node.go:1237-1263`) are unchanged.

## Overload: how the design behaves when the node's own host is the problem

This is the sw-1 case. The RPC calls time out because the host is too busy, not because the endpoint is.

1. For the first 10 minutes nothing changes: retries every 5 s, as today.
2. After 10 minutes the node becomes stale. It stays on the network and keeps serving retrieval. It skips any lottery round it is selected for. Retries slow down to one per minute, which removes most of the listener's own share of the load (one `BlockNumber` and one `FilterLogs` per attempt).
3. If `FilterLogs` alone times out on a large catch-up page, the page shrinks, so a call that the busy host can complete is found.
4. When the host recovers, the next attempt succeeds, the page grows back, and the stale flag clears once a page is applied. The node catches up the blocks it missed in pages, as after any outage.

What it does not do: it does not reduce the load that caused the overload. On sw-1 that load was the reserve eviction (#621, #623). This change only stops the stall check from turning a temporary overload into a node that stays down.

## Protocol impact

None. Nothing under `pkg/p2p`, `pkg/swarm` or `pkg/config` changes, and no message, protocol ID or wire constant changes. A stale node refuses chunks of batches it does not know with the same refusal a stock node gives for an unknown batch.

**Cost to other nodes:**
- Push-sync and pull-sync of chunks from batches created during the stall are refused by the stale node, so peers spend a retry and send them elsewhere. Today those peers would instead find the node gone entirely after 10 minutes and spend dial attempts on it until they drop it.
- A stale node may keep chunks of batches that expired during the stall and offer them in pull-sync; peers that are up to date reject them on stamp validation after transferring them. This costs them bandwidth for as long as the stall lasts. The amount is bounded by what expired during the stall.

## Measurement

### Setup

One bench node (role `bench-1` or `bench-2`, whichever is not running another experiment), started from a copy of its data directory a few hours behind the chain head, so the listener has to page. Its `blockchain-rpc-endpoint` points at a small local proxy in front of the real endpoint. The proxy has four modes, switched at run time:
- **pass:** forwards everything;
- **block:** accepts the connection and never answers, so calls time out;
- **slow:** delays every answer by 30 s;
- **range limit:** answers `eth_getLogs` with a JSON-RPC error when `toBlock - fromBlock` is greater than 1,000, and forwards everything else.

The proxy and the data-directory copy live in the private bench tooling, not in the repository. The node is unstaked; the lottery gate is tested in unit tests (below), since a bench node cannot be made to be selected on demand.

### Conditions

Each condition runs three times on today's build (`main` before this change) and three times on the new build, alternating builds, with the data directory restored before each run.

| Condition | Proxy | Duration |
|---|---|---|
| C1 endpoint unreachable | block for 20 min, then pass | until caught up |
| C2 endpoint slow | slow for 20 min, then pass | until caught up |
| C3 provider range limit | range limit throughout | 60 min |
| C4 short outage | block for 3 min, then pass | until caught up |

### Recorded per run

- whether the process is still running, and its exit status if not;
- whether the node answers on its API and to the libp2p greeting on its p2p port, every minute;
- `bee_postage_listener_stale`, `..._seconds_since_progress`, `..._page_blocks` and the listener's backend call and error counters, every 15 s;
- RPC calls per minute seen by the proxy;
- time from the proxy returning to pass until the batch store reaches the chain head (`/chainstate` block against the endpoint's block number minus the confirmation depth);
- `isFullySynced` from `/redistributionstate`.

### Acceptance

The new build is accepted when, in all three runs of each condition:
1. **C1, C2:** the node never exits, keeps answering its API and the libp2p greeting, reports `isFullySynced` false while stale, and catches up within 10 minutes of the proxy returning to pass.
2. **C1, C2:** RPC calls from the listener while stale are at most about 2 per minute (one `BlockNumber`, one `FilterLogs` per attempt at the 60 s cap), against about 24 per minute today before the stop.
3. **C3:** the node reaches the chain head within the 60 minutes with the page at 1,000 blocks or below, where today's build stops after 10 minutes in every run.
4. **C4:** time to catch up is within 10 % of today's build, so a short outage is not slowed down.

Unit tests cover what the bench cannot show: the agent skips a round while the listener is stale; a stall marks stale and does not signal `syncingStopped`; processing and parse errors still stop the node; the page halves only when `BlockNumber` succeeded and `FilterLogs` failed, never below 100, and doubles back after 3 applied pages; the wait doubles to 60 s only while stale and resets on progress; `postage-stall-shutdown` stops the node and the process exits 75; `exitCode` maps the new sentinel to 75 and leaves 78 and 1 unchanged.

**A negative result** is any of: the new build still exits in C1 or C2; it does not catch up after the proxy returns (for example because the wait cap or the shrunken page leaves it permanently behind); it shrinks the page and still makes no progress in C3; or C4 recovers more slowly. Any of these is recorded in `results.md` and the change is not shipped as is.

### On a host under real overload

The sw-1 radius scenarios (#621) are the natural repeat of the observed case. If a re-run of scenario A happens after this change has merged, the run records for every node whether it stopped, its exit status and its stale periods. This is reported as supporting evidence only: the bench conditions above decide acceptance, because sw-1 cannot reproduce the same overload three times on demand.

## Rollout and rollback

- **Rollout:** on by default in the release that carries it. An operator who wants today's behaviour sets `postage-stall-shutdown: 10m`.
- **Rollback:** `postage-stall-shutdown: 10m` restores the stop after 10 minutes (with exit status 75 instead of 0). A full rollback is the previous release.
- No data format changes: the batch store and the listener's stored block number are unchanged, so the node can move between versions freely.
- The implementation pull request updates `docs/DIFFERENCES.md` (rule 13): the stall behaviour, the exit status, the setting and the new metrics.

## Upstream portability

Verified on `upstream/v2.8.2` (the tag in `.upstream-base`) and `upstream/master`:
- `listener.go` returns `ErrPostageSyncingStalled` after `stallingTimeout` (v2.8.2 lines 260-261, master 269-270) and signals `syncingStopped` (v2.8.2 line 368, master 379);
- `start.go` waits on `SyncingStopped()` (v2.8.2 and master line 103) and then returns `nil` (v2.8.2 line 159);
- upstream `main.go` exits with 1 only when `Execute` returns an error (v2.8.2 line 17), so the stall path exits 0;
- `postageSyncingStallingTimeout` is 10 minutes (v2.8.2 `node.go:215`);
- the packaged unit uses `Restart=always` in both refs.

So the stop on a stall and its exit status 0 are unmodified upstream behaviour. wasp's changes to these files (#540, #545, #578, #490) do not touch this path. The design ports as: the stale flag and growing wait in `listener.go`, one condition in the `isFullySynced` function in `node.go`, the page reduction in `listener.go`, and the exit status in `start.go` and `main.go`. The exit status part does not depend on wasp's `exitCode` function from #490: upstream would map the sentinel in its own `main`.

## Configuration

**`postage-stall-shutdown`** (duration). How long the postage sync may stay stale before the node stops with exit status 75.
- **Default:** `0`, meaning never stop; the node stays up and stale until the batch store catches up. **This differs from today's behaviour** (stop after 10 minutes), which rule 8 would keep as the default; see the open question below.
- **Raising it** (or leaving it at 0) keeps a node with a stale batch store on the network longer. It serves retrieval but refuses chunks of new batches, skips lottery rounds, and may offer chunks of batches that expired meanwhile, which costs peers that pull them bandwidth.
- **Lowering it** to a duration brings back a stop. With a short value, a temporary overload or endpoint outage takes the node off the network and the restart adds load to a host that may already be overloaded; values under the stall timeout (10 minutes) are refused at start, since the node cannot be stale sooner.

**Not settings (compiled in):** the 60 s wait cap, the 100-block minimum page and the 3-page growth step. None has a measurement showing it matters yet (rule 8: measure first, expose second). The measurement above records the RPC call rate and catch-up time that would justify exposing them.

## Open questions for the operator

1. **Default of `postage-stall-shutdown`:** `0` (never stop, the recommendation here, because a stop helps neither cause) or `10m` (today's timing, now with a non-zero exit status). Rule 8 asks for the current value by default unless you decide otherwise.
2. **The sw-1 units** use `Restart=on-failure`. Independently of this change, should they move to `Restart=always` like the packaged unit? That is a server setting and only an operator extra; the change above works without it.

## Not in scope

- The load that caused the sw-1 overload: reserve eviction (#621, #623, #628).
- A second RPC endpoint or failover policy: `blockchain-rpc-endpoint` already accepts several endpoints, and the failover itself was not at fault.
- Checking the chain state for staleness in other consumers of the batch store beyond the lottery.
