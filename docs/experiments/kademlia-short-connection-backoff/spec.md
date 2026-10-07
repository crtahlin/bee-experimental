# Kademlia: back off from a peer that drops us right after connecting

Issue: [#607](https://github.com/crtahlin/wasp/issues/607). Type: fix. Affects upstream.

## Terms

- **Dial:** an outbound connection attempt that kademlia's manage loop starts to a known peer.
- **Retry entry (`waitNext`):** kademlia's in-memory record per peer address of when it may be dialled again (`tryAfter`) and how many dials to it have failed (`failedAttempts`). It lives in `pkg/topology/kademlia/internal/waitnext` and is not stored on disk.
- **`ShortRetry` and `TimeToRetry`:** kademlia's retry intervals, 10 s and 20 s by default (`defaultShortRetry`, `defaultTimeToRetry` in `pkg/topology/kademlia/kademlia.go`).
- **Pruning a peer from the address book:** after `maxConnAttempts` (4) failed dials, or `maxNeighborAttempts` (6) for a peer at or above the node's depth, kademlia removes the peer from its address book and its known-peer list. The peer comes back when another peer announces it again through the hive protocol.
- **Connection breaker:** the libp2p service's global dial breaker. While it is open, every dial returns `p2p.ConnectionBackoffError` with the time it closes (`pkg/p2p/libp2p/libp2p.go`, around line 1100).
- **Short-lived connection:** in this spec, a connection kademlia counted that ends less than `stableConnection` (one minute) after it was established.
- Rules 6, 7, 8, 11 and 13 are those of `AGENTS.md`: the frozen wire surface, three runs per condition, measuring before adding a setting, tagging defects that are also upstream, and keeping `docs/DIFFERENCES.md` current.

## Problem

### What was seen

In the 24-hour baseline on [#599](https://github.com/crtahlin/wasp/issues/599) (10 nodes on sw-1 plus stake-1, `main` builds, 6 to 7 October 2026), one sw-1 node (node 6) counted **17,673 kademlia inbound disconnects and 6,464 libp2p disconnects in a day**. Most nodes on the same machine counted a few hundred; the lowest was 99. These are counter readings, so they are measured.

The kademlia counter overstates real disconnects: one disconnect can call `Kad.Disconnected` twice, once from libp2p's own handler when the remote closes the connection and once from a `Disconnect` that kademlia itself calls (see point 5 below). It is used here only to compare nodes and builds with each other.

The issue records two further observations on that node, both from its logs (measured):
- Over 6 hours, 2,753 `could not connect to peer` lines, mostly for 4 peers. The worst peer had 508 failed dials in 6 hours, **about 85 an hour**, one every 25 to 70 seconds, with no longer gap. Two of the 4 are in its neighbourhood.
- With debug logging for 9 minutes, one cycle per neighbour about every 25 seconds: most dials fail during setup (the remote side drops the connection and the pricing announcement or the handshake close fails), and about one in three succeeds and **is closed by the remote side within one second**.

**Not verified:** that this cycle accounts for all, or most, of node 6's 17,673 disconnects. The logs show the cycle for 4 peers; nobody has counted the disconnects per peer. The measurement below settles it.

**Not known:** why the remote side drops the connection. A remote bin that is full would fit. It is not visible from our side and this spec does not depend on it.

**Not known:** whether the connection breaker opened on node 6 during the observation. Nothing recorded it. Point 6 below matters only if it did.

### Why the loop never stops (verified in code)

All references are to `pkg/topology/kademlia/kademlia.go` on `origin/main` (`a6d045a0`):

1. **A failed dial** (`connect`, line 1128 onward) increases `failedAttempts` in the retry entry and sets `tryAfter` to now plus `TimeToRetry` (line 1157). At `maxConnAttempts`, or `maxNeighborAttempts` for a neighbour, the peer is pruned from the address book and its retry entry is deleted (lines 1149 to 1155).
2. **A successful dial** sets the retry entry to `tryAfter` now plus `ShortRetry` **with `failedAttempts` 0** (line 467: `k.waitNext.Set(peer.addr, time.Now().Add(k.opt.ShortRetry), 0)`).
3. **A disconnect** (`Disconnected`, line 1366) sets only `tryAfter`, to now plus `TimeToRetry`, and keeps `failedAttempts` as it is.
4. **An inbound connection** (`onConnected`, line 1351) deletes the retry entry.
5. **A connection that fails kademlia's own peer announcement** (`Announce`, lines 1238 to 1243, called at the end of `connect`) is disconnected by kademlia, and `connect` returns the error. The dial handler's last error branch (line 462) only logs it. libp2p's `Disconnect` calls `notifier.Disconnected` synchronously, also when the remote side has already gone (`pkg/p2p/libp2p/libp2p.go` lines 1254 to 1281), so kademlia's `Disconnected` runs before `connect` returns. Today the count is neither increased nor reset on this path.
6. **A dial refused by the connection breaker** sets `failedAttempts` to 0 (lines 1132 to 1137, then line 1157 with `retryTime` from the error). This is a fourth path that resets the count.

So a peer that accepts the connection and drops it at once resets the failure count every time it accepts. With roughly one success in three dials, the count never reaches 4 or 6, the peer is never pruned, and it is dialled again about 20 s after each event, plus up to 15 s until the manage loop's next pass. That is the 25-second cycle in the logs. A 25-second cycle would give about 140 dials an hour; the measured worst peer had about 85 failed dials an hour, with the successful ones between them not counted in that figure.

Each cycle costs a handshake on both sides and a disconnect. On our side a disconnect of a counted peer also recalculates the depth, wakes the manage loop and signals the puller, which recalculates its sync peers.

### Upstream (rule 11)

Checked, not assumed:
- `git show upstream/v2.8.2:pkg/topology/kademlia/kademlia.go` has the same reset at line 462, the same `SetTryAfter` in `Disconnected` (line 1270), the same `Remove` in `onConnected` (line 1255), and the same failure path including the breaker reset (lines 1035 to 1061).
- `upstream/master` (`7001a067`, 7 October 2026) has the same lines (463, 1308, 1293, and the failure path with the breaker reset at 1052 to 1078).
- `git diff upstream/v2.8.2 origin/main -- pkg/topology/kademlia/` touches none of these lines, and `pkg/topology/kademlia/internal/waitnext/` is identical in all three.

**What this covers, and what it does not.** "Unmodified upstream code" is verified for `pkg/topology/kademlia` only. Wasp changes other code on the observed path: `pkg/p2p/libp2p/libp2p.go`, `pkg/p2p/libp2p/peer.go`, and `pkg/pricing/announce.go`, which the failed dials in the logs mention. Those changes can affect **why** and **how often** the remote side drops us. They cannot affect the defect itself: the reset at line 467 follows any successful `p2p.Connect`, whatever happens after it, so any peer that accepts and then drops the connection repeats the loop. To show that on upstream code rather than argue it, the kademlia test "a peer that accepts every dial and is disconnected at once" (Design, section 6) is also run on an `upstream/v2.8.2` checkout with only that test added, and must show the loop there. The `affects-upstream` label rests on the code comparison above and on that test.

## Hypothesis

The failure count should only start again from zero after a connection that **lasted**. If a connection that ends within a minute does not reset the count, makes the next dial wait longer each time, and that wait survives pruning, then:
- a peer that keeps dropping us is dialled at most a few times an hour, instead of the measured 85 failed dials an hour;
- a peer whose dials also fail reaches the existing prune limit, as the limit intends;
- a healthy peer, whose connections last minutes to days, is treated as today.

If node 6's disconnects come mainly from this cycle, its kademlia disconnect count falls to the level of the other nodes, a few hundred a day.

## Design

### 1. The retry entry remembers when a connection started

`waitnext.next` gains three fields:
- `connectedAt time.Time`: when kademlia counted the current connection, zero when not connected;
- `shortLived int`: how many connections in a row ended within `stableConnection`;
- `expiresAt time.Time`: zero for a normal entry; set only on the reduced entry kept after pruning (section 3).

`waitnext.New(stableConnection, maxShortLivedBackoff, prunedEntryTTL time.Duration)` takes the three durations, so tests can set them (section 6).

`backoff(n, base)` is `base * 2^(n-1)`, capped at `maxShortLivedBackoff` (15 minutes), and `base` when `n` is 0. With the default `TimeToRetry` of 20 s the waits are 20 s, 40 s, 80 s, 160 s, 320 s, 640 s, then 15 minutes.

### 2. The methods, and what each does to `tryAfter`

All under the existing mutex. "Keep the later" means `tryAfter` becomes the later of its current value and the new one; "overwrite" means it becomes the new one.

| Method | Called from | `connectedAt` | `failedAttempts` | `shortLived` | `tryAfter` |
|---|---|---|---|---|---|
| `Connected(addr, now)` | outbound: `connect`, right after `p2p.Connect` succeeds; inbound and bootnode: `onConnected` | **overwrite** with `now` | kept | kept | **not changed**; a connected peer is never dialled |
| `Disconnected(addr, now, base)`, no entry or `connectedAt` zero | `Kad.Disconnected` | stays zero | kept | kept | keep the later of current and `now + base` |
| `Disconnected`, connection lasted at least `stableConnection` | `Kad.Disconnected` | cleared | **0** | **0** | **overwrite** with `now + base` |
| `Disconnected`, connection shorter than `stableConnection` | `Kad.Disconnected`; returns true | cleared | kept | **+1** | keep the later of current and `now + backoff(shortLived, base)` |
| `Failed(addr, now, retryAt, attempts)` | `connect`, failed dial and breaker refusal | not changed | set to `attempts` | kept | keep the later of current, `retryAt` and `now + backoff(shortLived, TimeToRetry)` |
| `Pruned(addr, now)` | prune at line 1149 | cleared | **0** | kept | kept; sets `expiresAt` (section 3) |

Only one method overwrites `tryAfter` with an earlier time: the stable branch of `Disconnected`. That is deliberate. A connection that lasted shows the peer is healthy again, so any backoff from earlier short connections ends there. Every other method keeps the later time, so a disconnect can no longer shorten a wait set by the connection breaker, which `SetTryAfter` can do today.

`Connected` always overwrites `connectedAt`. There is no "do not move" rule: a stale `connectedAt` left by the race in section 5 would otherwise make a later short connection look long and reset both counts, which is the defect itself.

### 3. Kademlia uses them

In `pkg/topology/kademlia/kademlia.go`:
- **Successful dial:** in `connect`, directly after `k.p2p.Connect` succeeds and before `Announce`, call `k.waitNext.Connected(peer, now)`. Line 467 no longer touches the retry entry, so a success no longer resets the failure count. The call sits before `Announce` because a remote that drops us within a second can close the connection while `Announce` is still running. The already-connected case (`p2p.ErrAlreadyConnected` with the same overlay) does not call `Connected`, so the start time of the existing connection is kept.
- **Failed announcement:** nothing extra. `Announce` disconnects the peer, libp2p calls `Kad.Disconnected` synchronously before `connect` returns (Problem, point 5), and `Disconnected` finds `connectedAt` set and counts one short-lived connection. A second call of `Kad.Disconnected` for the same connection, from libp2p's own handler, finds `connectedAt` cleared and only keeps the later `tryAfter`, so it counts nothing.
- **Failed dial (line 1157):** `Failed` instead of `Set`, with `attempts` increased by one as today. The prune decision at line 1149 still counts only failed dials, with 4 and 6.
- **Breaker refusal (lines 1132 to 1137):** `Failed` with `attempts` equal to the current `k.waitNext.Attempts(peer)`, not 0, and `retryAt` the time the breaker closes. A breaker refusal is not the peer's fault, so it neither increases nor resets the count, and `shortLived` is kept.
- **Prune (lines 1149 to 1155):** `Pruned` instead of `Remove` when the entry has `shortLived` above 0; `Remove` as today otherwise. The `remove` helper at line 437 (a dial that turned out to be a light node, an overlay mismatch) keeps `Remove`: those peers are not being backed off.
- **Inbound connection and bootnode connection (`onConnected`, line 1351):** `Connected` instead of `Remove`. `onConnected` runs **after** `Announce`, so a remote that drops an inbound connection within the announcement is not counted as short-lived. That is consistent with today, where inbound connections never touch the count, and kademlia does not dial such a peer while it holds no backoff for it.
- **Disconnect (line 1366):** `Disconnected` instead of `SetTryAfter`. When it returns true, increase a new counter `bee_kademlia_short_lived_connections` and log at debug level with the peer and the connection's duration.

Short-lived connections do not count towards pruning. The peer is reachable, and pruning on its own would only make the node forget it until the next announcement brings it back. The backoff in the reduced entry gives the bound instead.

### 4. The reduced entry after pruning

Neighbours re-announce each other every 15 minutes (the loop at lines 672 to 698) and on every new connection, so a pruned neighbour that keeps dropping us comes back quickly. If pruning deleted the entry, as it does today, `shortLived` would be lost and the 20-second cycle would start again, and the 15-minute bound would not hold.

So pruning a peer with `shortLived` above 0 keeps a reduced entry: `tryAfter` and `shortLived` as they are, `failedAttempts` and `connectedAt` cleared, and `expiresAt` set to `tryAfter + prunedEntryTTL` (1 hour). When the peer is announced again, its dial still waits for `tryAfter`, and the next short connection continues the doubling from where it was. Any later event on the entry clears `expiresAt` again.

Expired entries are deleted by a sweep in the existing loop that runs every `PruneWakeup` (5 minutes, line 634), and by `Waiting` and `Attempts` when they meet one.

**Memory bound:** one reduced entry per peer pruned with a short-lived history in the last `maxShortLivedBackoff + prunedEntryTTL` (75 minutes). Each entry is about a hundred bytes. On node 6 that is the 4 known peers; even every known peer, a few thousand, is well under a megabyte.

Retry entries are also no longer deleted when a peer connects inbound, so each connected peer keeps one entry. That is bounded by the connected peers. Nothing is written to disk; a restart clears all entries, as today.

### 5. What this does to each kind of peer

- **A healthy peer:** its connections last far longer than a minute, so every disconnect resets both counts, as today.
- **A peer that restarts:** the connection before the restart was long, so the counts reset; the dials during the restart fail and count as today; the next connection lasts and resets them again. Two differences:
  - the failed dials from the restart are cleared only when the next connection has lasted a minute, not the moment it is made;
  - a neighbour that collected 5 failed dials while restarting and then drops us once within its first minute back is pruned on its next failed dial, where today the success would have cleared the count. This is a small new risk of pruning a peer that is coming back. It recovers through the next announcement, and the reduced entry does not apply because its `shortLived` is 1 and its backoff is only 20 s.
- **The peer from #607:** failed dials accumulate across its short connections and reach the prune limit after 4 or 6 of them; each short connection doubles the wait, and the wait survives pruning. On the observed pattern (two failed dials, one short connection) a neighbour is pruned after about 6 failed dials, a few minutes, and is then dialled at most every 15 minutes while it keeps dropping us.
- **Our own disconnects:** a disconnect that this node causes within a minute of connecting also counts as short-lived: a blocklisting, an accounting disconnect, or `pruneOversaturatedBins`. The peer then waits longer before we dial it again. That is acceptable: in each case we chose to drop the peer, and dialling it again at once would repeat the cost.
- **Bootnodes:** a node in bootnode mode never dials (the manage loop skips dialling in bootnode mode), so nothing changes for it. `connectBootNodes` does not consult the retry entry, so a node with no peers still reaches its bootnodes at once. A bootnode that this node dials as an ordinary peer and that drops it quickly (bootnodes evict a random peer when a bin is full) is now dialled less often, which reduces the load on the bootnode.
- **Light peers:** kademlia never dials them and never calls `Connected` for them, so their entries never get `connectedAt` and their disconnects behave as today. If the light-peer churn change (#594, in PR #606) lands first, `Disconnected` returns before this code for peers kademlia never counted, which gives the same result.
- **Static peers (`static-nodes`):** they go through the same dial path, so a static peer that drops us within a minute also backs off, up to 15 minutes. This is listed as an open question below.

### 6. Tests

**Test hook.** The three durations are package-level variables in `pkg/topology/kademlia` (`stableConnection`, `maxShortLivedBackoff`, `prunedEntryTTL`), passed to `waitnext.New` in `kademlia.New`. `export_test.go` exposes a setter for each that returns a function restoring the old value. Tests that use a setter do not call `t.Parallel()`, because the variables are shared. In the kademlia tests, which run on real time, `stableConnection` is set to about 200 ms and `TimeToRetry` to 500 ms as the existing tests already do (`kademlia_test.go` around line 778).

In `internal/waitnext` (time is a parameter, so no sleeping):
- a connection shorter than `stableConnection` increases `shortLived` and sets the doubled wait; a longer one resets both counts;
- the wait doubles from `base` and stops at `maxShortLivedBackoff`;
- for each method in the table, whether it overwrites or keeps the later `tryAfter`, including that the stable branch of `Disconnected` overwrites a running 15-minute backoff and that the other methods never shorten it;
- `Connected` overwrites a stale `connectedAt`;
- a second `Disconnected` for the same connection counts nothing;
- `Pruned` keeps `shortLived` and `tryAfter`, clears `failedAttempts`, and the entry expires after `tryAfter + prunedEntryTTL`; the sweep deletes it.

In `pkg/topology/kademlia` (with the existing mock p2p service `pkg/p2p/mock`, whose `Disconnect` also notifies synchronously, and mock discovery):
- a peer that accepts every dial and is disconnected at once is dialled with growing gaps, and the short-lived counter increases by one per connection. This test is also run on an `upstream/v2.8.2` checkout, where it must show the loop (Problem, rule 11);
- a peer whose announcement fails counts **one** short-lived connection, and its next wait is 20 s, not 40 s;
- a peer that alternates two failed dials and one short connection is pruned from the address book, and when it is announced again it is not dialled before its `tryAfter`;
- a breaker refusal leaves `failedAttempts` and `shortLived` as they were;
- a peer that stays connected past `stableConnection` and then disconnects is retried after `TimeToRetry` with both counts at 0.

Mutation checks (lifecycle playbook, step 4): removing the duration check, removing the doubling, putting back the reset at line 467, deleting the entry on prune, resetting the count on a breaker refusal, and letting a disconnect shorten `tryAfter` must each fail at least one test.

### 7. A remaining race

A disconnect can still arrive between `k.p2p.Connect` returning and the `Connected` call that follows it. The disconnect then sees no `connectedAt`, and `Connected` then marks a connection that is already gone. The effect is one short connection that is not counted, and a `connectedAt` that stays set until the next event. Because `Connected` always overwrites `connectedAt`, the next successful dial replaces the stale value, so it cannot make a later short connection look long. The window is a few instructions long. Today the same race lets kademlia add a peer to its peer list after it has gone (line 469); that existing behaviour is not changed here.

## Protocol impact

None. This changes when the node dials a peer, which is local scheduling. No message, protocol identifier, stream, handshake field or constant on the frozen surface changes, and nothing under `pkg/p2p/`, `pkg/swarm/` or `pkg/config/` is touched, so `.github/protocol-freeze.lock` is unaffected. A stock peer sees fewer connection attempts from us and nothing else.

## Measurement

**Where:** sw-1 node 6 (the affected node) and one sw-1 node without the problem as a control, for example node 4 (99 kademlia disconnects in the baseline). Not before the radius-change scenarios on sw-1 have finished, because they restart every node.

**How:** the remote peers can change their behaviour at any time, so a single before and after is not enough. Node 6 alternates builds in **8-hour windows: old, new, old, new, old, new**. That gives three runs per build (rule 7), each next to a window of the other build. The control node stays on the old build throughout, to show whether the network changed during the run. **The first 30 minutes after each build switch are excluded** from every count, because a restart dials all known peers at once.

**What, per node, over each whole window:**
- **Failed dials per peer:** the Info-level `could not connect to peer` lines (`kademlia.go` line 1129), counted per peer address from the journal. They are always logged, so they need no extra logging and cover the whole window.
- **Successful dials and short connections per peer:** the kademlia logger only (not the whole node) is raised to debug through the `/loggers` API for the whole run, and the `connected to peer` and `disconnected peer` lines are counted per peer. On the new build, also `bee_kademlia_short_lived_connections` and its debug line, and the number of **distinct** peers it was counted for.
- **Counters every 5 minutes:** `bee_kademlia_total_inbound_disconnections`, `bee_kademlia_total_outbound_connection_attempts`, `bee_kademlia_total_outbound_connection_failed_attempts`, `bee_kademlia_total_outbound_connections`, and process CPU time.
- **Topology health every 5 minutes:** connected full peers, depth, and neighbourhood size (connected peers at or above the depth), from `/topology`.

**Accepted when**, on node 6, in each new-build window compared with the mean of its neighbouring old-build windows:
1. failed dials to each of the peers that keep dropping us fall from the measured level (about 85 an hour for the worst) to **at most 10 an hour**;
2. kademlia inbound disconnects fall by **at least 75 %**, reported with the spread of the three runs;
3. **healthy peers are not penalised:** connected full peers, depth and neighbourhood size stay within 10 % of the old-build windows and move with the control node; and the distinct peers counted as short-lived are the known peers that keep dropping us plus at most a few others, each of which is checked in the log to have really dropped us within a minute.

If disconnects fall to the level of the control node, the cycle was the main cause of node 6's count, and that is recorded as verified. If they fall but stay well above it, the cycle was one cause among others, and the rest is reported as unexplained.

**A negative result** looks like this: criteria 1 and 2 are not met while the short-lived counter is non-zero. That would mean the dials come from somewhere this change does not touch. A short-lived counter that stays at zero means the remote peers stopped the behaviour during the run; the run must then be repeated when it is seen again, and is counted as neither result. A failed criterion 3 is a regression and blocks the change, whatever 1 and 2 show.

## Rollout and rollback

There is no setting. The change ships in a wasp release and is on for every node running it. The implementation pull request adds the new counter `bee_kademlia_short_lived_connections` and the changed retry behaviour to `docs/DIFFERENCES.md` (rule 13).

Rollback is installing the previous release. The retry entries are in memory only, so going back needs no migration and leaves nothing behind.

## Upstream portability

The change is self-contained: `internal/waitnext` (three fields, a changed constructor, four methods and a sweep), the call sites in `kademlia.go` listed in section 3, one counter in `metrics.go`, and tests. It uses nothing wasp-specific. The lines it replaces are the same in `upstream/v2.8.2` and `upstream/master` (see Problem), so the patch applies to either with only line offsets.

The only interaction with wasp code is the light-peer churn change (#594), which makes `Disconnected` return early for peers kademlia never counted. Upstream has no such check; there the `connectedAt` test in `Disconnected` gives the same result for those peers.

## Configuration

No new settings. Three new compiled-in durations in `pkg/topology/kademlia`, package variables only so that tests can set them (section 6):

| Constant | Value | Raising it costs | Lowering it costs |
|---|---|---|---|
| `stableConnection` | 1 minute | A connection that ends after a normal short session, for example a remote restart soon after we connected, is treated as short-lived: we wait longer before dialling that peer again. | A peer that drops us after that time, for example when its periodic pruning (every 5 minutes by default) removes us, resets the count and is dialled at today's rate again. Below the one second seen on node 6 it does nothing. |
| `maxShortLivedBackoff` | 15 minutes | A peer that stopped dropping us, for example because its bin has room again, is reconnected later; for a neighbour that delays syncing with it. | More dials and handshakes per hour to a peer that keeps dropping us, on our side and on theirs. |
| `prunedEntryTTL` | 1 hour | Reduced entries stay in memory longer; a peer that changed its behaviour long ago still starts from its old backoff. | A pruned peer that is announced again after the entry expired starts again at 20 s, so the bound on dials no longer holds across long gaps. |

**Why compiled in (rule 8):** nothing shows that the right value depends on the machine, and no measurement shows any of them matters within a reasonable range. The existing `ShortRetry` and `TimeToRetry` stay as they are. If the measurement shows that a different value matters, a setting is added in a separate issue with that measurement, in the shape of `db-block-cache-capacity`.

**Cost to other nodes:** fewer dials from us to a peer that keeps dropping us, so less handshake work on its side as well. A peer that drops us briefly and then would have accepted us gets our next dial up to 15 minutes later.

## Open questions for the operator

1. Should static peers (`static-nodes`) be exempt from the backoff? An operator who configures one probably wants it dialled at today's rate even if it drops us. Exempting them is one check in `Disconnected`.
2. Is one minute the right threshold, or should it be above the default prune interval of 5 minutes, so that a remote that prunes us periodically also backs us off? The longer value treats more legitimate short connections as short-lived.
