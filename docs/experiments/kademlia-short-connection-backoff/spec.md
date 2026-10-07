# Kademlia: back off from a peer that drops us right after connecting

Issue: [#607](https://github.com/crtahlin/wasp/issues/607). Type: fix. Affects upstream.

## Terms

- **Dial:** an outbound connection attempt that kademlia's manage loop starts to a known peer.
- **Retry entry (`waitNext`):** kademlia's in-memory record per peer address of when it may be dialled again (`tryAfter`) and how many dials to it have failed (`failedAttempts`). It lives in `pkg/topology/kademlia/internal/waitnext` and is not stored on disk.
- **`ShortRetry` and `TimeToRetry`:** kademlia's retry intervals, 10 s and 20 s by default (`defaultShortRetry`, `defaultTimeToRetry` in `pkg/topology/kademlia/kademlia.go`).
- **Pruning a peer from the address book:** after `maxConnAttempts` (4) failed dials, or `maxNeighborAttempts` (6) for a peer at or above the node's depth, kademlia removes the peer from its address book and its known-peer list. The peer comes back only when another peer announces it again through the hive protocol.
- **Short-lived connection:** in this spec, a connection kademlia counted that ends less than one minute after it was established.
- Rules 6, 7, 8 and 11 are those of `AGENTS.md`: the frozen wire surface, three runs per condition, measuring before adding a setting, and tagging defects that are also upstream.

## Problem

### What was seen

In the 24-hour baseline on [#599](https://github.com/crtahlin/wasp/issues/599) (10 nodes on sw-1 plus stake-1, `main` builds, 6 to 7 October 2026), one sw-1 node (node 6) counted **17,673 kademlia inbound disconnects and 6,464 libp2p disconnects in a day**. Most nodes on the same machine counted a few hundred; the lowest was 99. These are counter readings, so they are measured.

The issue records two further observations on that node, both from its logs (measured):
- Over 6 hours, 2,753 `could not connect to peer` lines, mostly for 4 peers. The worst peer had 508 dials in 6 hours, one every 25 to 70 seconds, with no longer gap. Two of the 4 are in its neighbourhood.
- With debug logging for 9 minutes, one cycle per neighbour about every 25 seconds: most dials fail during setup (the remote side drops the connection and the pricing announcement or the handshake close fails), and about one in three succeeds and **is closed by the remote side within one second**.

**Not verified:** that this cycle accounts for all, or most, of node 6's 17,673 disconnects. The logs show the cycle for 4 peers; nobody has counted the disconnects per peer. The measurement below settles it.

**Not known:** why the remote side drops the connection. A remote bin that is full would fit. It is not visible from our side and this spec does not depend on it.

### Why the loop never stops (verified in code)

All references are to `pkg/topology/kademlia/kademlia.go` on `origin/main` (`a6d045a0`):

1. **A failed dial** (`connect`, line 1128 onward) increases `failedAttempts` in the retry entry and sets `tryAfter` to now plus `TimeToRetry` (line 1157). At `maxConnAttempts`, or `maxNeighborAttempts` for a neighbour, the peer is pruned from the address book (line 1149).
2. **A successful dial** sets the retry entry to `tryAfter` now plus `ShortRetry` **with `failedAttempts` 0** (line 467: `k.waitNext.Set(peer.addr, time.Now().Add(k.opt.ShortRetry), 0)`).
3. **A disconnect** (`Disconnected`, line 1366) sets only `tryAfter`, to now plus `TimeToRetry`, and keeps `failedAttempts` as it is.
4. **An inbound connection** (`onConnected`, line 1351) removes the retry entry altogether.

So a peer that accepts the connection and drops it at once resets the failure count every time it accepts. With roughly one success in three dials, the count never reaches 4 or 6, the peer is never pruned, and it is dialled again about 20 s after each event, plus up to 15 s until the manage loop's next pass. That is the 25-second cycle in the logs.

Each cycle costs a handshake on both sides and a disconnect. On our side a disconnect of a counted peer also recalculates the depth, wakes the manage loop and signals the puller, which recalculates its sync peers.

### Upstream (rule 11)

Checked, not assumed:
- `git show upstream/v2.8.2:pkg/topology/kademlia/kademlia.go` has the same reset at line 462, the same `SetTryAfter` in `Disconnected` (line 1270), the same `Remove` in `onConnected` (line 1255) and the same failure path (lines 1054 to 1061).
- `upstream/master` (`7001a067`, 7 October 2026) has the same four lines (463, 1308, 1293, 1071 to 1078).
- `git diff upstream/v2.8.2 origin/main -- pkg/topology/kademlia/` touches none of these lines, and `pkg/topology/kademlia/internal/waitnext/` is identical in all three.

The behaviour was measured on a wasp node; the code path that produces it is unmodified upstream code. The `affects-upstream` label is therefore justified.

## Hypothesis

The failure count should only start again from zero after a connection that **lasted**. If a connection that ends within a minute does not reset the count, and also makes the next dial wait longer each time, then:
- a peer that keeps dropping us is dialled a few times an hour instead of about 140 times;
- a peer whose dials also fail reaches the existing prune limit, as the limit intends;
- a healthy peer, whose connections last minutes to days, is treated exactly as today.

If node 6's disconnects come mainly from this cycle, its kademlia disconnect count falls to the level of the other nodes, a few hundred a day.

## Design

### 1. The retry entry remembers when a connection started

`waitnext.next` gains two fields:
- `connectedAt time.Time`: when kademlia counted the current connection, zero when not connected;
- `shortLived int`: how many connections in a row ended within a minute.

New methods in `internal/waitnext`, under the existing mutex:

- **`Connected(addr, now, tryAfter)`**: create the entry if missing; keep `failedAttempts` and `shortLived`; set `connectedAt` to `now` unless it is already set (a second success while connected must not move it); set `tryAfter`.
- **`Disconnected(addr, now, base) (shortLived bool)`**:
  - no entry, or `connectedAt` zero: set `tryAfter` to `now + base`, as `SetTryAfter` does today, and return false;
  - otherwise clear `connectedAt`, then
    - if the connection lasted at least `stableConnection` (one minute): set `failedAttempts` and `shortLived` to 0 and `tryAfter` to `now + base`, and return false. This is today's behaviour after a disconnect;
    - if it was shorter: increase `shortLived`, set `tryAfter` to `now + backoff(shortLived, base)`, keep `failedAttempts`, and return true.
- **`Failed(addr, now, tryAfter, attempts)`** replaces `Set` on the failure path: it updates `failedAttempts` and sets `tryAfter` to the **later** of the given time and `now + backoff(shortLived, base)`, and keeps `shortLived`. Without this, a failed dial between two short connections would bring the next dial back to 20 s and undo the backoff.

`backoff(n, base)` is `base * 2^(n-1)`, capped at `maxShortLivedBackoff` (15 minutes), and `base` when `n` is 0. With the default `TimeToRetry` of 20 s the waits are 20 s, 40 s, 80 s, 160 s, 320 s, 640 s, then 15 minutes.

### 2. Kademlia uses them

In `pkg/topology/kademlia/kademlia.go`:
- **Successful dial (line 467):** `k.waitNext.Connected(peer.addr, now, now+ShortRetry)` instead of `Set(..., 0)`. The failure count is no longer reset here.
- **Inbound connection and bootnode connection (`onConnected`, line 1351):** `k.waitNext.Connected(addr, now, now)` instead of `Remove`. A connected peer is never dialled, so a `tryAfter` of now changes nothing while it is connected; what changes is that the counts survive until the connection has shown it lasts.
- **Disconnect (line 1366):** `k.waitNext.Disconnected(peer.Address, now, TimeToRetry)` instead of `SetTryAfter`. When it returns true, increase a new counter `bee_kademlia_short_lived_connections` and log at debug level with the peer and the connection's duration.
- **Failed dial (line 1157):** `Failed` instead of `Set`. The prune decision at line 1149 is unchanged: it still counts only failed dials, and still uses 4 and 6.

Short-lived connections do not count towards pruning. The peer is reachable, and pruning would only make the node forget it until the next announcement brings it back, with a fresh count. The backoff gives a bound that does not depend on how often that happens.

### 3. What this does to each kind of peer

- **A healthy peer:** its connections last far longer than a minute, so every disconnect resets both counts, as today.
- **A peer that restarts:** the connection before the restart was long, so the counts reset; the dials during the restart fail and count as today; the next connection lasts and resets them again. One difference: the failed dials from the restart are now cleared only when that next connection has lasted a minute, not the moment it is made.
- **The peer from #607:** failed dials accumulate across its short connections and reach the prune limit after 4 or 6 of them; each short connection doubles the wait. On the observed pattern (two failed dials, one short connection) a neighbour is pruned after about 6 failed dials, a few minutes, instead of never.
- **Bootnodes:** a node in bootnode mode never dials (the manage loop skips dialling in bootnode mode), so nothing changes for it. `connectBootNodes` does not consult the retry entry, so a node with no peers still reaches its bootnodes at once. A bootnode that this node dials as an ordinary peer and that drops it quickly (bootnodes evict a random peer when a bin is full) is now dialled less often, which also takes load off the bootnode.
- **Light peers:** kademlia never dials them and never calls `Connected` for them, so their entries never get `connectedAt` and their disconnects behave as today. If the light-peer churn change (#594, in PR #606) lands first, `Disconnected` returns before this code for peers kademlia never counted, which gives the same result.
- **Static peers (`static-nodes`):** they go through the same dial path, so a static peer that drops us within a minute also backs off, up to 15 minutes. This is listed as an open question below.

### 4. Memory

Retry entries are no longer removed when a peer connects inbound, so a connected peer keeps one entry. Entries are still removed when a peer is pruned. The map is bounded by the known peers, a few thousand entries of a few dozen bytes each. Nothing is written to disk; a restart clears all entries, as today.

### 5. Tests

In `internal/waitnext`:
- a connection of 59 s increases `shortLived` and sets the doubled wait; one of 61 s resets both counts;
- the wait doubles from 20 s and stops at 15 minutes;
- `Failed` never shortens a backoff that is already longer;
- `Connected` twice does not move `connectedAt`.

The time comes from a parameter, so the tests need no sleeping.

In `pkg/topology/kademlia` (with the existing mock p2p service): a peer that accepts every dial and is disconnected at once is dialled with growing gaps, and the short-lived counter increases; a peer that alternates two failed dials and one short connection is pruned from the address book; a peer that stays connected past the threshold and then disconnects is retried after `TimeToRetry` with counts at 0.

Mutation checks (lifecycle playbook, step 4): removing the duration check, removing the doubling, and putting back the reset at line 467 must each fail at least one test.

## Protocol impact

None. This changes when the node dials a peer, which is local scheduling. No message, protocol identifier, stream, handshake field or constant on the frozen surface changes, and nothing under `pkg/p2p/`, `pkg/swarm/` or `pkg/config/` is touched, so `.github/protocol-freeze.lock` is unaffected. A stock peer sees fewer connection attempts from us and nothing else.

## Measurement

**Where:** sw-1 node 6 (the affected node) and one sw-1 node without the problem as a control, for example node 4 (99 kademlia disconnects in the baseline). Not before the radius-change scenarios on sw-1 have finished, because they restart every node.

**What, per node, every 5 minutes:**
- `bee_kademlia_total_inbound_disconnections`, `bee_kademlia_total_outbound_connection_attempts`, `bee_kademlia_total_outbound_connection_failed_attempts`, `bee_kademlia_total_outbound_connections`;
- `bee_kademlia_short_lived_connections` (new build only);
- process CPU time;
- from the debug log, sampled for 10 minutes per window: dials per peer, to see the cycle per peer.

**How:** the remote peers can change their behaviour at any time, so a single before and after is not enough. Node 6 alternates builds in **8-hour windows: old, new, old, new, old, new**. That gives three runs per build (rule 7), each next to a window of the other build. The control node stays on the old build throughout, to show whether the network changed during the run.

**Accepted when**, on node 6, in each new-build window against its neighbouring old-build windows:
- dials to any single peer that keeps dropping us fall from about 140 an hour to at most about 10 an hour;
- kademlia inbound disconnects fall clearly, with the spread of the three runs reported.

If they fall to the level of the control node, the cycle was the main cause of node 6's count, and that is recorded as verified. If they fall only partly, the cycle was one cause among others, and the rest is reported as unexplained.

**A negative result** looks like this: no fall in dials or disconnects on node 6 while the short-lived counter is non-zero. That would mean the dials come from somewhere this change does not touch. A short-lived counter that stays at zero means the remote peers stopped the behaviour during the run, and the measurement must be repeated when it is seen again, rather than counted as either result.

## Rollout and rollback

There is no setting. The change ships in a wasp release and is on for every node running it.

Rollback is the previous build. The retry entries are in memory only, so going back needs no migration and leaves nothing behind.

## Upstream portability

The change is self-contained: `internal/waitnext` (two fields, three methods), four call sites in `kademlia.go`, one counter in `metrics.go`, and tests. It uses nothing wasp-specific. The lines it replaces are the same in `upstream/v2.8.2` and `upstream/master` (see Problem), so the patch applies to either with only line offsets.

The only interaction with wasp code is the light-peer churn change (#594), which makes `Disconnected` return early for peers kademlia never counted. Upstream has no such check; there the `connectedAt` test in `Disconnected` gives the same result for those peers.

## Configuration

No new settings. Two new compiled-in constants in `pkg/topology/kademlia`:

| Constant | Value | Raising it costs | Lowering it costs |
|---|---|---|---|
| `stableConnection` | 1 minute | A connection that ends after a normal short session, for example a remote restart soon after we connected, is treated as short-lived: we wait longer before dialling that peer again. | A peer that drops us after that time, for example when its periodic pruning (every 5 minutes by default) removes us, resets the count and is dialled at today's rate again. Below the one second seen on node 6 it does nothing. |
| `maxShortLivedBackoff` | 15 minutes | A peer that stopped dropping us, for example because its bin has room again, is reconnected later; for a neighbour that delays syncing with it. | More dials and handshakes per hour to a peer that keeps dropping us, on our side and on theirs. |

**Why compiled in (rule 8):** nothing shows that the right value depends on the machine, and no measurement shows either value matters within a reasonable range. The existing `ShortRetry` and `TimeToRetry` stay as they are. If the measurement shows that a different value matters, a setting is added in a separate issue with that measurement, in the shape of `db-block-cache-capacity`.

**Cost to other nodes:** fewer dials from us to a peer that keeps dropping us, so less handshake work on its side as well. A peer that drops us briefly and then would have accepted us gets our next dial up to 15 minutes later.

## Open questions for the operator

1. Should static peers (`static-nodes`) be exempt from the backoff? An operator who configures one probably wants it dialled at today's rate even if it drops us. Exempting them is one check in `Disconnected`.
2. Is one minute the right threshold, or should it be above the default prune interval of 5 minutes, so that a remote that prunes us periodically also backs us off? The longer value treats more legitimate short connections as short-lived.
