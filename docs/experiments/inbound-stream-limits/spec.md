# Inbound stream limits per peer and per protocol group

Issue: [#638](https://github.com/crtahlin/wasp/issues/638). Split out: the per-address stream limit is [#681](https://github.com/crtahlin/wasp/issues/681). Related: [#636](https://github.com/crtahlin/wasp/issues/636) (refusals at the fixed cap), [#637](https://github.com/crtahlin/wasp/issues/637) and #639 (the stream metrics), [#640](https://github.com/crtahlin/wasp/issues/640), [#641](https://github.com/crtahlin/wasp/issues/641) and [#643](https://github.com/crtahlin/wasp/issues/643) (the pull-sync root-cause fixes), [#597](https://github.com/crtahlin/wasp/issues/597) and [#608](https://github.com/crtahlin/wasp/issues/608) (the per-IP connection limits and the resource-manager wrapper; the wrapper merged in PR #617, issue #608 stays open for its measurement).
Type: optimization.
Depends on: #640, #641 and #643, all merged. Without them, a per-peer limit refuses a neighbour while its dead requests stay open, which frees nothing.

## Decided (operator, 2026-10-09)

1. **Off by default now.** The limits are switched on by default only after the sw-1 comparison in this spec shows no legitimate peer is refused.
2. **Values:** 256 inbound streams per peer, 2,000 reserved for pull-sync, 1,000 reserved for the other protocols.
3. **The per-address limit is split out** into #681 and is not part of this spec.

## Terms

- **Resource manager (`rcmgr`):** libp2p's component that admits or refuses connections and streams against limits. Limits are set per **scope**: `System` (the whole node), `Transient` (streams whose protocol is not yet negotiated), `Peer` (one peer ID, all protocols), `Protocol` (one protocol ID, all peers), and others not used here. A stream counts against every scope it belongs to; a transient stream counts against `System` too (`rcmgr.go:587-590`, `scope.go:168-178`).
- **Inbound stream:** a stream another peer opens to this node.
- **Stream cap:** the node-wide inbound limit, `System.StreamsInbound = IncomingStreamCountLimit = 5,000` (`pkg/p2p/libp2p/libp2p.go:87, 276-282`). Transient streams are charged to the system scope, so 5,000 is a hard limit. (Earlier notes gave an "effective" 5,014; that figure is unexplained and is not used here.)
- **4098:** `network.StreamResourceLimitExceeded` (0x1002), the code a peer sees when this node refuses its stream. Any error from the resource manager or a wrapper around it leads to this reset (`swarm_conn.go:139-142`, `basic_host.go:350-352`).
- **Pull-sync group:** the inbound protocol IDs `/swarm/pullsync/1.4.0/pullsync` and `/swarm/pullsync/1.4.0/cursors`, read from the registered protocols, not hard-coded. **Other group:** every other negotiated protocol ID, including retrieval, push-sync, hive, pricing, status, pseudosettle, swap, and libp2p's identify, ping and autonat.
- Rules 6, 7, 8 and 10 are those of `AGENTS.md`: the frozen wire surface, three runs per condition with the spread, measure before setting a default (and state what a setting costs), and no hosts or addresses in public text.

## Problem

Checked on `main` (`pkg/p2p/libp2p/libp2p.go:276-292`): the limits are built from `rcmgr.InfiniteLimits`, and only the `System` stream counts are set. `Peer`, `Protocol` and `Transient` keep infinite limits. So:

1. **One peer can take the whole cap.** Before #640 this happened in production: one neighbour held about 4,500 of the 5,000 inbound streams on stake-1, and one peer each held 700 to 1,100 on seven of ten sw-1 nodes (#636). At the cap, the node refuses every new inbound stream from everyone, retrieval and push-sync included.
2. **One protocol can starve another.** The cap is shared, so a surge of retrieval or push-sync streams can leave no room for pull-sync, which the reserve and the storage incentives depend on, and the other way round (the #636 pile-up was all pull-sync and starved everything else).

Point 1 has a root-cause fix for pull-sync (#640, #641, #643) and remains for other protocols; point 2 remains in full. Many identities behind one address are #681.

### Corrections to the issue text

- The issue and its later comment estimated "about 30" and "about 32" pull-sync streams per neighbour. After #640 and #643 the bound is **at most 64 waiting pull-sync requests per peer** (two per bin, 32 bins). At radius 6 a neighbour syncs 26 bins (6 to 31), so in practice at most 52 plus the cursors stream; peers at one or two proximity orders below the radius sync only their own bin and hold at most 2.
- The comment's arithmetic (40 to 57 neighbours times a per-peer cap of 256 is more than the cap) stands: a per-peer limit alone cannot bound the node total. That is what the group limits are for.

## What was measured before this spec (sw-1 and stake-1, 2026-10-09)

### The sampled per-peer maximum

From `bee_libp2p_inbound_streams_per_peer_max` (#639), the largest inbound stream count held by any single peer, **sampled every 30 s**:

| Condition | Largest single peer | Total inbound per node |
|---|---|---|
| Before #640, 2.5 h after a restart | 210 to 354 (seven of ten nodes) | up to 530 |
| Before #640, days after a restart | 700 to 1,100 on sw-1; about 4,500 on stake-1 | up to 4,830 |
| With #640, during a refill (all ten nodes pulling 430 to 770 chunks/s), 3.5 h | 22 to 24 on every node | 54 to 278 |

A 30-second sample misses bursts, so this table is not enough to size the per-peer limit.

### The exact count of times a peer passed 256 (pre-measurement)

`libp2p_rcmgr_peer_streams{dir="inbound"}` is the resource manager's own histogram: one observation each time a peer's inbound stream count changes (`stats.go:27-29, 58-63, 257-260`). The count of its `+Inf` bucket minus its `le="256"` bucket is the exact number of observations above 256, recorded whether or not any limit is set. Read on 2026-10-09 at 22:16 CEST:

| Node | Build | Observations since start | Above 64 | **Above 256** |
|---|---|---|---|---|
| sw-1 nodes 1, 2, 5 to 9 | with #640, started 17:43 | 261,000 to 375,000 each | 0 | **0** |
| sw-1 node 3 | with #640 | 367,486 | 1 | 0 |
| sw-1 node 4 | with #640 | 282,000 | 9 | 0 |
| sw-1 node 10 | with #640 | 556,911 | 2,825 | **91** |
| stake-1 | without #640, started 08:23 | 320,674 | 9,587 | 5,791 |

So, with #640 in place, one peer on node 10 briefly held more than 256 inbound streams during the refill: too briefly for the 30-second sample (which read 24) and too briefly for the hourly log line of #639. Which protocol it used is not known. This is the hypothesis the review raised (push-sync forwarding has no per-downstream bound, `pusher.go:67` limits only the origin), and it is now partly confirmed: a legitimate-looking burst above 256 exists.

**What this means for 256.** The operator chose 256 and the limits stay off by default, so nothing is refused today. The value changes if the measurement below shows:
- any refusal at 256 of a peer that is not misbehaving (a peer whose streams are spread over protocols in normal use, not a pile-up of one protocol), or
- bursts above 256 that recur on more than one node.

In that case the value is raised to the smallest power of two above the observed maximum (the histogram buckets are powers of two) before the default is switched on.

### The other group

Never measured on its own before. `libp2p_rcmgr_streams{dir="inbound",scope="protocol"}` gives it per protocol. On sw-1 node 10 at 22:16, during the refill: pull-sync 277, autonat 2, every other protocol 0 at that instant. The measurement below records its maximum over time.

## Hypothesis

With per-peer and per-group inbound limits set above the measured legitimate need, no single peer or protocol group can take the node's stream cap, and no legitimate peer is refused.

## Design

All limits are inbound only. Outbound streams are this node's own and are not counted.

### Where refusals happen (verified in go-libp2p v0.48.0)

- **At stream accept:** `swarm_conn.go:139` calls `ResourceManager().OpenStream(peer, DirInbound)` before any protocol negotiation. This checks `System`, `Transient` and `Peer`. On failure the stream is reset with 4098 (`:141`).
- **At protocol negotiation:** `basic_host.go:350` calls `SetProtocol(protoID)` on the stream's scope, which moves it from the transient scope into the `Protocol` scope (`rcmgr.go:856-883`). On failure the stream is reset with 4098 (`:352`). `SetProtocol` also runs for **outbound** streams (`basic_host.go:481, 510`), so the group check must test the direction.
- A wrapper can intercept `SetProtocol`: the swarm calls it on the scope returned by `OpenStream` (`swarm_stream.go:154-155`), with no type assertion on the concrete type.

### 1. Per-peer inbound limit (resource manager, `PeerDefault`)

`PeerDefault.StreamsInbound = N_peer` (256 when on). Counted across all protocols of one peer ID, checked at accept. `Build` fills every zero field from `InfiniteLimits`, so the other peer limits stay infinite.

### 2. Protocol groups with guaranteed room (wrapper)

The resource manager limits each protocol separately but cannot reserve room for one protocol against the sum of the others. The existing wrapper (`inboundLimiter`, `pkg/p2p/libp2p/inboundlimit.go:186`, which already wraps `OpenConnection` for #608) gains an `OpenStream` override that returns a wrapped scope, and the wrapped scope overrides `SetProtocol` and `Done`.

**Transient streams.** A stream counts against the system cap from accept, before it has a protocol and so before it belongs to a group. Without a bound, un-negotiated streams could fill the room reserved for pull-sync and pull-sync would be refused at accept, before any group check runs. So the design also sets **`Transient.StreamsInbound = T`**, with **T = 256**. Negotiation takes milliseconds, so transient streams are few: 0 on every sw-1 node at each reading; the measurement records the maximum.

The group limits are then:
- the other group may hold at most `cap - R_pull - T` = 5,000 - 2,000 - 256 = **2,744** negotiated inbound streams;
- the pull-sync group may hold at most `cap - R_other - T` = 5,000 - 1,000 - 256 = **3,744**.

With these, pull-sync always has at least `R_pull` streams of room (the others plus transient can never exceed `cap - R_pull`), and the other group at least `R_other`. The guarantee covers negotiated streams; transient streams are bounded separately by T.

Startup refuses a configuration where `R_pull + R_other + T >= cap`.

**Order of check, count and release** (one mutex, or an atomic add with rollback):
1. `OpenStream(peer, dir)`: call the inner `OpenStream`; on error return it. Wrap the returned scope, recording the direction.
2. Wrapped `SetProtocol(proto)`, only for an inbound stream:
   - find the stream's group from the protocol ID;
   - **reserve first:** increment the group count; if the result is above the group limit, decrement again and return the limit error, without calling the inner `SetProtocol`;
   - otherwise call the inner `SetProtocol`; if it fails (`rcmgr.go:868-883`), decrement the group count and return its error;
   - on success, store the group in the wrapped scope.
   - For an outbound stream, pass straight through to the inner `SetProtocol`.
3. Wrapped `Done()`: call the inner `Done()`; if a group is stored and the stream has not been released yet, decrement that group's count once (a `sync.Once` or a released flag).

A refusal in step 2 returns before the inner scope moves to a protocol. The swarm then resets the stream and calls `Done()` on the returned scope, which releases the inner scope's transient reservation; the wrapped `Done()` does not touch the group count, because no group was stored.

### 3. Interactions

- **#640, #641 and #643:** they keep each peer's pull-sync use at or below 64 waiting requests and free abandoned ones, so these limits are a backstop.
- **Light and ultra-light peers:** they open retrieval and push-sync streams, not pull-sync. The per-peer limit applies to them as to anyone; 256 is far above their need.
- **The other group includes identify, ping, autonat and the handshake follow-ups.** When it is full, new connections cannot complete identify, so new peers fail to connect. That is the intended effect under a flood of other protocols, and the reason the other group's limit (2,744) is far above anything measured.
- **Bootnodes:** the sibling connection limit of #608 is off on bootnodes unless set. Here the same applies: the switch below is off by default everywhere, and on a bootnode the limits apply only when the operator sets the switch.
- **The connection rate limit of #608:** unchanged. It acts on connections; these limits act on streams.

### 4. What a refused peer experiences

The peer's stream fails with 4098. A wasp requester counts it in `bee_libp2p_streams_refused_by_peer_total` and its puller backs off (#573). **An upstream puller retries a failed sync at once, with no backoff** (upstream `pkg/puller/puller.go`), so a refused upstream neighbour can open and lose streams in a tight loop; #573 measured such a loop at about 2,000 attempts per second and 1.4 cores on the requester side. A refusal at accept costs this node little (no negotiation, no handler); the measurement below triggers the loop on purpose to put numbers on both sides.

### 5. Metrics

- Per-peer and transient refusals: already exported by the resource manager, `libp2p_rcmgr_blocked_resources{dir="inbound",scope="peer"|"transient",resource="stream"}`.
- New: `bee_libp2p_inbound_streams_refused_total{limit="group_pullsync"|"group_other"}`, each label created at start so the series exports 0.
- New gauge: `bee_libp2p_inbound_streams_group{group="pullsync"|"other"}`.
- The per-peer histogram `libp2p_rcmgr_peer_streams` stays the evidence for the per-peer value (above).

## Protocol impact

None. No message changes (rule 6). A refusal is the existing 4098 reset, which every libp2p peer already handles.

## Configuration

Follows the convention of the sibling settings (`p2p-inbound-connection-rate` and `-burst`, `inboundlimit.go:31-33, 63-87`): for each value, **0 means the built-in default and -1 means that limit is off**; other negative values are refused at start.

| Setting | Built-in default (0) | Meaning of -1 |
|---|---|---|
| `p2p-inbound-stream-limits` | **false** | (boolean switch) |
| `p2p-inbound-streams-per-peer` | 256 | no per-peer limit |
| `p2p-inbound-stream-reserve-pullsync` | 2,000 | no reserve for pull-sync |
| `p2p-inbound-stream-reserve-other` | 1,000 | no reserve for the others |
| `p2p-inbound-streams-transient` | 256 | no transient limit |

The switch is the operator's decision 1: while it is false, none of the limits apply and the built limit configuration equals today's. Switching the default to true, after the measurement, changes nothing else. With the switch on, any value set to -1 turns only that limit off; turning off a reserve while the other stays on is allowed.

Help text in the rule 8 form, for example for the per-peer value: "inbound streams one peer may hold at once across all protocols, when p2p-inbound-stream-limits is on; 0 uses the default of 256, -1 turns this limit off. Raising it lets one peer take more of this node's stream cap; lowering it can refuse a legitimate neighbour, which then cannot sync from or retrieve through this node, and an upstream neighbour may retry in a tight loop that costs both nodes CPU." The others follow the same form. All are documented in `docs/config-reference.yaml` and `docs/DIFFERENCES.md`.

## Measurement

### Unit and integration tests

1. **Per-peer limit:** two libp2p test hosts; the client opens `N_peer + 1` inbound streams on a test protocol; the last is refused with 4098 and counted in `blocked_resources{scope="peer"}`; after one stream closes, a new one is admitted.
2. **Groups:** with a small cap, fill the other group to its limit; a pull-sync stream is still admitted until its reserve is used; the reverse for the other group; refusals counted per group.
3. **Transient bound:** with streams held un-negotiated up to T, a further stream is refused at accept, and a pull-sync stream that negotiates within the reserve is still admitted.
4. **Release:** after every stream of tests 1 to 3 closes, the system, transient, protocol and group counts are all back to 0 (a leak test on `ResourceManagerState.Stat()`).
5. **Inner `SetProtocol` failure:** with an inner protocol limit that refuses, the group count is unchanged afterwards.
6. **Outbound:** outbound streams on group protocols are never counted or refused by the group check.
7. **Parallel opens under `-race`:** many goroutines open and close streams on both groups at once; counts end at 0 and never exceed the limits.
8. **Off:** with the switch false, and with every value -1, the built limit configuration equals `main`'s.
9. **Settings:** through the real command flags (`setAllFlags`): defaults, 0, -1 and explicit values pass through; other negative values and `R_pull + R_other + T >= cap` are refused at start.

Mutation checks: per-peer limit not applied; group count not released on `Done()`; group count released twice; group count not released when the inner `SetProtocol` fails; inner scope not released when the wrapper refuses; group check applied to outbound streams; groups swapped; transient limit not set.

### On the bench: does it protect

On bench-1 with throwaway nodes (no stake), three runs each, limits off and on:
1. **One peer flooding:** a test client opens inbound streams on one protocol as fast as it can. Off: the node's inbound count reaches the cap and a second, normal peer is refused. On: the flooding peer is refused at 256 and the normal peer is not.
2. **One protocol flooding:** several test clients flood the other group. On: pull-sync from a normal neighbour still gets streams up to its reserve.
3. **The upstream retry loop:** an unmodified upstream node syncs from a wasp node whose per-peer limit is set low (for example 16), so the upstream puller is refused. Record CPU on both nodes, attempts per second and refusals per second, for 10 minutes per run.
4. **A burst probe:** a test client that uploads at a high rate through a forwarding node, to show how many push-sync streams one forwarding peer can hold, against the 256 limit.

Pass for 1 and 2: the protected peer or protocol is never refused in any run. For 3: the numbers are recorded; if the loop costs either node more than 0.5 core, the refusal path for pull-sync gets its own issue (for example a short refusal backoff on this side) before the default is switched on.

### On sw-1: is any legitimate peer refused

After the next build with #640, #641 and #643 is on all ten nodes, three windows of 24 hours, each including normal load and at least one refill:
- in each window, five nodes with the limits on and five off, **alternating which half is on** between windows, so every node and neighbourhood is measured both ways;
- per node and window: the per-peer histogram (observations above 64, 128 and 256), `blocked_resources{scope="peer"|"transient"}`, the new group refusals, the group gauges' maxima, `bee_libp2p_streams_refused_by_peer_total` (refusals we receive), refill rate and CPU;
- reported with the spread across nodes and windows.

Pass, the condition for the operator's decision 1:
- **zero refusals** of any peer at the per-peer, group or transient limits on the "on" nodes, in all three windows, except peers shown to be misbehaving (a pile-up of one protocol, checked in a goroutine dump as for #640);
- refill rate and CPU of the "on" half within the spread of the "off" half;
- if any refusal of a peer in normal use occurs, the value is raised as stated above and the windows are repeated.

Refusals this node causes to others are exactly its own `blocked_resources` and the new group counter; there is no other way to see them from here.

## Rollout and rollback

The switch is off by default. An operator can turn it on, and any single limit off with -1, without a rebuild. Switching the default on is a separate change after the sw-1 measurement passes.

## Upstream portability

Upstream has the same infinite peer, protocol and transient limits (`upstream/master` `pkg/p2p/libp2p/libp2p.go`, same construction). The per-peer and transient limits are resource-manager settings upstream could take directly. The group reserve depends on wasp's resource-manager wrapper (#608). No `affects-upstream` label: the missing limits are a hardening gap, not a defect shown by a reproduction.

## To check during implementation

- How the pull-sync protocol IDs are best passed to the wrapper from the registered protocols.
- Whether the wrapped stream scope must also forward `PeerScope()`, `ProtocolScope()` and `ServiceScope()` unchanged (it must; the swarm and identify use them).
