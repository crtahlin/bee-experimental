# Inbound stream limits per peer, per address and per protocol group

Issue: [#638](https://github.com/crtahlin/wasp/issues/638). Related: [#636](https://github.com/crtahlin/wasp/issues/636) (refusals at the fixed cap), [#637](https://github.com/crtahlin/wasp/issues/637) and #639 (the stream metrics this spec sizes from), [#640](https://github.com/crtahlin/wasp/issues/640), [#641](https://github.com/crtahlin/wasp/issues/641) and [#643](https://github.com/crtahlin/wasp/issues/643) (the pull-sync root-cause fixes), [#597](https://github.com/crtahlin/wasp/issues/597) and [#608](https://github.com/crtahlin/wasp/issues/608) (the per-IP connection limits and the resource-manager wrapper this builds on).
Type: optimization.
Depends on: #640, #641 and #643, all merged. Without them, a per-peer limit refuses a neighbour while its dead requests stay open, which frees nothing.

## Terms

- **Resource manager (`rcmgr`):** libp2p's component that admits or refuses connections and streams against limits. Limits are set per **scope**: `System` (the whole node), `Transient` (streams whose protocol is not yet negotiated), `Peer` (one peer ID, all protocols), `Protocol` (one protocol ID, all peers), `ProtocolPeer` (one protocol for one peer), and others not used here. A stream counts against every scope it belongs to.
- **Inbound stream:** a stream another peer opens to this node.
- **Stream cap:** the node-wide inbound limit, `System.StreamsInbound = IncomingStreamCountLimit = 5,000` (`pkg/p2p/libp2p/libp2p.go:87, 276-282`). The effective cap measured on nodes is 5,014: libp2p adds a small allowance for the transient scope.
- **4098:** `network.StreamResourceLimitExceeded` (0x1002), the error code a peer sees when this node's resource manager refuses its stream.
- **Address key:** the remote IPv4 address (/32) or IPv6 /56 prefix, computed by `addressKey` (`pkg/p2p/libp2p/inboundlimit.go:92`), as for the per-IP connection limits.
- **Pull-sync group:** the two inbound pull-sync protocol IDs, `/swarm/pullsync/1.4.0/pullsync` and `/swarm/pullsync/1.4.0/cursors`. **Other group:** every other Swarm protocol ID (retrieval, push-sync, hive, pricing, status, pseudosettle, swap, handshake follow-ups and so on).
- Rules 6, 8 and 10 are those of `AGENTS.md`: the frozen wire surface, measure before setting a default (and state what a setting costs), and no hosts or addresses in public text.

## Problem

Checked on `main` (`pkg/p2p/libp2p/libp2p.go:276-292`): the limits are built from `rcmgr.InfiniteLimits`, and only the `System` stream counts are set. `Peer`, `Protocol`, `ProtocolPeer` and `Transient` keep infinite limits. So:

1. **One peer can take the whole cap.** Before #640 this happened in production: one neighbour held about 4,500 of the 5,014 inbound streams on stake-1, and one peer each held 700 to 1,100 on seven of ten sw-1 nodes (#636). At the cap, the node refuses every new inbound stream from everyone, retrieval and push-sync included.
2. **Many identities behind one address can do the same.** Peer IDs cost nothing to create. The per-IP connection limit (200 connections per address, #597) bounds connections, not streams.
3. **One protocol can starve another.** The cap is shared, so a surge of retrieval or push-sync streams can leave no room for pull-sync, which the reserve and the storage incentives depend on, and the other way round (the #636 pile-up was all pull-sync and starved everything else).

The problem in point 1 now has a root-cause fix. Point 1 remains for other protocols, and points 2 and 3 remain in full.

### Corrections to the issue text

- The issue and its later comment estimated "about 30" and "about 32" pull-sync streams per neighbour. After #640 and #643 the bound is **at most 64 waiting pull-sync requests per peer** (two per bin, 32 bins), plus requests in flight, which are short.
- The comment's arithmetic (40 to 57 neighbours times a per-peer cap of 256 is more than the cap) stands: a per-peer limit alone cannot bound the node total. That is what the group limits are for.

## What was measured (sw-1, ten doubled nodes, and stake-1)

From the stream metrics of #639 (`bee_libp2p_inbound_streams_per_peer_max`, the largest inbound stream count held by any single peer, sampled every 30 s):

| Condition | Largest single peer | Total inbound per node |
|---|---|---|
| Before #640, 2.5 h after a restart | 210 to 354 (seven of ten nodes) | up to 530 |
| Before #640, days after a restart | 700 to 1,100 on sw-1; about 4,500 on stake-1 | up to 4,830 |
| With #640, during a refill (all ten nodes pulling 430 to 770 chunks/s), 3.5 h | **22 to 24 on every node** | 54 to 278 |

Neighbourhood sizes on these nodes: 36 to 57 peers on sw-1 (radius 6), 9 on stake-1 (radius 9). A peer outside the neighbourhood syncs only its own proximity bin, so it holds at most two waiting pull-sync requests.

What has not been measured: a single peer's retrieval and push-sync stream bursts (a forwarding node during a heavy upload or download). The 24-hour baseline the issue asked for exists only as the 3.5-hour refill window above; the measurement in this spec extends it before any default is turned on (see Decisions).

## Hypothesis

With per-peer, per-address and per-group inbound limits set well above any measured legitimate need, no single peer, address or protocol group can take the node's stream cap, and no legitimate peer is refused.

## Design

All limits are inbound only. Outbound streams are this node's own and are out of scope.

### Where refusals happen (verified in go-libp2p v0.48.0)

- **At stream accept:** `swarm_conn.go:139` calls `ResourceManager().OpenStream(peer, DirInbound)` before any protocol negotiation. This checks `System`, `Transient` and `Peer`. On failure the stream is reset with 4098 (`:141`). This is cheap: no protocol negotiation, no handler.
- **At protocol negotiation:** `basic_host.go:350` calls `SetProtocol(protoID)`, which moves the stream from the transient scope into the `Protocol` and `ProtocolPeer` scopes (`rcmgr.go:856-868`). On failure the stream is reset with 4098 (`:352`).

### 1. Per-peer inbound limit (resource manager, `PeerDefault`)

`PeerDefault.StreamsInbound = N_peer`. Counted across all protocols of one peer ID.

Sizing: a correct full neighbour needs at most 64 waiting pull-sync requests (#640, #643) plus a few in flight, plus retrieval, push-sync and control streams. Measured maximum with #640: 24. The proposed value **256** is four times the pull-sync bound and ten times the measured maximum, so only a faulty or hostile peer reaches it. With #640 in place, even the faulty requesters found in #636 stay at or below 64 waiting pull-sync requests, so they do not reach it either.

### 2. Per-address inbound limit (wrapper)

The resource manager has no per-address stream scope. The existing wrapper (`inboundLimiter`, `pkg/p2p/libp2p/inboundlimit.go:186`, which already wraps `OpenConnection` for #608) gains an `OpenStream` override:

- for an inbound stream, look up the address key of the peer's connection (from the host's network, injected after the host is built, as the known-full-peer set already is); a peer with connections from several keys counts against the key of its first connection;
- refuse with the resource manager's limit error when that key already holds `N_addr` inbound streams, otherwise call the inner `OpenStream` and wrap the returned scope so that `Done()` lowers the count once;
- loopback is exempt, as for the connection limits.

Sizing: one address may legitimately carry several nodes (an operator's host, or many clients behind one NAT). The proposed value **1,024** allows four nodes at the per-peer limit, or sixteen at the pull-sync bound, behind one address, and is about a fifth of the cap.

### 3. Protocol groups with guaranteed room (wrapper)

The resource manager limits each protocol separately but cannot reserve room for one protocol against the sum of the others. The `OpenStream` wrapper's scope also overrides `SetProtocol`:

- the pull-sync group may hold at most `cap - R_other` inbound streams;
- the other group may hold at most `cap - R_pull` inbound streams;
- a refusal returns the limit error, so libp2p resets with 4098 as for any protocol limit;
- `Done()` lowers the group count only for a stream that was counted.

This guarantees `R_pull` streams to pull-sync whatever the other protocols do, and `R_other` to the other protocols whatever pull-sync does. Proposed: **R_pull = 2,000**, **R_other = 1,000**.
- 2,000 covers 57 neighbours at the measured 24 each (about 1,370) with room; a full neighbourhood at the 64 worst case (about 3,650) can still use up to `cap - 1,000 = 4,000` when the others are idle.
- 1,000 for the others is about four times the largest total measured on any node with #640 (278).

### 4. Interactions

- **#640 and #641:** they keep each peer's pull-sync use at or below 64 waiting requests and free abandoned ones, so these limits are a backstop, not the main protection. Without them, the limits would refuse peers while freeing nothing.
- **Light and ultra-light peers:** they open retrieval and push-sync streams, not pull-sync. The per-peer limit applies to them as to anyone; 256 is far above their need. The light-node limits of #606 bound how many there are, not their streams.
- **Bootnodes:** they serve mostly hive. The limits apply unchanged; a bootnode never needs more than a few streams per peer.
- **The connection rate limit of #608:** unchanged. It acts on connections; these limits act on streams of admitted connections.

### 5. What a refused peer experiences

The peer's `NewStream` or first read fails with 4098. A wasp requester counts it in `bee_libp2p_streams_refused_by_peer_total` and its puller backs off (#573). **An upstream puller retries a failed sync at once, with no backoff** (upstream `pkg/puller/puller.go`), so a refused upstream neighbour can open and lose streams in a tight loop. A refusal at accept costs this node little (no negotiation, no handler), but the loop costs both sides CPU. With the proposed values, no correct requester reaches a limit, so the loop should only affect peers that are already misbehaving; the measurement checks this.

### 6. Metrics

- The resource manager already exports refusals by scope: `libp2p_rcmgr_blocked_resources{dir="inbound",scope=...,resource="stream"}`. That covers `peer`.
- New: `bee_libp2p_inbound_streams_refused_total{limit="address"|"group_pullsync"|"group_other"}` for the wrapper's refusals, with each label created at start so the series exports 0.
- New gauges: `bee_libp2p_inbound_streams_group{group="pullsync"|"other"}` and `bee_libp2p_inbound_streams_per_address_max`.

## Protocol impact

None. No message changes (rule 6). A refusal is the existing 4098 reset, which every libp2p peer already handles.

## Configuration

| Setting | Proposed value if on | Today's value |
|---|---|---|
| `p2p-inbound-streams-per-peer` | 256 | no limit |
| `p2p-inbound-streams-per-address` | 1,024 | no limit |
| `p2p-inbound-stream-reserve-pullsync` | 2,000 | none |
| `p2p-inbound-stream-reserve-other` | 1,000 | none |

`0` means no limit for the first two and no reserve for the last two; negative values are refused. Help text (rule 8 form), for example for the per-peer limit: "inbound streams one peer may hold at once, across all protocols; 0 means no limit. Raising it lets one peer take more of this node's stream cap; lowering it can refuse a legitimate neighbour, which then cannot sync from or retrieve through this node, and an upstream neighbour may retry in a tight loop." The others follow the same form. All four are documented in `docs/config-reference.yaml` and `docs/DIFFERENCES.md`.

Whether the values in the first column become the defaults is the operator's decision (see Decisions).

## Measurement

### Unit and integration tests

1. **Per-peer limit:** two libp2p test hosts; the client opens `N_peer + 1` inbound streams on a test protocol; the last is refused with 4098 and counted in the `peer` scope; after one stream closes, a new one is admitted.
2. **Per-address limit:** several peer IDs on one loopback-mapped address key (with the loopback exemption turned off for the test, as `inboundLimitLoopback` does for #608); the stream that passes `N_addr` is refused and counted as `address`; `Done()` releases exactly once (double `Done()` does not underflow).
3. **Groups:** with a small cap, fill the other group to its limit; a pull-sync stream is still admitted until `R_pull` is used; the reverse for the other group; refusals counted per group.
4. **Off:** with all four settings at 0, behaviour and limits are identical to `main` (the built limit config equals today's).
5. **Settings:** the real command flags (`setAllFlags`) give the defaults; 0 and explicit values pass through; negative values are refused at start.

Mutation checks: per-peer limit not applied; address count not released on `Done()`; address count released twice; group check applied to outbound streams; groups swapped; loopback not exempt.

### On a node

On sw-1, after the next build with #640, #641 and #643 is on all ten nodes:
- nodes 1 to 5 with the limits on at the proposed values, nodes 6 to 10 with them off, for 24 hours at normal load and through one refill window;
- record per node: `bee_libp2p_inbound_streams_per_peer_max`, the per-address maximum, the group gauges, `libp2p_rcmgr_blocked_resources{scope="peer"}`, the new refusal counter, `bee_libp2p_streams_refused_by_peer_total` (refusals we receive), and CPU;
- pass: zero refusals on the limited nodes from legitimate traffic, no difference in refill rate or CPU between the halves, and no tight retry loop from any refused peer.

Refusals this node causes to others are exactly its own `blocked_resources` and the new counter; there is no other way to see them from here.

## Rollout and rollback

Each limit is a setting; 0 turns it off without a rebuild. If the operator keeps them off by default, operators opt in.

## Upstream portability

Upstream has the same infinite peer, protocol and transient limits (`upstream/master` `pkg/p2p/libp2p/libp2p.go`, same construction). The per-peer limit is a one-line resource-manager change upstream could take. The address and group limits depend on wasp's resource-manager wrapper (#608). No `affects-upstream` label: the missing limits are a hardening gap, not a defect shown by a reproduction.

## Decisions for the operator

1. **On by default, or off by default (operators opt in)?** Rule 8 keeps today's value (no limit) unless the operator decides. Options:
   - **Off by default:** no risk to legitimate peers; the node stays exposed to points 2 and 3 of the problem until an operator turns them on.
   - **On by default at the proposed values:** protects every node; risk of refusing a legitimate peer whose bursts we have not measured (retrieval and push-sync forwarding), and of a tight retry loop from upstream neighbours that hit a limit.
   - **Recommended: off by default now, on by default after the 24-hour A/B on sw-1 shows zero legitimate refusals.** This matches #608, where the operator chose "on by default" once the values were measured.
2. **The values** (256 per peer, 1,024 per address, 2,000 and 1,000 reserved), or others.
3. **Per-address limit at all?** It is the most complex part (a wrapper with its own accounting). If many peers behind one address are not a concern for these nodes, it can be split into its own issue and the per-peer and group limits built first.

## To check during implementation

- Whether the resource manager's limit error from `OpenStream` can be returned unchanged from the wrapper so libp2p still resets with 4098 (the wrapper must return an error that `swarm_conn.go` treats like the inner one).
- How the address key of an inbound stream's connection is best found (the stream's own connection is not passed to `OpenStream`; the peer's connections are).
- Whether `PeerDefault` also needs `Streams` (both directions) left infinite explicitly when built from the partial config.
- The exact list of protocol IDs in the pull-sync group, read from the registered protocols rather than hard-coded.
