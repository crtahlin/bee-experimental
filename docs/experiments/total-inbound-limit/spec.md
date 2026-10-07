# A total limit on new and open inbound connections per node

Issue: [#608](https://github.com/crtahlin/wasp/issues/608). Epic: [#600](https://github.com/crtahlin/wasp/issues/600). Measurement it builds on: [#599](https://github.com/crtahlin/wasp/issues/599).
Type: optimization.
Depends on: the per-IP limit settings from #597, built in PR #606 (`pkg/p2p/libp2p/connlimits.go`). This change is implemented after that pull request merges, or on top of its branch.

## Terms

- **Security handshake:** the encrypted connection setup libp2p runs on every new connection, before anything else. On our nodes it is libp2p's TLS. Only after it does the node know the remote peer ID.
- **Resource manager:** libp2p's component that admits or refuses connections and streams against configured limits (`rcmgr`).
- **Connection gater:** a libp2p hook (`connmgr.ConnectionGater`) that can refuse a connection at fixed points. `InterceptAccept` is the point for a new inbound connection.
- **Token bucket:** a rate limit that refills at a fixed rate (connections per second) up to a maximum (the burst). Each admitted connection takes one token.
- **Known full peer:** a peer that completed a Swarm handshake with this node and declared itself a full node.
- **Address key:** an IPv4 address, or the /56 prefix of an IPv6 address. The same keys the per-IP limits use.
- Rules 6 and 8 are those of `AGENTS.md`: the frozen wire surface, and measuring before adding a setting.

## Problem

Every inbound connection costs CPU before the node can refuse it. Measured on #599 ([results](https://github.com/crtahlin/wasp/issues/599#issuecomment-6040276978)), one sw-1 node, 3 rounds of 20 minutes per condition, one 45-second CPU profile per condition:

- With the build of PR #606, which refuses a light peer at the limit, one refused attempt costs about 13 ms of CPU. The largest part is the security handshake (about 38 % of the extra CPU), then identify (about 14 %).
- On `main`, where the attempt evicts another light peer instead, one cycle costs about 60 ms.

The limits that protect a node must hold whatever its clients do: a client can be faulty or hostile, can use many IP addresses, and can make new identities for free.

Today every limit on new connections is **per address**. Verified in the source of `main` and of `upstream/v2.8.2`, `pkg/p2p/libp2p/libp2p.go`:

- the resource-manager limits are built from `rcmgr.InfiniteLimits`; only stream counts are set;
- the per-address limits are 200 open connections and 10 new connections per second with a burst of 40, per IPv4 address or IPv6 /56;
- the connection rate limiter is configured with `GlobalLimit: libp2prate.Limit{}`, which means no total limit;
- no connection gater is installed.

So a client with 100 addresses can make one node do about 1,000 security handshakes per second. At 13 ms each that is about 13 CPU cores, on any build (estimate from the measured cost, not measured at that rate). On a machine that runs several nodes, this also takes CPU from the reserve samples of the others.

**Today's load, for scale** (verified from the 24-hour baseline on #599, 11 nodes, counter `bee_libp2p_handled_connection_count` read every 5 minutes). The counter counts every connection that completed setup, in both directions, so it is an upper bound for inbound:
- average 0.13 to 0.28 connections per second per node;
- highest 5-minute average on a settled node: 0.72 per second;
- highest overall: 2.8 per second, on two nodes during their first hours after creation.

Open inbound connections on three sw-1 nodes at a quiet time (verified with `ss`, 2026-10-07): 27 to 50 per node, out of 153 to 180 connections in total.

### Corrections to the issue text

- The issue suggests the global limit of the existing connection rate limiter. **That limiter also counts the node's own outbound dials** (verified in go-libp2p v0.48.0: `resourceManager.openConnection` calls `connRateLimiter.Allow(ip)` for every direction, and the TCP transport's dial calls `OpenConnection(network.DirOutbound, ...)` at `p2p/transport/tcp/tcp.go:255`). A global limit there would slow kademlia's own dials under a flood. This spec uses a connection gater instead.
- The issue suggests exempting addresses of full peers in the address book. **The address book is also filled by hive gossip** (`pkg/hive/hive.go`), and a gossiped entry is stored with the same `verified` flag as one from a handshake. Anyone can gossip a signed full-node address carrying any IP address. This spec takes known full peers only from handshakes with this node.

## Hypothesis

A total budget for new inbound connections, checked before the security handshake, bounds the CPU a node spends on connection setup to roughly the budget times the cost per attempt, whatever the number of attacking addresses. A separate budget for known full peers keeps them connecting while the general budget is used up. The node's own outbound dials, and connections already open, are not affected.

## Design

### Where the checks run (verified in go-libp2p v0.48.0)

For every transport the node listens on (TCP, and WS and WSS when enabled), the inbound path is:

1. `gatedMaListener.Accept` (`p2p/net/upgrader/listener.go`) accepts the TCP connection;
2. it calls the gater's `InterceptAccept`; a refusal closes the connection;
3. it calls `resourceManager.OpenConnection(DirInbound, ...)`, which applies the per-address rate, the per-address count and the open-connection limits;
4. only then does `upgrader.Upgrade` run the security handshake (`setupSecurity`).

For WS and WSS, the gated listener wraps the raw TCP listener (`p2p/transport/websocket/listener.go`), so the gate also runs before the HTTP upgrade and before the WSS TLS.

So both new checks below run before any cryptographic work.

### 1. A total rate for new inbound connections, in a connection gater

A new type in `pkg/p2p/libp2p` implements `connmgr.ConnectionGater` and is installed with `libp2p.ConnectionGater`. Every method except `InterceptAccept` returns true, so nothing else changes. In particular, `InterceptPeerDial` and `InterceptAddrDial` allow everything, so outbound dials are never limited by this change.

`InterceptAccept` takes the remote IP from the connection's multiaddr and:
- **loopback addresses pass**, as they pass the per-IP limits today;
- **a known full peer's address key takes a token from the known-full-peer bucket;**
- **every other address takes a token from the general bucket;**
- if the bucket is empty, the connection is refused, and the refusal is counted by reason.

The two buckets use `golang.org/x/time/rate`, already a direct dependency. Both have the same rate and burst (settings below). The total admitted rate is therefore at most twice the setting.

Connections from a known full peer still pass through the per-address limits afterwards, so one known address cannot take more than the per-address rate.

### 2. A cap on open inbound connections, in the resource manager

The system scope's `ConnsInbound` limit is set to the configured cap. The resource manager counts it per direction (`resources.addConns`), so outbound connections are not counted.

When the cap is reached, the resource manager checks its allowlist (`rcmgr.GetAllowlist(rm)`) and admits an allowlisted address in a separate scope (`newAllowListedConnectionScope`, `rcmgr.go`). Known full peers' address keys are kept in that allowlist. Its system limit `ConnsInbound` is set to the same cap, so known full peers have a pool of their own, also bounded.

### 3. The set of known full peers

- **Added:** the remote address key of every peer that completes a handshake as a full node with this node, inbound or outbound. The hook is where the handshake result is known in `handleIncoming` and `Connect`.
- **Kept:** in a bounded map with a time to live: at most 10,000 address keys, each kept 24 hours after it was last seen. The `expirable` LRU from `hashicorp/golang-lru/v2` is already used for the same purpose in PR #606.
- **Not seeded from the address book** (see the corrections above). After a restart the set is empty. The node's own outbound dials to full peers are not limited and refill it quickly. Full peers that reconnect inbound in the first minutes use the general bucket, whose burst is sized for that (below).
- **The allowlist follows the set:** an address key is added to the resource-manager allowlist when it enters the set, and removed when it expires.
- **What a dishonest peer gains:** declaring `FullNode = true` costs nothing unless `chequebook-verification` is on. Such a peer's address becomes known. It then draws from the known-full-peer bucket, which has its own bound, and still meets the per-address limits. So it cannot exceed the total bound.

### 4. Metrics

- `bee_libp2p_inbound_refusals{reason}`, with the reasons `rate`, `rate_known_full` and `open_cap`. The open-cap refusal is counted from the resource manager's trace reporter, or by the gater wrapping it; the implementation chooses and documents which.
- `bee_libp2p_known_full_addresses`: the size of the set.

### Not in this change

- **A cap on connections in the middle of setup** (the resource manager's transient scope). It would bound how many slow handshakes run at once, but a client that holds that many open for the 15-second accept timeout would block every new inbound connection, full peers included. Left out until a measurement shows slow handshakes matter.
- **Refusing light peers before the security handshake.** Light peers can only be recognised from the Swarm handshake, which runs after the security handshake (see the note on #600 about refusing recently refused peer IDs).

## Protocol impact

None. No message, field, stream or protocol version changes. A refused connection is closed before the security handshake, which a stock dialer sees as a failed dial, the same as a refusal by today's per-address limits. `.github/protocol-freeze.lock` stays unchanged.

## Configuration

Three new settings, next to the per-IP settings of #597. A value of 0 or below means the default, as for those settings.

| Setting | Default | What it limits |
|---|---|---|
| `p2p-inbound-connection-rate` | 10 | new inbound connections per second, per bucket (general, and known full peers) |
| `p2p-inbound-connection-burst` | 100 | the burst of each bucket |
| `p2p-max-inbound-connections` | 500 | open inbound connections, per pool (general, and known full peers) |

**Why these defaults change nothing today (to be confirmed by the measurement):**
- The rate is 3.5 times the highest 5-minute average seen in 24 hours on 11 nodes (2.8 per second, on new nodes), and more than 10 times the highest on settled nodes.
- The burst of 100 allows 100 connections at once, for example full peers reconnecting after a restart.
- The open cap is 10 times the open inbound connections seen today (27 to 50), and more than 3 times the light limit (100) plus today's inbound full peers.

**Worst-case CPU at the defaults** (estimate): at most 20 connections per second reach the security handshake. At the measured 13 ms for a refused light attempt, that is about 0.26 cores per node. Connections that go further cost more: a full peer that kademlia accepts, or a light peer that gets a slot. Those are limited by kademlia's bins and by `light-node-limit`.

These defaults differ from today's behaviour, which has no total limit. Rule 8 asks for the current value as the default. This spec instead proposes defaults that today's load never reaches, because the point of the change is protection. The measurement below must show zero refusals at today's load before the defaults are accepted. If it does not, the defaults are raised, not the measurement explained away.

**Raising them:**
- more connection setups get through under a flood: more CPU for this node, and less for its reserve samples and for other nodes on the same machine;
- no cost to other nodes.

**Lowering them:**
- while a limit is reached, new light clients wait longer, and so do full peers that connect from an address this node has not completed a handshake with;
- set too low, this happens at normal load too: new full peers find this node harder to reach, and light clients go to other full nodes, which then carry more load;
- a full peer that cannot connect inbound can still be reached by this node's own outbound dials, which are not limited.

## Measurement

### Unit tests

- **Gater:**
  - with a fake clock, the general bucket admits the burst, then the rate, and refuses beyond it;
  - a known full peer's address uses its own bucket and is admitted while the general bucket is empty;
  - loopback is always admitted;
  - an IPv6 address is keyed by its /56;
  - the dial methods always return true.
- **Known set:** an entry expires after its time to live and is removed from the allowlist; the set stays at its bound.
- **Resource manager:** the limits built from the settings carry the configured `ConnsInbound` for the system and the allowlisted system scope.
- **Integration**, two libp2p services in one test:
  - inbound connections above the rate are refused before the security handshake: the handshake counter of the receiving service does not move for them;
  - the receiving service's own outbound dials succeed while its inbound bucket is empty.

**Mutation checks:** each check is removed in turn (the bucket, the known-full exemption, the loopback exemption, the `ConnsInbound` value), and each removal must fail a test.

### On a node

**1. Today's load, 24 hours.** Two sw-1 nodes on this build at the defaults, two on `main`, as in the 24-hour comparison on #599. Accepted when the new refusal counters stay at zero, and the connection and kademlia counters match the `main` nodes within the spread of the baseline.

**2. A flood, 3 runs of 20 minutes per condition.** The client load test on #599, against one sw-1 node:
- **To stand in for many addresses**, the target node's per-address limits are raised for the test (`p2p-connection-rate-per-ip` and `p2p-max-connections-per-ip`). The test clients run behind one public address, so without that change the per-address limit would refuse them first and the total limit would not be tested.
- **Clients:** ultra-light clients on bench-1 and bench-2, each asking to connect to the target every 500 ms, enough to exceed 20 attempts per second in total.
- **Conditions:** the build of PR #606 without this change, and this change at its defaults.
- **Recorded:** the node's process CPU, refusals by reason, inbound connections, kademlia's connected full peers, inbound full-peer connections during the flood, and a worst-case reserve sample per run.

**Accepted when, at the defaults:**
- connection-handling CPU stays under the bound above (about 0.26 cores over idle);
- it is lower than without this change, by about the share the refused handshakes had;
- kademlia's connected full peers do not fall below their level before the flood, and full peers still connect;
- the reserve sample stays within the spread of its baseline.

**A negative result looks like:**
- CPU does not fall, because the work moved to accepting TCP connections, or to the resource manager;
- kademlia loses full peers, or the known-full-peer bucket is used up by the flood;
- the new limits refuse connections at today's load.

Any of these stops the change at its current design.

## Rollout and rollback

On by default at the values above. An operator who wants no total limit sets the three settings to large values. Rolling back means running a build without the change. Nothing new is stored on disk.

## Upstream portability

`upstream/v2.8.2` has the same arrangement (verified): resource-manager limits from `InfiniteLimits`, a rate limiter with no global limit, per-address limits only, and no connection gater. The change applies there as written, apart from the per-IP settings of #597, which upstream does not have; the gater and the open cap do not depend on them.

This is a missing protection, not a defect in existing code, so it gets no `affects-upstream` label (rule 11).
