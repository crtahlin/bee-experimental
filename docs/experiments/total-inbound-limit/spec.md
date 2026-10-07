# A total rate limit on new inbound connections per node

Issue: [#608](https://github.com/crtahlin/wasp/issues/608). Epic: [#600](https://github.com/crtahlin/wasp/issues/600). Measurement it builds on: [#599](https://github.com/crtahlin/wasp/issues/599).
Type: optimization.
Depends on: the per-IP limit settings from #597, built in PR #606 (`pkg/p2p/libp2p/connlimits.go`). This change is implemented after that pull request merges, or on top of its branch.
Out of scope: a cap on open inbound connections, split out after review into [#614](https://github.com/crtahlin/wasp/issues/614), which first needs a measurement showing that held-open connections matter.

## Terms

- **Security handshake:** the encrypted connection setup libp2p runs on every new connection, before anything else. The node offers libp2p's TLS and Noise (`libp2p.DefaultSecurity`); the dialer's preference decides which one runs. Only after it does the node know the remote peer ID.
- **Identify:** the libp2p exchange, right after the security handshake, in which both sides send their addresses and supported protocols.
- **Resource manager:** libp2p's component that admits or refuses connections and streams against configured limits (`rcmgr`). Its `OpenConnection` is called for every new connection. The **scope** it returns tracks what that connection holds; `Done()` gives it back.
- **Main host:** the libp2p host that carries the node's Swarm protocols. The node also runs two helper hosts: the **AutoNAT dialer**, which answers other nodes' requests to check whether they are reachable by dialling them back, and the **ping dialer**, which checks a peer's reachability before kademlia keeps it. Both helper hosts use libp2p's default resource manager and are not covered by this change.
- **AutoNAT:** the libp2p service by which a node learns whether it is reachable from outside, by asking peers to dial it back.
- **NAT:** network address translation; several machines behind one router share its public address.
- **WS and WSS:** libp2p over WebSocket, without and with TLS. **AutoTLS** is the service that issues the WSS certificate.
- **Token bucket:** a rate limit that refills at a fixed rate (connections per second) up to a maximum (the burst). Each admitted connection takes one token.
- **Address key:** the unit every limit here counts by. The remote IP is read with `manet.ToIP` from the connection's remote multiaddr and converted with `netip.AddrFromSlice` and `Unmap()`. The key is the IPv4 address (/32), or the /56 prefix of an IPv6 address. `manet` already returns IPv4 peers as IPv4, so `Unmap()` is a safeguard. One function computes the key for every use below.
- **Known full peer:** a peer that completed a Swarm handshake with this node as a full node and that kademlia accepted (section 3).
- **LRU:** a bounded cache that drops the least recently used entry when full.
- **Hive gossip:** the hive protocol by which peers send each other lists of peer addresses.
- **Underlay:** a peer's network address (IP and port), as opposed to its overlay address in Swarm.
- Rules 6, 7, 8, 11 and 13 are those of `AGENTS.md`: the frozen wire surface, three runs per condition, measuring before adding a setting, marking defects that also exist upstream, and keeping `docs/DIFFERENCES.md` current.

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

So a client with 100 addresses can make one node run about 1,000 security handshakes per second. At 13 ms each that is about 13 CPU cores, on any build (estimate from the measured cost, not measured at that rate). On a machine that runs several nodes, this also takes CPU from the reserve samples of the others.

**Today's load, for scale** (from the 24-hour baseline, 11 nodes, counter `bee_libp2p_handled_connection_count` read every 5 minutes; the per-node numbers are posted on #599 [here](https://github.com/crtahlin/wasp/issues/599#issuecomment-6045010083)):
- average 0.13 to 0.28 connections per second per node;
- highest 5-minute average on a settled node: 0.72 per second;
- highest overall: 2.8 per second, on two nodes during their first hours after creation.

The counter counts connections in both directions, and only after the security handshake and the connection setup that follows it. The check in this spec also sees port scans and failed handshakes, so the rate it sees is higher than this counter by an unknown amount. Section 4 adds a counter for it.

### Corrections to the issue text

- **The issue suggests the global limit of the existing connection rate limiter.** That limiter also counts the node's own outbound dials. Verified in go-libp2p v0.48.0: `resourceManager.openConnection` calls `connRateLimiter.Allow(ip)` for every direction, and the TCP and WebSocket transports' dials call `OpenConnection(network.DirOutbound, ...)` (`p2p/transport/tcp/tcp.go:255`, `p2p/transport/websocket/websocket.go:167`). A global limit there would slow kademlia's own dials under a flood. This spec counts inbound connections only.
- **The issue suggests exempting addresses of full peers in the address book.** The address book is also filled by hive gossip (`pkg/hive/hive.go`). The underlays in a gossiped record are chosen by whoever signed the record and were never observed by this node. With `chequebook-verification` on, gossip writes them with `verified = true`, the same flag a handshake writes. So the address book cannot tell which addresses really belong to full peers. This spec takes known full peers only from handshakes with this node.
- **The issue also proposes a cap on open inbound connections.** Its review found three problems that need their own design and a measurement first; it moved to #614.

## Hypothesis

A total budget for new inbound connections, checked before the security handshake and after the per-address limits, bounds the CPU a node spends on connection setup to roughly the budget times the cost per attempt, whatever the number of attacking addresses. Because the per-address limits run first, one address cannot use up the budget. A separate budget for known full peers keeps them connecting while the general budget is used up. The node's own outbound dials and connections already open are not affected.

## Design

### Where the check runs (verified in go-libp2p v0.48.0)

For every transport the main host listens on (TCP, and WS and WSS when enabled), the inbound path is:

1. `gatedMaListener.Accept` (`p2p/net/upgrader/listener.go`) accepts the TCP connection; there is one accept goroutine per listener;
2. it calls a connection gater's `InterceptAccept`, if one is installed;
3. it calls `resourceManager.OpenConnection(DirInbound, ...)`, which applies the per-address rate and count;
4. only then does `upgrader.Upgrade` run the security handshake (`setupSecurity`).

For WS and WSS, the gated listener wraps the raw TCP listener (`p2p/transport/websocket/listener.go`), so steps 2 and 3 also run before the HTTP upgrade and before the WSS TLS. The shared-TCP listener path is not used, because the node does not set `ShareTCPListener`. The node does not listen on QUIC.

### 1. A wrapper around the resource manager

A new type in `pkg/p2p/libp2p` embeds the resource manager (`network.ResourceManager`) and replaces only `OpenConnection`. It is passed to the main host with `libp2p.ResourceManager`, in place of the plain one. Embedding loses only libp2p's check that warns when a connection manager's limits exceed the resource manager's (`connmgr.GetConnLimiter`); the node uses no connection manager.

For `DirOutbound`, it returns the inner result unchanged.

For `DirInbound`:
1. it calls the inner `OpenConnection` first, so the per-address rate and count apply before anything else;
2. if that admits the connection, it computes the address key and takes a token:
   - loopback: no token;
   - a known full peer's key: a token from the known-full-peer bucket;
   - any other key: a token from the general bucket;
3. if the bucket is empty, it calls `Done()` on the scope the inner call returned, which gives back the per-address count, and returns an error; the connection is closed before the security handshake.

The buckets use `golang.org/x/time/rate`, already a direct dependency, and only `Allow`, never `Wait`: the call runs inside each listener's single accept goroutine and must not block.

Because the inner call runs first, one address is held to the per-address rate (10 per second by default) before it reaches a bucket. The bucket rate is set to three times that (Configuration), so no single address can take more than a third of a bucket.

A wrapper is used, not a connection gater: a gater's `InterceptAccept` runs before the resource manager, so it would spend bucket tokens on attempts the per-address limits then refuse.

**Who shares a bucket with whom:**
- Light peers that connect from the same IPv4 address as a known full peer, for example a gateway next to a full node, or many users behind one carrier NAT with a full node among them, draw from the known-full-peer bucket. They are still held to the per-address rate.
- A WSS reverse proxy on the same host makes every proxied client look like loopback, so those clients are exempt from the buckets. Docker's userland proxy makes every client look like one bridge address, so they share one per-address rate. The operator documentation in #609 states both.

### 2. The set of known full peers

**Added:** the remote address key of a full peer, inbound or outbound, after `notifier.Connected` returns without error. That is the last check that can still disconnect the peer (kademlia's acceptance), and it comes after the blocklist check, the address-book write and the `ConnectIn` hooks. The address-book write runs only when the peer's addresses are in the peerstore (`len(peerAddrs) > 0`); the known set does not depend on it, because the key comes from the connection's own remote address. So a full peer behind NAT is recorded by the address it connects from.

**Kept:** a plain `lru.Cache` from `hashicorp/golang-lru/v2` (v2.0.7) with at most 10,000 keys, each holding the time it was last seen. This follows `lightannounce.go` in PR #606, which uses the plain cache because the expiring cache runs a cleanup goroutine that never stops and reads the real clock.
- "Last seen" is refreshed when the full peer connects, when it disconnects, and by a pass over the connected full peers every hour. A full peer that stays connected for days therefore stays in the set. A connection that ends in the "peer already exists" early return in `handleIncoming` does not refresh it; the hourly pass covers that peer.
- A key expires 24 hours after it was last seen. A sweep every 10 minutes removes expired keys. It runs on a goroutine that stops with the libp2p service, and reads an injected clock, so tests can move time.
- **The hot path reads with `Peek`**, which does not change the LRU order and takes only the cache's own lock. Writes (`Add`, the sweep) happen off the accept path.

**After a restart** the set is empty. The node's own outbound dials to full peers are not limited and refill it. Whether they refill it quickly enough is a hypothesis: an address learned from an outbound dial is the remote's listen address, and its inbound connections can come from another address (several network interfaces, NAT, IPv6 temporary addresses). Until then, full peers that reconnect inbound use the general bucket, whose burst is sized for a restart (Configuration). The restart run in the measurement tests this.

**What a dishonest peer gains:**
- Declaring `FullNode = true` costs nothing unless `chequebook-verification` is on. A peer that kademlia accepts as full gets its key into the set, and then draws from the known-full-peer bucket. It is still held to the per-address rate, so it can take at most a third of that bucket.
- **Pushing real peers out of the set:** with IPv6, an attacker can use many /56 keys. Each key enters the set only through a full-node handshake that kademlia accepts, and kademlia accepts a bounded number of peers per bin. Repeated over time this can still cycle fake keys through the 10,000 entries and push real full peers out. The real peers' own reconnects and the hourly pass put them back; until then, they use the general bucket. With `chequebook-verification` on, each fake full peer needs a deployed chequebook, which costs gas.

### 3. Bootnode mode

A node in bootnode mode keeps today's behaviour: no total rate, unless the operator sets one. A bootnode is the first contact for new nodes and clients, so every caller is unknown and would use the general bucket. This matches the light-peer churn spec, where bootnodes also keep today's behaviour.

### 4. Metrics

- `bee_libp2p_inbound_admitted{bucket}`, with `general` and `known_full`: connections the wrapper admitted, so the margin to the limit is visible, not only the refusals.
- `bee_libp2p_inbound_refusals{bucket}`, with the same labels.
- `bee_libp2p_known_full_addresses`: the size of the set.

The resource manager's existing per-address refusals stay visible through its own metrics.

## Protocol impact

None on the wire. No message, field, stream or protocol version changes. `.github/protocol-freeze.lock` stays unchanged.

The behaviour other nodes see does change while a bucket is empty: their dials to this node fail before the security handshake. Today the same failure happens only to one address that exceeds its per-address limit; with a total limit it happens to every node that tries to connect while the bucket is empty. The effects on them are listed under Configuration.

## Configuration

Two new settings, next to the per-IP settings of #597, with the same rule: a value of 0 or below means the default.

| Setting | Type | Default | What it limits |
|---|---|---|---|
| `p2p-inbound-connection-rate` | float, connections per second | 30 | the refill rate of each bucket (general, and known full peers) |
| `p2p-inbound-connection-burst` | integer | 200 | the burst of each bucket |

They are one token bucket and are proposed under one issue. To turn the limit off, an operator sets a rate far above any real load, for example `p2p-inbound-connection-rate: 1000000`; the documentation names that value. Bootnodes have it off unless set.

**Why these values:**
- **Rate:** three times the per-address rate of 10, so one address takes at most a third of a bucket. It is about 10 times the highest 5-minute average seen in 24 hours on 11 nodes (2.8 per second, on new nodes).
- **Burst:** after a restart, up to the light-node limit (100) plus the inbound full peers (about 50 today) reconnect within seconds, while the known set is still empty. 200 leaves a third more than that. Today's nodes hold at most 7 light peers, so a plain restart reconnects only about 57; the measurement restarts a node while test clients fill its light slots.

**Worst-case CPU at the defaults** (estimate): at most 60 connections per second (two buckets of 30) reach the security handshake. At the measured 13 ms for a refused light attempt, that is about 0.8 cores per node, against no bound today. Connections that go further cost more: a full peer that kademlia accepts, or a light peer that gets a slot. Those are limited by kademlia's bins and by `light-node-limit`.

**Raising them:**
- more connection setups get through under a flood: more CPU for this node, and less for its reserve samples and for other nodes on the same machine;
- no cost to other nodes.

**Lowering them**, or a flood that empties a bucket, costs other nodes, and this is the part an operator does not pay for:
- **New light clients wait longer**, and go to other full nodes, which then carry more load.
- **Full peers that do not know this node stop trying.** A stock node counts each failed dial as a failed attempt. After 4 failed attempts (`maxConnAttempts` in `pkg/topology/kademlia/kademlia.go`, verified at `upstream/v2.8.2`; 6 inside its neighbourhood) it removes this node from its known peers and from its address book. A flood that lasts about a minute can therefore make new full peers forget this node.
- **Other nodes may judge this node unreachable.** They check reachability with their ping dialer before kademlia keeps this node. If that dial is refused, they mark this node as not publicly reachable, and their kademlia drops it before other peers. The dial comes from the other node's own address, which is in the known set if that node has completed a full-node handshake with this one; from an unknown address it uses the general bucket.
- **This node may judge itself unreachable** (open question below).
- **This node's own dials are not affected**, so it keeps reaching full peers itself.

## Measurement

### Unit tests

- **Wrapper:**
  - with a fake clock, the general bucket admits the burst, then the rate, and refuses beyond it;
  - a known full peer's key uses its own bucket and is admitted while the general bucket is empty;
  - an attempt refused by the per-address limits takes no bucket token;
  - a refused connection gives back its scope (the inner resource manager's per-address count returns to its previous value);
  - one key that floods cannot starve a second key;
  - loopback takes no token;
  - an IPv4 address carried as IPv6 gets the IPv4 key; an IPv6 address is keyed by its /56;
  - outbound connections are passed through unchanged;
  - values of 0 or below give the defaults; bootnode mode has no limit.
- **Known set:**
  - with an injected clock, a key expires 24 hours after it was last seen, and a connected full peer refreshed by the hourly pass does not expire;
  - a peer that kademlia refuses (`notifier.Connected` returns an error) is not recorded;
  - the set stays at its bound.
- **Integration**, two libp2p services in one test:
  - inbound connections above the rate are refused before the security handshake: the receiving service's handshake counter does not move for them;
  - the receiving service's own outbound dials succeed while its inbound bucket is empty.

**Mutation checks:** each check is removed in turn (the bucket, the order after the inner call, the scope release, the known-full exemption, the loopback exemption, the key function's `Unmap`, the expiry), and each removal must fail a test.

### On a node

**What our hosts can produce.** On #599, 8 stock ultra-light clients on bench-1, each asking to connect every 500 ms, produced about 5.3 inbound connection attempts per second in total, about 0.67 per client. Each client holds about 137 connections, and the network bench-1 is on held about 1,100 to 1,865 connections in earlier tests. So bench-1 and bench-2 together can produce roughly 10 to 20 attempts per second (estimate), below the default rate of 30. The flood runs therefore lower the target's rate through the setting for the test. They test the mechanism and its cost per refused attempt, not the default values. The defaults are tested for false refusals by runs 1 and 2.

**1. Today's load.** Two sw-1 nodes on this build at the defaults against two on the build of PR #606, over three windows of 8 hours each (rule 7: three runs, reported with the spread). Accepted when the refusal counters stay at zero, the admitted counter shows the margin to the limits, and the connection and kademlia counters match the other nodes within the spread.

**2. A restart under load, 3 times.** On the target node, `light-node-limit` is set to 4, and 8 test clients from bench-1 fill its light slots, as on #599. The node is restarted at the default rate and burst. Recorded: refusals and the admitted rate in the first 10 minutes. Accepted when there are no refusals.

**3. A flood, 3 runs of 20 minutes per condition.** Against one sw-1 node (the target):
- **Test settings on the target:** `p2p-inbound-connection-rate: 2`, `p2p-inbound-connection-burst: 10`, and the per-address limits raised (`p2p-connection-rate-per-ip` and `p2p-max-connections-per-ip`), so that the clients behind one public address are not refused by the per-address limit first.
- **General bucket:** 16 ultra-light clients on bench-1 and bench-2, each asking to connect every 500 ms, about 10 attempts per second. Before each run, the target's `/peers` is checked for a full node from the bench network; if one is connected, the bench address is already known, and the run records that it tested the known-full-peer bucket instead.
- **Known-full-peer bucket:** 8 ultra-light clients on sw-1 itself, dialling the target's public address. Their source is sw-1's address, which is known once another sw-1 node is connected to the target as a full peer; this is checked in `bee_libp2p_known_full_addresses` and the target's `/peers` before the run. The clients take CPU on the same machine, so only the target process's CPU is compared.
- **Conditions:** the build of PR #606 without this change, and this change with the test settings.
- **Recorded:** the target process's CPU over idle, admissions and refusals by bucket, kademlia's connected full peers, the node's reachability as reported by `/topology`, a worst-case reserve sample per run, and whether a full peer on another bench machine still holds the target in its address book after the run.

**Accepted when:**
- the CPU per refused attempt, over idle, is much lower than the 13 ms of a refusal at the light limit, because no security handshake runs;
- admissions stay at the bucket rate;
- kademlia's connected full peers do not fall below their level before the flood, full peers still connect, and the full peer on the other bench machine still has the target in its address book;
- the target stays reachable;
- the reserve sample stays within the spread of its baseline.

**A negative result looks like:**
- the CPU per refused attempt does not fall, because the work moved to accepting TCP connections or to the resource manager;
- kademlia loses full peers, full peers drop the target from their address books, or the target judges itself unreachable;
- the defaults refuse connections at today's load or through a restart.

Any of these stops the change at its current design.

## Rollout and rollback

On by default at the values above, except in bootnode mode. An operator turns the limit off with a very high rate. Rolling back means running a build without the change. Nothing new is stored on disk. The implementation pull request adds the two settings and the new behaviour to `docs/DIFFERENCES.md` (rule 13).

## Upstream portability

`upstream/v2.8.2` has the same arrangement (verified): resource-manager limits from `InfiniteLimits`, a rate limiter with no global limit, per-address limits only, and no connection gater. The change applies there as written, apart from the per-IP settings of #597, which upstream does not have; the wrapper runs after upstream's compiled-in per-address limits in their place.

This is a missing protection, not a defect in existing code, so it gets no `affects-upstream` label (rule 11).

## Open questions for the operator

- **On by default (rule 8).** Rule 8 asks for the current value as the default, and today there is no total limit. This spec proposes the limit on by default, because it is a protection and today's load is far below it, and accepts the defaults only if runs 1 and 2 show no refusals. The operator decides whether a protection may ship on, or must ship off with the values documented.
- **Own reachability under a flood.** The node learns whether it is reachable from AutoNAT dial-backs (`reachabilityOverridePublic` is `"false"`, `pkg/p2p/libp2p/libp2p.go`). Those dial-backs are inbound to the main host and use the general bucket unless they come from a known full peer's address. If they are refused during a flood, the node may judge itself not publicly reachable, and `IsReachable()` gates storing pushed chunks in its neighbourhood (`pkg/pushsync/pushsync.go`). Run 3 records reachability; if it changes, the dial-backs need an exemption.
- **AutoTLS (hypothesis, not checked).** The certificate registration for WSS may dial the node back to confirm it is reachable. If so, those dials use the general bucket during a flood and could delay a certificate renewal. To check in the implementation.

## Review notes not applied

- **Seeding the known set after a restart from the chequebook registry** (suggested in review). Not applied: the registry (`pkg/settlement/swap/chequebook/registry.go`) is in memory only, records no source per entry, and removes entries when a peer disconnects, so it is empty after a restart too.
