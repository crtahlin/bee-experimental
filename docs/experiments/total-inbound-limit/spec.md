# A total limit on new and open inbound connections per node

Issue: [#608](https://github.com/crtahlin/wasp/issues/608). Epic: [#600](https://github.com/crtahlin/wasp/issues/600). Measurement it builds on: [#599](https://github.com/crtahlin/wasp/issues/599).
Type: optimization.
Depends on: the per-IP limit settings from #597, built in PR #606 (`pkg/p2p/libp2p/connlimits.go`). This change is implemented after that pull request merges, or on top of its branch.

## Terms

- **Security handshake:** the encrypted connection setup libp2p runs on every new connection, before anything else. Bee offers libp2p's TLS and Noise (`libp2p.DefaultSecurity`); the dialer's preference decides which one runs. Only after it does the node know the remote peer ID.
- **Resource manager:** libp2p's component that admits or refuses connections and streams against configured limits (`rcmgr`). Its `OpenConnection` is called for every new connection.
- **Token bucket:** a rate limit that refills at a fixed rate (connections per second) up to a maximum (the burst). Each admitted connection takes one token.
- **Address key:** the unit every limit here counts by. The remote IP is read with `manet.ToIP`, converted with `netip.AddrFromSlice` and `Unmap()`, so an IPv4 address carried as IPv6 is treated as IPv4. The key is then the IPv4 address (/32), or the /56 prefix of an IPv6 address. One function computes it for every use below.
- **Known full peer:** a peer that completed a Swarm handshake with this node as a full node and passed every later check of that connection.
- Rules 6, 8 and 11 are those of `AGENTS.md`: the frozen wire surface, measuring before adding a setting, and marking defects that also exist upstream.

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

**Today's load, for scale** (verified from the 24-hour baseline on #599, 11 nodes, counter `bee_libp2p_handled_connection_count` read every 5 minutes):
- average 0.13 to 0.28 connections per second per node;
- highest 5-minute average on a settled node: 0.72 per second;
- highest overall: 2.8 per second, on two nodes during their first hours after creation.

The counter counts connections in both directions, and only after the security handshake and the connection setup that follows it. The check in this spec also sees port scans and failed handshakes, so the rate it sees is higher than this counter by an unknown amount. Section 5 adds a counter for it.

Open inbound connections on three sw-1 nodes at a quiet time (verified with `ss`, 2026-10-07): 27 to 50 per node, out of 153 to 180 connections in total.

### Corrections to the issue text

- **The issue suggests the global limit of the existing connection rate limiter.** That limiter also counts the node's own outbound dials. Verified in go-libp2p v0.48.0: `resourceManager.openConnection` calls `connRateLimiter.Allow(ip)` for every direction, and the TCP transport's dial calls `OpenConnection(network.DirOutbound, ...)` at `p2p/transport/tcp/tcp.go:255`. A global limit there would slow kademlia's own dials under a flood. This spec counts inbound connections only.
- **The issue suggests exempting addresses of full peers in the address book.** The address book is also filled by hive gossip (`pkg/hive/hive.go`). The underlay addresses in a gossiped record are chosen by whoever signed the record and were never observed by this node. With `chequebook-verification` on, gossip writes them with `verified = true`, the same flag a handshake writes. So the address book cannot tell which addresses really belong to full peers. This spec takes known full peers only from handshakes with this node.

## Hypothesis

A total budget for new inbound connections, checked before the security handshake and after the per-address limits, bounds the CPU a node spends on connection setup to roughly the budget times the cost per attempt, whatever the number of attacking addresses. Because the per-address limits run first, one address cannot use up the budget. A separate budget for known full peers keeps them connecting while the general budget is used up. The node's own outbound dials and connections already open are not affected.

## Design

### Where the check runs (verified in go-libp2p v0.48.0)

For every transport the node listens on (TCP, and WS and WSS when enabled), the inbound path is:

1. `gatedMaListener.Accept` (`p2p/net/upgrader/listener.go`) accepts the TCP connection;
2. it calls a connection gater's `InterceptAccept`, if one is installed;
3. it calls `resourceManager.OpenConnection(DirInbound, ...)`, which applies the per-address rate, the per-address count and the open-connection limits;
4. only then does `upgrader.Upgrade` run the security handshake (`setupSecurity`).

For WS and WSS, the gated listener wraps the raw TCP listener (`p2p/transport/websocket/listener.go`), so steps 2 and 3 also run before the HTTP upgrade and before the WSS TLS. The shared-TCP listener path is not used, because the node does not set `ShareTCPListener`.

### 1. A wrapper around the resource manager

A new type in `pkg/p2p/libp2p` embeds the resource manager (`network.ResourceManager`) and replaces only `OpenConnection`. It is passed to libp2p with `libp2p.ResourceManager`, in place of the plain one. The node keeps a reference to the inner resource manager for `rcmgr.GetAllowlist`.

For `DirOutbound`, it returns the inner result unchanged.

For `DirInbound`:
1. it calls the inner `OpenConnection` first, so the per-address rate and count, and the open-connection cap of section 2, apply before anything else;
2. if that admits the connection, it computes the address key and takes a token:
   - loopback: no token;
   - a known full peer's key: a token from the known-full-peer bucket;
   - any other key: a token from the general bucket;
3. if the bucket is empty, it calls `Done()` on the scope it was given, which releases the counts the inner call took, and returns an error; the connection is closed before the security handshake.

The buckets use `golang.org/x/time/rate`, already a direct dependency, and only `Allow`, never `Wait`: the call runs inside each listener's single accept loop and must not block.

Because the inner call runs first, one address is held to the per-address rate (10 per second by default) before it reaches a bucket. The bucket rates are set to several times that (Configuration), so no single address can take more than a share of a bucket. A unit test checks that one key that floods cannot starve a second key.

A wrapper is used, not a connection gater: a gater's `InterceptAccept` runs before the resource manager, so it would spend global tokens on attempts the per-address limits then refuse.

### 2. A cap on open inbound connections

The system scope's `ConnsInbound` limit is set to the configured cap. The resource manager counts it per direction (`resources.addConns`), so outbound connections are not counted.

When the cap is reached, the resource manager checks its allowlist and, for an allowlisted address, retries in a separate scope (`newAllowListedConnectionScope`, `rcmgr.go`). Known full peers' address keys are kept in that allowlist. The allowlisted system scope's `ConnsInbound` (`PartialLimitConfig.AllowlistedSystem`) is set to the same cap, so known full peers have a pool of their own, also bounded.

Loopback connections count toward the cap. This is intended: local tools open few connections. A WSS reverse proxy on the same host makes every proxied client look like loopback, so those clients are exempt from the rates and counted only by the cap; Docker's userland proxy makes every client look like one bridge address, so they share one per-address limit. Operators who run either should know this; the operator documentation in #609 states it.

### 3. The set of known full peers

**Added:** the remote address key of a full peer, inbound or outbound, only after every check of that connection has passed: in `handleIncoming` after the blocklist check, the address-book write and the `ConnectIn` hooks; in `Connect` at the same point.

**Kept:** a plain `lru.Cache` from `hashicorp/golang-lru/v2` with at most 10,000 keys, each holding the time it was last seen. This follows `lightannounce.go` in PR #606, which uses the plain cache because the expiring cache runs a cleanup goroutine that never stops, and reads the real clock.
- "Last seen" is refreshed when the full peer connects, when it disconnects, and by a pass over the connected full peers every hour. A full peer that stays connected for days therefore stays in the set.
- A key expires 24 hours after it was last seen.
- A sweep every 10 minutes removes expired keys. It runs on a goroutine that stops with the libp2p service, and reads an injected clock, so tests can move time.

**The allowlist follows the set**, under one mutex that covers both:
- a key is added to the allowlist only when it is not in the set yet, so the allowlist never holds a key twice;
- it is removed when the sweep expires it or the LRU evicts it. The LRU's eviction callback runs while the LRU holds its own lock, so the callback only records the key, and the removal from the allowlist happens after the LRU call returns, still under the outer mutex.
- Multiaddr forms: `/ip4/<address>` for an IPv4 key, and `/ip6/<prefix>/ipcidr/56` with the prefix already masked, so that `Allowlist.Remove` finds exactly the entry `Add` made.
- Cost: `Allowlist.Allowed` scans every entry. It is called only for a connection that reached the open cap, and the buckets limit those to at most the sum of the two bucket rates per second. At 10,000 entries this is a scan of 10,000 entries per such connection (estimate, to be measured in the unit benchmark).

**After a restart** the set is empty. The node's own outbound dials to full peers are not limited and refill it. Whether they refill it quickly enough is a hypothesis: an address learned from an outbound dial is the remote's listen address, and its inbound connections can come from another address (several network interfaces, NAT, IPv6 temporary addresses). Until then, full peers that reconnect inbound use the general bucket, whose burst is sized for a restart (Configuration). The restart run in the measurement tests it.

**What a dishonest peer gains:** declaring `FullNode = true` costs nothing unless `chequebook-verification` is on. Such a peer's key becomes known. It then draws from the known-full-peer bucket, still after the per-address limits, so it can take at most the per-address rate out of that bucket. One known full peer allowlists its whole /56 for the open cap, which can include other customers of the same hosting provider; the cap of the allowlisted pool still bounds them.

### 4. Bootnode mode

A node in bootnode mode keeps today's behaviour: no total rate and no open cap, unless the operator sets them. A bootnode is the first contact for new nodes and clients, so every caller is unknown and would use the general bucket. This matches the light-peer churn spec, where bootnodes also keep today's behaviour.

### 5. Metrics

- `bee_libp2p_inbound_admitted{bucket}`, with `general` and `known_full`: connections the wrapper admitted, so the margin to the limit is visible, not only the refusals.
- `bee_libp2p_inbound_refusals{reason}`, with `rate` and `rate_known_full`, counted by the wrapper.
- Refusals at the open cap are counted from the error the inner `OpenConnection` returns: a refusal by the system `ConnsInbound` limit returns `network.ErrResourceLimitExceeded` from `addConns`, which differs from the per-address rate and count refusals (`rcmgr.go`, `openConnection`). The wrapper counts it as `reason="open_cap"`.
- `bee_libp2p_known_full_addresses`: the size of the set.

### Not in this change

- **A cap on connections in the middle of setup** (the resource manager's transient scope). It would bound how many slow handshakes run at once, but a client that holds that many open for the 15-second accept timeout would block every new inbound connection, full peers included. Left out until a measurement shows slow handshakes matter.
- **Refusing light peers before the security handshake.** Light peers can only be recognised from the Swarm handshake, which runs after the security handshake (see the note on #600 about refusing recently refused peer IDs).

## Protocol impact

None on the wire. No message, field, stream or protocol version changes. `.github/protocol-freeze.lock` stays unchanged.

The behaviour other nodes see does change while a limit is reached: their dials to this node fail before the security handshake. Today the same failure happens only to one address that exceeds its per-address limit; with a total limit it happens to every node that tries to connect while the bucket is empty. The effects on them are listed under Configuration.

## Configuration

Three new settings, next to the per-IP settings of #597. A value of 0 means the default. A value of -1 turns that limit off, which is today's behaviour.

| Setting | Default | What it limits |
|---|---|---|
| `p2p-inbound-connection-rate` | 30 | new inbound connections per second, per bucket (general, and known full peers) |
| `p2p-inbound-connection-burst` | 150 | the burst of each bucket |
| `p2p-max-inbound-connections` | 0, meaning `light-node-limit` + 400 (500 at today's light limit) | open inbound connections, per pool (general, and known full peers) |

**Why these values:**
- **Rate:** three times the per-address rate of 10, so one address takes at most a third of a bucket. It is about 10 times the highest 5-minute average seen in 24 hours on 11 nodes (2.8 per second, on new nodes).
- **Burst:** 150 covers a restart, when about the light limit plus the inbound full peers (about 150 today) reconnect within seconds while the known set is still empty.
- **Open cap:** it follows `light-node-limit`, so an operator who raises the light limit does not hit the cap at normal load. At today's values it is 10 times the open inbound connections seen today (27 to 50).

**Worst-case CPU at the defaults** (estimate): at most 60 connections per second (two buckets of 30) reach the security handshake. At the measured 13 ms for a refused light attempt, that is about 0.8 cores per node, against no bound today. Connections that go further cost more: a full peer that kademlia accepts, or a light peer that gets a slot. Those are limited by kademlia's bins and by `light-node-limit`.

**Rule 8:** today there is no total limit. This spec proposes defaults that today's load is not expected to reach, because the point of the change is protection, not tuning. The measurement must show zero refusals at today's load, and through a restart, before the defaults are accepted. If it does not, the defaults are raised.

**Raising them:**
- more connection setups get through under a flood: more CPU for this node, and less for its reserve samples and for other nodes on the same machine;
- no cost to other nodes.

**Lowering them**, or a flood that reaches them, costs other nodes, and this is the part an operator does not pay for:
- **New light clients wait longer**, and go to other full nodes, which then carry more load.
- **Full peers that do not know this node stop trying.** A stock node counts each failed dial as a failed attempt. After 4 failed attempts (`maxConnAttempts` in `pkg/topology/kademlia/kademlia.go`, verified at `upstream/v2.8.2`; a different count applies inside its neighbourhood) it removes this node from its known peers and from its address book. A flood that lasts a minute or two can therefore make new full peers forget this node.
- **Other nodes may judge this node unreachable.** They check reachability by dialling from a separate libp2p host (the ping dialer). If that dial is refused, they mark this node as not publicly reachable, and their kademlia drops it before other peers. The check comes from the other node's own address, which is in the known set if that node has completed a full-node handshake with this one; from an unknown address it uses the general bucket.
- **This node's own dials are not affected**, so it keeps reaching full peers itself.

## Measurement

### Unit tests

- **Wrapper:**
  - with a fake clock, the general bucket admits the burst, then the rate, and refuses beyond it;
  - a known full peer's key uses its own bucket and is admitted while the general bucket is empty;
  - an attempt refused by the per-address limits takes no bucket token;
  - a refused connection releases its scope (the inner resource manager's counts return to their previous values);
  - one key that floods cannot starve a second key;
  - loopback takes no token;
  - an IPv4 address carried as IPv6 gets the IPv4 key; an IPv6 address is keyed by its /56;
  - outbound connections are passed through unchanged;
  - with -1, nothing is refused.
- **Known set:**
  - with an injected clock, a key expires 24 hours after it was last seen, and a connected full peer refreshed by the hourly pass does not expire;
  - adding the same key twice, then expiring it, leaves no allowlist entry;
  - the set stays at its bound, and an evicted key leaves the allowlist.
- **Resource manager:** the limits built from the settings carry the configured `ConnsInbound` for the system and the allowlisted system scope; the open cap follows `light-node-limit` when the setting is 0.
- **Integration**, two libp2p services in one test:
  - inbound connections above the rate are refused before the security handshake: the receiving service's handshake counter does not move for them;
  - the receiving service's own outbound dials succeed while its inbound bucket is empty.
- **Benchmark:** `Allowlist.Allowed` with 10,000 entries.

**Mutation checks:** each check is removed in turn (the bucket, the order after the inner call, the scope release, the known-full exemption, the loopback exemption, the key function's `Unmap`, the `ConnsInbound` value), and each removal must fail a test.

### On a node

**1. Today's load, 24 hours.** Two sw-1 nodes on this build at the defaults, two on `main`, as in the 24-hour comparison on #599. Accepted when the refusal counters stay at zero, the admitted counter shows the margin to the limits, and the connection and kademlia counters match the `main` nodes within the spread of the baseline.

**2. A restart, 3 times.** Restart one node at the defaults and record refusals and the admitted rate in the first 10 minutes. Accepted when there are no refusals.

**3. A flood, 3 runs of 20 minutes per condition.** The client load test on #599, against one sw-1 node:
- **To stand in for many addresses**, the target node's per-address limits are raised for the test (`p2p-connection-rate-per-ip` and `p2p-max-connections-per-ip`). The test clients run behind one public address, so without that change the per-address limit would refuse them first and the total limit would not be tested.
- **Clients:** ultra-light clients on bench-1 and bench-2, each asking to connect to the target every 500 ms, enough to exceed 60 attempts per second in total.
- **Conditions:** the build of PR #606 without this change, and this change at its defaults.
- **Recorded:** the node's process CPU, refusals and admissions by reason, kademlia's connected full peers, inbound full-peer connections during the flood, a worst-case reserve sample per run, and whether a full peer on another bench machine still holds this node in its address book after the run.

**4. The open cap, 3 runs of 20 minutes.** As in run 3, with `p2p-max-inbound-connections: 20` and clients that keep their connections open. Records refusals with reason `open_cap`, and checks that known full peers are still admitted through the allowlisted pool.

**Accepted when, at the defaults:**
- connection-handling CPU stays under the bound above (about 0.8 cores over idle);
- it is lower than without this change, by about the share the refused handshakes had;
- kademlia's connected full peers do not fall below their level before the flood, full peers still connect, and the full peer on the other bench machine still has this node in its address book;
- the reserve sample stays within the spread of its baseline;
- in run 4, `open_cap` refusals appear and known full peers are admitted.

**A negative result looks like:**
- CPU does not fall, because the work moved to accepting TCP connections, or to the resource manager;
- kademlia loses full peers, the known-full-peer bucket is used up by the flood, or full peers drop this node from their address books;
- the new limits refuse connections at today's load or through a restart.

Any of these stops the change at its current design.

## Rollout and rollback

On by default at the values above, except in bootnode mode. An operator turns any limit off by setting it to -1. Rolling back means running a build without the change. Nothing new is stored on disk.

## Upstream portability

`upstream/v2.8.2` has the same arrangement (verified): resource-manager limits from `InfiniteLimits`, a rate limiter with no global limit, per-address limits only, and no connection gater. The change applies there as written, apart from the per-IP settings of #597, which upstream does not have; the wrapper and the open cap use upstream's compiled-in per-address limits in their place.

This is a missing protection, not a defect in existing code, so it gets no `affects-upstream` label (rule 11).

## Open questions

- **Rule 8 asks for one issue per setting.** This spec adds three settings under #608. They only make sense together, so the spec proposes keeping them under one issue; the operator decides.
- **AutoTLS (hypothesis, not checked):** the certificate registration used for WSS may dial the node back to confirm it is reachable. If so, those dials use the general bucket during a flood and could delay a certificate renewal. To check in the implementation.
