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

**Today's load, for scale** (from the 24-hour baseline, 11 nodes, counter `bee_libp2p_handled_connection_count` read every 5 minutes; the per-node numbers are posted on #599 [here](https://github.com/crtahlin/wasp/issues/599#issuecomment-6045010083), with a [correction of the stake-1 row](https://github.com/crtahlin/wasp/issues/599#issuecomment-6045125372)):
- average 0.13 to 0.28 connections per second per node;
- highest 5-minute average on a settled node, outside a restart: 0.72 per second (stake-1);
- in the 5 minutes that contain a restart: 2.9 per second (stake-1, its one restart in the window, counted as the counter value after the restart);
- 2.5 and 2.8 per second on two new sw-1 nodes during their first hours after creation.

The counter counts connections in both directions, and only after the security handshake and the connection setup that follows it. The check in this spec also sees port scans and failed handshakes, so the rate it sees is higher than this counter by an unknown amount. Section 5 adds a counter for it.

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

**Added:** the remote address key of a full peer, in two places.
- **Inbound,** in `handleIncoming`, after `notifier.Connected` returns without error. That is the last check that can still disconnect the peer (kademlia's acceptance), and it comes after the blocklist check, the address-book write and the `ConnectIn` hooks.
- **Outbound,** at the end of `Connect`, after its final `peers.Exists` check, for a peer that declared itself full. The outbound path never calls `notifier.Connected` (verified); kademlia dialled the peer itself, so it wanted it.

 The address-book write runs only when the peer's addresses are in the peerstore (`len(peerAddrs) > 0`); the known set does not depend on it, because the key comes from the connection's own remote address. So a full peer behind NAT is recorded by the address it connects from.

**Kept:** a plain `lru.Cache` from `hashicorp/golang-lru/v2` (v2.0.7) with at most 10,000 keys, each holding the time it was last seen. This follows `lightannounce.go` in PR #606, which uses the plain cache because the expiring cache runs a cleanup goroutine that never stops and reads the real clock.
- "Last seen" is refreshed when the full peer connects, when it disconnects, and by a pass over the connected full peers every hour. A full peer that stays connected for days therefore stays in the set. A connection that ends in the "peer already exists" early return in `handleIncoming` does not refresh it; the hourly pass covers that peer.
- **A disconnect only refreshes an address already in the set; it never adds one** (decided during implementation). Kademlia refusing a full peer ends in a disconnect of that peer, so adding the address at disconnect would record a full peer that kademlia did not accept, against the rule that addresses come only from handshakes kademlia accepted. A test found this in the first implementation.
- **With the limit off** (-1, or a bootnode without either setting), nothing is recorded and the sweep does not run.
- A key expires 24 hours after it was last seen. A sweep every 10 minutes removes expired keys. It runs on a goroutine that stops with the libp2p service, and reads an injected clock, so tests can move time.
- The sweep's read, expiry check and removal are not one step, so a refresh in between could be undone. All writers (the refreshes, the hourly pass and the sweep) therefore take one mutex of their own. The accept path never takes it.
- **The hot path reads with `Peek`**, which does not change the LRU order and takes only the cache's own lock. Writes (`Add`, the sweep) happen off the accept path.

**After a restart** the set is empty. The node's own outbound dials to full peers are not limited and refill it. Whether they refill it quickly enough is a hypothesis: an address learned from an outbound dial is the remote's listen address, and its inbound connections can come from another address (several network interfaces, NAT, IPv6 temporary addresses). Until then, full peers that reconnect inbound use the general bucket, whose burst is sized for a restart (Configuration). The restart run in the measurement tests this.

**What a dishonest peer gains:**
- Declaring `FullNode = true` costs nothing unless `chequebook-verification` is on. A peer that kademlia accepts as full gets its key into the set, and then draws from the known-full-peer bucket. It is still held to the per-address rate, so it can take at most a third of that bucket. **So an attacker with three known keys empties the known-full-peer bucket.** Without `chequebook-verification`, a key costs one full-node handshake that kademlia accepts. The exemption therefore protects known full peers against floods from unknown addresses only, not against an attacker who has made itself known from three addresses.
- **Pushing real peers out of the set:** with IPv6, an attacker can use many /56 keys. Each key enters the set only through a full-node handshake that kademlia accepts, and kademlia accepts a bounded number of peers per bin. Repeated over time this can still cycle fake keys through the 10,000 entries and push real full peers out. The real peers' own reconnects and the hourly pass put them back; until then, they use the general bucket. With `chequebook-verification` on, each fake full peer needs a deployed chequebook, which costs gas.

### 3. Bootnode mode

A node in bootnode mode keeps today's behaviour: no total rate, unless the operator sets one. Because 0 means the default, "sets one" is read with the configuration's `IsSet` check on either of the two settings, as `cmd/bee/cmd/start.go` already does for other options. `IsSet` covers the configuration file, flags and environment variables. A bootnode with only the burst set gets the limit with the default rate. A bootnode is the first contact for new nodes and clients, so every caller is unknown and would use the general bucket. This matches the light-peer churn spec, where bootnodes also keep today's behaviour.

### 4. This node's own reachability during a flood

**How the node learns it is reachable** (verified in `pkg/p2p/libp2p/libp2p.go` and go-libp2p v0.48.0, `p2p/host/autonat` and `config/config.go`): `reachabilityOverridePublic` is `"false"`, so AutoNAT (version 1; version 2 is not enabled) decides.

**Two AutoNAT clients run on the main host.** `libp2p.New` always adds one (`cfg.addAutoNAT`, without the dial-back service, since the node does not set `EnableNATService`), and the node then adds a second one with the service (`autonat.New(h, autonat.EnableService(dialer.Network()))` in `libp2p.go`). Both emit `EvtLocalReachabilityChanged` on the host's event bus, and the node's `reachabilityWorker` passes on whichever event arrives last. So probes run about twice as often as one client would make them, each client counts its own failures, and the reported state is that of the client that changed last. Filed separately as #615; the decision below holds with either client.

Each AutoNAT client asks one of its connected peers to dial it back. The peer is picked at random among the connected peers that support the AutoNAT protocol and have a public address that is not on this node's own IP (`getPeerToProbe`, `dialPolicy.skipPeer`). So other nodes on the same machine are never asked. The asked peer dials back from its own AutoNAT dialer host, a separate libp2p host on the same machine, with its own peer ID and a new port, but normally the same IP as the peer's main connection (`autoNATService.doDial` with `config.dialer`; the node creates that host with `o.hostFactory`).

**So the dial-backs normally come from known full peers' addresses**, because the asked peers are connected peers with public addresses, which are in practice full peers that completed a handshake with this node. They use the known-full-peer bucket. A light peer with a public address that supports AutoNAT could also be asked; its dial-back uses the general bucket. A peer whose dialer host leaves from another IP than its main connection (several network interfaces) also uses the general bucket.

**How many refusals turn the node private** (verified in `autonat.go`, per client): when the node is public with full confidence (3), each failed dial-back lowers the confidence by one, and the fourth failure switches it to private. The failures need not be strictly consecutive: a success while public raises the confidence by one again, and a result that is neither success nor dial failure lowers it without switching. A refused dial-back is recorded as a dial failure. While public and receiving inbound connections, a probe runs every 30 minutes (`refreshInterval` of 15 minutes, doubled); after the first failure, every 90 seconds (`retryInterval`). So a flood must refuse the dial-backs for about 4.5 minutes after a failed probe before the node turns private. One successful dial-back switches it back to public at once, and retries continue until the confidence is back at 3. While private, `IsReachable()` is false, and the node stops storing pushed chunks for its own neighbourhood (`pkg/pushsync/pushsync.go`).

**Decision:** no exemption for dial-backs. They cannot be recognised before the security handshake: the dialer host's peer ID is unknown until then, and only the IP is available. That IP is a known full peer's, so the dial-backs already fall in the known-full-peer bucket. The remaining risk is a flood that empties that bucket, which section 2 bounds to an attacker with three known keys. The change adds:
- the gauge `bee_libp2p_reachability_public` (1 when public), and
- a warning in the log when the node turns private while the wrapper has refused connections in the last 5 minutes, naming both, so the cause can be seen. The time of the last refusal is read from the injected clock, so the 5-minute window is testable;
- a counter `bee_libp2p_reachability_private_switches`, increased on every change to private, so a short private period between two samples is not missed.

### 5. Metrics

- `bee_libp2p_inbound_admitted{bucket}`, with `general` and `known_full`: connections the wrapper admitted, so the margin to the limit is visible, not only the refusals.
- `bee_libp2p_inbound_refusals{bucket}`, with the same labels.
- `bee_libp2p_known_full_addresses`: the size of the set.

The resource manager's existing per-address refusals stay visible through its own metrics.

## Protocol impact

None on the wire. No message, field, stream or protocol version changes. `.github/protocol-freeze.lock` stays unchanged.

The behaviour other nodes see does change while a bucket is empty: their dials to this node fail before the security handshake. Today the same failure happens only to one address that exceeds its per-address limit; with a total limit it happens to every node that tries to connect while the bucket is empty. The effects on them are listed under Configuration.

## Configuration

Two new settings, next to the per-IP settings of #597. Like every node option, each can be set in the configuration file, as a command-line flag, or as a `BEE_` environment variable. A value of 0 means the default, as for the per-IP settings. A value of -1 turns the limit off, which is today's behaviour. Any other negative value is refused at start with an error. This differs from the per-IP settings, where every value of 0 or below means the default; the documentation of both states it.

| Setting | Type | Default | What it limits |
|---|---|---|---|
| `p2p-inbound-connection-rate` | float, connections per second | 30 | the refill rate of each bucket (general, and known full peers) |
| `p2p-inbound-connection-burst` | integer | 200 | the burst of each bucket |

They are one token bucket and are proposed under one issue. Setting either to -1 turns the limit off. Bootnodes have it off unless set.

**Decided by the operator: the limit is on by default.** Rule 8 asks for the current value as the default, and today there is no total limit. The operator decided that this protection ships on, because it is a protection rather than a tuning, and the defaults are far above the measured load (30 per second against at most 2.9 seen). The defaults are accepted only if runs 1 and 2 show no refusals; otherwise they are raised.

**Why these values:**
- **Rate:** three times the per-address rate of 10, so one address takes at most a third of a bucket. It is about 10 times the highest 5-minute average seen in 24 hours on 11 nodes (2.8 per second, on new nodes).
- **Burst:** after a restart, up to the light-node limit (100) plus the inbound full peers (about 50 today) reconnect within seconds, while the known set is still empty. 200 leaves a third more than that. Today's nodes hold at most 7 light peers, so a plain restart reconnects only about 57. Run 2 restarts a node with test clients in its light slots, but its light limit is 4 for that run, so it also exercises only about 58 reconnects: it tests the mechanism at small scale. The case of about 150 reconnects is not tested.

**Worst-case CPU at the defaults** (estimate): at most 60 connections per second (two buckets of 30) reach the security handshake. At the measured 13 ms for a refused light attempt, that is about 0.8 cores per node, against no bound today. Connections that go further cost more: a full peer that kademlia accepts, or a light peer that gets a slot. Those are limited by kademlia's bins and by `light-node-limit`.

**Raising them:**
- more connection setups get through under a flood: more CPU for this node, and less for its reserve samples and for other nodes on the same machine;
- no cost to other nodes.

**Lowering them**, or a flood that empties a bucket, costs other nodes, and this is the part an operator does not pay for:
- **New light clients wait longer**, and go to other full nodes, which then carry more load.
- **Full peers that do not know this node stop trying.** A stock node counts each failed dial as a failed attempt. After 4 failed attempts (`maxConnAttempts` in `pkg/topology/kademlia/kademlia.go`, verified at `upstream/v2.8.2`; 6 inside its neighbourhood) it removes this node from its known peers and from its address book. A flood that lasts about a minute can therefore make new full peers forget this node.
- **Other nodes may judge this node unreachable.** They check reachability with their ping dialer before kademlia keeps this node. If that dial is refused, they mark this node as not publicly reachable, and their kademlia drops it before other peers. The dial comes from the other node's own address, which is in the known set if that node has completed a full-node handshake with this one; from an unknown address it uses the general bucket.
- **This node may judge itself unreachable** if its own reachability checks are refused (section 4).
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
  - 0 gives the defaults, -1 turns the limit off, and any other negative value, such as -0.5, is refused at start; a burst of -1 alone turns the limit off; bootnode mode has no limit unless either setting is set;
  - the warning is logged when the reachability changes to private within 5 minutes of a refusal, and not otherwise.
- **Known set:**
  - with an injected clock, a key expires 24 hours after it was last seen, and a connected full peer refreshed by the hourly pass does not expire;
  - a peer that kademlia refuses (`notifier.Connected` returns an error) is not recorded;
  - the set stays at its bound.
- **Integration**, two libp2p services in one test:
  - inbound connections above the rate are refused before the security handshake: the receiving service's handshake counter does not move for them;
  - the receiving service's own outbound dials succeed while its inbound bucket is empty.

**Mutation checks:** each check is removed in turn (the bucket, the order after the inner call, the scope release, the known-full exemption, the loopback exemption, the key function's `Unmap`, the expiry), and each removal must fail a test.

### On a node

**What our hosts can produce.** In the [load test on #599](https://github.com/crtahlin/wasp/issues/599#issuecomment-6040276978), 8 stock ultra-light clients on bench-1, each asking to connect every 500 ms, produced about 5.3 inbound connection attempts per second in total, about 0.67 per client. The 8 clients held about 1,100 connections together, about 137 each, from one host behind a home network, and that comment notes that larger client counts need a host on a public address. bench-2 is behind NAT with a port forward, so its outbound connections also pass through a NAT that may cap how many connections it holds, as bench-1's home network is limited. The client count on each host must therefore stay at what that host's network has been seen to hold (8 clients, about 1,100 connections, on bench-1), or that host's ceiling is measured first, by raising the client count step by step and watching the clients' connection count stop growing. So the test can count on about 5.3 attempts per second from bench-1, and about 10.7 if bench-2's network holds 8 more clients, both below the default rate of 30. The flood runs therefore lower the target's rate through the setting for the test. They test the mechanism and its cost per refused attempt, not the default values. The defaults are tested for false refusals by runs 1 and 2.

**1. Today's load.** Two sw-1 nodes on this build at the defaults against two on the build of PR #606, over three windows of 8 hours each (rule 7: three runs, reported with the spread). Accepted when the refusal counters stay at zero, the admitted counter shows the margin to the limits, and the connection and kademlia counters match the other nodes within the spread.

**2. A restart under load, 3 times.** On the target node, `light-node-limit` is set to 4, and 8 test clients from bench-1 fill its light slots, as on #599. The node is restarted at the default rate and burst. Recorded: refusals and the admitted rate in the first 10 minutes. Accepted when there are no refusals.

**3. A flood, 3 runs of 45 minutes per condition.** 45 minutes, so that at least one reachability probe (every 30 minutes while public) and the 4.5 minutes of retries after it fall inside each run. Against one sw-1 node (the target):
- **Test settings on the target:** `p2p-inbound-connection-rate: 2`, `p2p-inbound-connection-burst: 10`, and the per-address limits raised (`p2p-connection-rate-per-ip` and `p2p-max-connections-per-ip`), so that the clients behind one public address are not refused by the per-address limit first.
- **General bucket:** 8 ultra-light clients on bench-1, and 8 more on bench-2 if its network is measured to hold them, each asking to connect every 500 ms: about 5.3 or 10.7 attempts per second. Before each run, the target's `/peers` is checked for a full node from the bench network; if one is connected, the bench address is already known, and the run records that it tested the known-full-peer bucket instead.
- **Known-full-peer bucket:** 8 ultra-light clients on sw-1 itself, dialling the target's public address. Their source is sw-1's address, which is known once another sw-1 node is connected to the target as a full peer; this is checked in `bee_libp2p_known_full_addresses` and the target's `/peers` before the run. Whether the clients' connections really arrive from that key depends on routing on sw-1, so the run counts only if `bee_libp2p_inbound_admitted{bucket="known_full"}` and that bucket's refusals both move. The clients take CPU on the same machine, so only the target process's CPU is compared. The other sw-1 nodes share the flooded bucket with these clients and are expected to be refused during this run.
- **Conditions:** the build of PR #606 without this change, and this change with the test settings.
- **Recorded:** the target process's CPU over idle, admissions and refusals by bucket, kademlia's connected full peers, the node's reachability as reported by `/topology`, a worst-case reserve sample per run, and whether a full peer on another bench machine still holds the target in its address book after the run.

**Accepted when:**
- **the CPU per refused attempt falls**, measured from a 45-second CPU profile per run as on #599: the CPU in the connection upgrade (`upgrader.(*upgrader).upgrade`, which contains the security handshake) divided by all inbound attempts in the profile window. Without this change every attempt runs the upgrade; with it only the admitted ones do. Pass: this cost per attempt falls by at least half in all 3 runs. Inconclusive: fewer than 0.5 s of profile samples in the upgrade in either condition, or the runs disagree.
- **Process CPU is recorded but is not a pass criterion.** The expected saving is about 3.3 refused attempts per second x 13 ms = 0.04 cores with bench-1 alone, or 8.7 x 13 ms = 0.11 cores with bench-2 too, the same size as the spread between runs on #599 (0.12 to 0.19 cores).
- admissions stay at the bucket rate;
- kademlia's connected full peers do not fall below their level before the flood, full peers still connect (in the known-full-peer run, excluding the other sw-1 nodes, which share the flooded bucket), and the full peer on the other bench machine still has the target in its address book;
- the target stays public in every run: `bee_libp2p_reachability_public` is 1 before the run starts (it reads 0 until the first reachability event, so a run does not start before then) and stays 1, read every 30 seconds with the other metrics, and `bee_libp2p_reachability_private_switches` does not increase. Only the known-full-peer run exercises this: AutoNAT never asks peers on sw-1's own IP, but that run empties the bucket that dial-backs from known peers elsewhere also use. The general-bucket run passes this criterion without testing it. A switch to private in the known-full-peer run is the residual risk of section 4, a negative result, and calls for an exemption design;
- the reserve sample stays within the spread of its baseline.

**A negative result looks like:**
- the CPU per refused attempt does not fall, because the work moved to accepting TCP connections or to the resource manager;
- kademlia loses full peers, full peers drop the target from their address books, or the target judges itself unreachable;
- the defaults refuse connections at today's load or through a restart;
- the known-full-peer bucket empties under a flood from unknown addresses, so known full peers are refused.

Any of these stops the change at its current design.

## Rollout and rollback

On by default at the values above, except in bootnode mode, by the operator's decision (Configuration). An operator turns the limit off with -1, in the configuration file or as a flag. Rolling back means running a build without the change. Nothing new is stored on disk. The implementation pull request adds the two settings and the new behaviour to `docs/DIFFERENCES.md` (rule 13).

## Upstream portability

`upstream/v2.8.2` has the same arrangement (verified): resource-manager limits from `InfiniteLimits`, a rate limiter with no global limit, per-address limits only, and no connection gater. The change applies there as written, apart from the per-IP settings of #597, which upstream does not have; the wrapper runs after upstream's compiled-in per-address limits in their place.

This is a missing protection, not a defect in existing code, so it gets no `affects-upstream` label (rule 11).

## Decided

- **On by default (rule 8):** decided by the operator; see Configuration.
- **Own reachability during a flood:** left to the implementers by the operator; the decision and its evidence are in section 4.

## To check during implementation

- **AutoTLS (hypothesis, not checked).** The certificate registration for WSS may dial the node back to confirm it is reachable. If so, those dials use the general bucket during a flood and could delay a certificate renewal.

## Review notes not applied

- **Seeding the known set after a restart from the chequebook registry** (suggested in review). Not applied: the registry (`pkg/settlement/swap/chequebook/registry.go`) is in memory only, records no source per entry, and removes entries when a peer disconnects, so it is empty after a restart too.
