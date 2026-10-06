# Light-peer churn under many ultra-light clients

Epic: [#600](https://github.com/crtahlin/wasp/issues/600). Issues: #594, #593, #598, #595, #597, #596. Measurement: #599.
Type: optimization.

## Terms

- **Light peer:** a peer that declares `FullNode = false` in the handshake.
- **Ultra-light peer:** a light peer whose signed bzz address carries the zero chequebook address. It declares that it cannot pay for bandwidth.
- **Churn:** connections opening and closing.
- **Picker:** the function the handshake calls to accept or refuse a peer, before the signature in its address is checked. Today it is kademlia's `Pick`.
- **Kademlia's peer list (`connectedPeers`):** the full peers kademlia counts for its depth.
  - The **depth** decides which peers are neighbours.
  - The **manage loop** is kademlia's background task that dials and prunes peers.
  - The **topology signal** tells the puller (which pulls chunks from neighbours) that peers changed. The puller then recalculates its sync peers.
- **`ConnectIn` hooks:** the work each protocol does on a new inbound peer. For example, pricing opens a stream, and pseudosettle writes accounting records.
- **Announcement:** the list of peer addresses a node sends a new peer through the hive protocol.
- **Allowance:** the free bandwidth a light peer may use, refreshed each second. Its **disconnect limit** is the debt at which the node disconnects it.
- Rules 6 and 8 are those of `AGENTS.md`: the frozen wire surface, and measuring before adding a setting.

## Problem

Some applications run many ultra-light clients. Each keeps about 200 connections to full nodes, re-dials every 500 ms with no backoff, and overdraws its allowance often.

With 1,000 such clients, about 200,000 connections are spread over 1,500 to 3,000 reachable full nodes: 67 to 133 per node, against the default `light-node-limit` of 100.

Holding a connection is cheap. Opening and closing one is not. Today, on `main` and on `upstream/v2.8.2`:

1. **Every light-peer disconnect makes kademlia do full-peer work.** `Kad.Disconnected` does not check whether the peer was in its peer list. It recalculates the depth, wakes the manage loop and sends the topology signal, so the puller recalculates every sync peer. It also adds a retry entry and a log-out record for an address it never counted. (#594)
2. **When a node is at its light limit, it evicts a random existing light peer instead of refusing the new one.** The evicted client re-dials and evicts another, so the loop never settles. By the time the eviction happens, the new peer has already passed:
   - the handshake;
   - the `ConnectIn` hooks;
   - a full announcement. (#593, #598)
3. **Ultra-light and paying light peers share one limit.** (#595)
4. **The per-IP limits are compiled in:** 200 connections per IPv4 address or IPv6 /56, and 10 new connections per second with a burst of 40. (#597)
5. **An overdraw on a retrieval disconnects.** When a light peer's debt passes its disconnect limit, `debit.Apply` returns a `BlockPeerError`, and the node blocklists the peer for about 4 s and disconnects it. (#596)

## Hypothesis

Under a client load above the light limit, items 1 and 2 are where a full node spends most of its churn work.

If kademlia ignores disconnects of peers it never counted, and a full light limit refuses new peers early instead of evicting, the load settles:
- clients hold their slots;
- refused clients try elsewhere;
- the node's CPU, handshake rate and puller recalculations fall.

Items 3 to 6 reduce the remaining work.

## Design

### 1. Kademlia ignores disconnects of peers it never counted (#594)
At the start of `Kad.Disconnected`, return when `!k.connectedPeers.Exists(peer.Address)`.

The check is on kademlia's own peer list, not on `peer.FullNode`, because that flag is not reliable at this point. When the remote side closes the connection, the peer registry deletes the full-node flag before it notifies kademlia, so a full peer arrives marked as light (`pkg/p2p/libp2p/peer.go`, `Service.disconnected`).

Light peers are never in the list: `Kad.Connected` is only called for full peers, and dialing a light peer fails. So a light disconnect now skips the depth recalculation, the manage loop, the topology signal, the retry entry and the log-out record.

The metric `TotalInboundDisconnections` then counts only peers kademlia had counted. The change is noted in `docs/DIFFERENCES.md`.

### 2. Refuse at the light limit instead of evicting (#593)

**A slot is reserved before the new peer gets anything.** The libp2p service keeps a count of light slots in use plus reserved, under a small mutex.

1. **At the picker.** The libp2p service wraps the picker it gives to the handshake service (`SetPickyNotifier`). For a light peer, the wrapper refuses when used plus reserved slots are at `light-node-limit`. Otherwise it passes the decision to kademlia's `Pick`. This check runs before the signature in the peer's address is checked.
2. **After the handshake.** Straight after the handshake returns, before the blocklist check and `addIfNotExists`, the service **reserves** a slot for a light peer. If none is free, it closes the connection.
3. **Release.** The reservation becomes a used slot when the light-node container records the peer as connected. It is released on any failure before that. A used slot is released on disconnect.

The picker check is a cheap first filter. The reservation is the one that enforces the limit. Concurrent handshakes cannot exceed the limit, because a slot is reserved under the mutex.

The eviction branch after the announcement is removed.

**What a refused peer sees:**
- **On our side:** the existing rejection path. The stream is reset and the connection closed. No blocklist entry is written on either side, so a client can connect again once a slot is free.
- **On the dialer's side:** the handshake fails with a stream reset.
  - A stock light client counts it as a failed connection attempt.
  - After 4 such attempts (`maxConnAttempts`) it removes our address from its address book.
  - So "refused clients try elsewhere" means stock light clients forget a node that refused them 4 times.

**What a refused peer still costs us:** the noise handshake (libp2p's encrypted connection setup), the identify exchange, reading the Syn, resolving its addresses, and writing our cached signed address. It does not cost the signature check, the address-book write, the `ConnectIn` hooks or an announcement.

**Logging:** a picker refusal is logged at debug level, not error level, because it is expected under load.

**Bootnode mode:** a node in bootnode mode keeps today's behaviour, accepting and evicting. Light clients get their first announcement from bootnodes, and a bootnode is not a node this work targets.

### 3. Fewer announcements to light peers (#598)
- **Refused peers get no announcement.** With item 2, a refused light peer never reaches the announcement.
- **One announcement per light peer per 10 minutes.** An accepted light peer receives the announcement as today. But a light peer that received one less than 10 minutes ago gets none when it reconnects.
  - The record of which peers got one is an LRU map of at most 10,000 overlay addresses, with the time each received its announcement. LRU means the oldest entry is dropped when the map is full.
  - A client that reconnects often still has the peer list from its last announcement.

The content of an announcement does not change.

### 4. A separate limit for ultra-light peers (#595)
- **New setting `ultra-light-node-limit`, default 0, meaning "same as `light-node-limit`".** A value above `light-node-limit` is reduced to it, with a warning at start.
- **Who counts as ultra-light.** A light peer whose signed address carries the zero chequebook address.
  - **This is advisory.** A light peer's chequebook address is covered only by its own signature; it is not checked against the chain (`parseCheckAck` checks chequebooks only for full nodes).
  - A client can claim a chequebook to avoid this limit. It then still counts against `light-node-limit`, so it cannot exceed the overall light limit.
  - Checking light chequebooks on chain would cost a chain call per handshake, so this spec does not do it.
- **How it is enforced.** Through the same reservation as item 2, after the handshake, where the peer's signed address is known. The light-node container records whether each peer is ultra-light, and its `Connected` method gains that parameter. The picker cannot apply this limit, because the address is not parsed yet.
- **Raising it:** clients that cannot pay take more of the node's free bandwidth and more of its handshakes.
- **Lowering it:** fewer slots for such clients, and paying light peers keep theirs.

### 5. Settings for the per-IP limits (#597)
New settings, with today's values as defaults:
- `p2p-max-connections-per-ip`, default 200: connections per IPv4 address and per IPv6 /56;
- `p2p-connection-rate-per-ip`, default 10: new connections per second;
- `p2p-connection-burst-per-ip`, default 40: the burst.

The resource-manager limits are built in a separate function, so a test can check them.

The options flow from `cmd/bee/cmd` to `node.Options`, then to `libp2p.Options`. Localhost stays exempt: from the rate limit explicitly, and from the connection count by the go-libp2p default, as long as the change keeps using the per-subnet limits.

- **Raising them:** one address can use more of the node's handshakes and slots.
- **Lowering them:** users behind a shared carrier NAT can be shut out.

### 6. An overdraw on a retrieval keeps the connection (#596)

**New accounting method `CanDebit(peer, price) bool`.** It is added to `accounting.Interface` and its mock.

- **What it checks:** whether a debit of `price` would reach the peer's disconnect limit. It runs under the per-peer lock and includes the shadow reserved balance (amounts reserved for requests still being served).
- **The formula:** the same `refreshDue` as `debit.Apply`, copied exactly, so the two never disagree. That includes today's `min(elapsed, 1)`, whose intent is open in #603.

**Where it is called:** for light peers only, in the retrieval handler, after the price is known and **before** the local lookup and any forwarding.
- If it returns false, the handler answers with the existing error delivery (`pb.Delivery{Err: ...}`) and keeps the connection.
- No `PrepareDebit` was made, so no `Cleanup` runs, and no ghost overdraw is recorded.

**What clients see:**
- **A stock client** turns the error delivery into `ChunkDeliveryError`. It skips this node for that chunk for 1 minute and tries another peer. It does not blocklist. `Err` is free text, so the wire does not change (rule 6).
- **The ultra-light client used by the applications above** must be checked against the same response before this item is accepted.

**Unchanged:**
- `debit.Apply` keeps its `BlockPeerError` as a safety net.
- Push sync debits light peers too, and an overdraw on upload still disconnects; that is outside this item.

### 7. Metrics for the measurement
New metrics:
- **Light-peer refusals by reason:** `light_limit_picker`, `light_limit_reserve`, `ultra_light_limit`.
- **Light connects and disconnects.**
- **Announcements skipped** because a light peer received one recently.
- **`CanDebit` refusals.**
- **Puller `onChange` runs.**
- **Kademlia depth recalculations.**

`KickedOutPeersCount` stays, and should read 0 outside bootnode mode.

## Protocol impact

None. No message, field, stream or protocol version changes:
- item 1 changes local bookkeeping;
- item 2 uses the existing handshake rejection;
- item 3 skips an existing message;
- items 4 and 5 are local limits;
- item 6 uses the existing error delivery.

`.github/protocol-freeze.lock` stays unchanged.

## Configuration and rule 8

**New settings:** `ultra-light-node-limit` and the three per-IP settings. Each is built now, at a default that changes nothing.

Each merges only after #599 shows that it matters. Until then each stays on its own branch, so builds can be compared.

## Measurement

### Unit tests
- **Item 1:**
  - A disconnect of a peer kademlia never counted sends no topology signal.
  - A full peer whose disconnect arrives with `FullNode = false` is still removed.
  - The existing tests that pass a zero `FullNode` keep passing.
- **Item 2:**
  - At the limit, the next light peer is refused at the picker, and no existing peer is disconnected.
  - Concurrent handshakes never exceed the limit.
  - A reservation is released when the handshake fails after it.
  - `TestLightPeerLimit` (`connections_test.go`) is rewritten for refusal.
- **Item 3:**
  - A refused peer gets no announcement.
  - A light peer that reconnects within 10 minutes gets none.
  - The LRU map stays at its bound.
- **Item 4:**
  - An ultra-light peer is refused at its limit, while a light peer with a chequebook is accepted.
  - A value above `light-node-limit` is reduced to it.
- **Item 5:** the limit-building function returns the configured values.
- **Item 6:**
  - A light peer that would overdraw gets the error delivery and stays connected, with no ghost overdraw.
  - `CanDebit` and `Apply` agree at the boundary.
  - Concurrent requests cannot pass `CanDebit` together.
  - Full peers are unaffected.

**Mutation checks:** each change is reverted in turn, and each reversal must fail its test.

### On a node
The comparison in #599:
- the 24-hour baseline;
- three runs of a client load above the limit against one node, on `main`, on a build per item, and on a build with all items.

An item is accepted when its build shows less churn work (CPU, handshakes per second, puller and depth recalculations) at no loss for the clients: connections held, and retrievals served per second. Item 6 also needs the check against the ultra-light client.

## Rollout and rollback

Items 1, 2, 3 and 6 are always on. Items 4 and 5 change nothing until they are set. Rolling back means running a build without the change. Nothing new is stored on disk.

## Upstream portability

All items apply to `upstream/v2.8.2` as written. Item 1 is wasted work in unmodified upstream code. It gets `affects-upstream` once #599 has measured it.

## Found while specifying, filed separately
- The light-node container's list of disconnected peers is never pruned: #602.
- `refreshDue` uses `min(elapsed, 1)`, which caps the elapsed time at 1 second. It may be intentional or may be meant as `max`; the same line is upstream: #603.
