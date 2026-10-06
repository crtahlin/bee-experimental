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

**One slot ledger, in the light-node container, under its one mutex.** It holds:
- a used count;
- a reserved count;
- the same two counts for ultra-light peers (item 4);
- a map from each connected overlay to its ultra-light flag, so a disconnect releases the right count. `pslice` carries no payload, so the map is needed.

It has three operations:
- `TryReserve(ultraLight bool) (token, ok)` reserves a slot when used plus reserved slots are below the limit. For an ultra-light peer, its own limit must also not be reached.
- `Commit(token, overlay)` adds the peer as connected and turns the reservation into a used slot, in that order and under the same lock. A concurrent reservation therefore never sees a falsely low total.
- `Release(token)` drops a reservation that was not committed.

`Disconnected(overlay)` frees a used slot **only if the overlay is in the container's connected set**. It never looks at `peer.FullNode`, which can be wrong at that point (see item 1). The used count is the container's connected count, not a separate counter.

The `lightnodes` interface in `pkg/p2p/libp2p/libp2p.go` and the container tests change to match.

**Where it runs, inbound only** (light peers are never dialled, so there is no outbound case):
1. **At the picker.** The libp2p service wraps the picker it gives to the handshake service (`SetPickyNotifier`). For a light peer, the wrapper refuses when used plus reserved slots are at `light-node-limit`. Otherwise it passes the decision to kademlia's `Pick`. This runs before the signature in the address is checked. It is a cheap first filter and reserves nothing.
2. **After the blocklist check.** For a light peer, `handleIncoming` calls `TryReserve` after the blocklist check, so the blocklist paths need no release. If no slot is free, the connection is closed.
   - A deferred guard calls `Release` unless the token was committed, which covers every return path: the duplicate connection, a `FullClose` failure, a `ConnectIn` failure, and a disconnect during setup.
   - `Commit` replaces today's `lightNodes.Connected` call.
3. **The check after `Connected` stays.** That is the existing check that the peer is still registered, and it removes a peer that disconnected during setup. Under refusal, a missed check would leak a slot.

The eviction branch after the announcement is removed.

**Known imprecision:** a local `Disconnect` removes the registry entry before it updates the container. A reconnect of the same overlay in between can be counted once too few, until it disconnects. The limit can then be exceeded by one per such event.

**What a refused peer sees:**
- **On our side:** the existing rejection path. The stream is reset and the connection closed. No blocklist entry is written on either side, so a client can connect again once a slot is free.
- **On the dialer's side:** the handshake fails with a stream reset.
  - A stock light client counts it as a failed connection attempt.
  - After 4 attempts it removes our address from its address book; after 6 if we are within its neighbourhood depth.
  - Each refusal also uses up one of its retries for a retrieval.

**What a refused peer still costs us:** the noise handshake (libp2p's encrypted connection setup), the identify exchange, reading the Syn, resolving its addresses, and writing our cached signed address. It does not cost the signature check, the address-book write, the `ConnectIn` hooks or an announcement.

**Logging:** a rejection with `handshake.ErrPicker` is logged at debug level, not error level, because it is expected under load. This also lowers the level of kademlia's own refusals of full peers.

**Bootnode mode:** a node in bootnode mode keeps today's behaviour, accepting and evicting. Light clients get their first announcement from bootnodes. `libp2p.Options` gains `BootnodeMode`, set from the same node option that kademlia and hive receive.

### 3. Fewer announcements to light peers (#598)
- **Refused peers get no announcement.** With item 2, a refused light peer never reaches the announcement.
- **One announcement per light peer per 10 minutes.**
  - **Where:** the hook is in the light-peer branch of `handleIncoming`, around `notifier.Announce`, not in `Kad.Announce`, which full peers share. A light peer that received an announcement less than 10 minutes ago gets none when it reconnects.
  - **When it is recorded:** only when `Announce` succeeds, so a failed send does not suppress the next one.
  - **Storage:** the `expirable` LRU from `hashicorp/golang-lru/v2`, already in `go.mod`, with at most 10,000 overlays and a 10-minute time to live.
- **`AnnounceTo` is not gated.** It is what a node sends its light peers when a new full peer connects. It is driven by full-peer changes and keeps a client's list current.

The content of an announcement does not change. The 10-minute window and the 10,000 bound are compiled-in constants. Under rule 8 they become settings only if #599 shows that they matter.

### 4. A separate limit for ultra-light peers (#595)
- **New setting `ultra-light-node-limit`, default 0, meaning "same as `light-node-limit`".** A value above `light-node-limit` is reduced to it, with a warning at start.
- **Who counts as ultra-light.** A light peer whose signed address carries the zero chequebook address.
  - **This is advisory.** A light peer's chequebook address is covered only by its own signature; it is not checked against the chain (`parseCheckAck` checks chequebooks only for full nodes).
  - A client can claim a chequebook to avoid this limit. It then still counts against `light-node-limit`, so it cannot exceed the overall light limit.
  - Checking light chequebooks on chain would cost a chain call per handshake, so this spec does not do it.
- **How it is enforced.** Through `TryReserve(ultraLight)` in item 2. The ledger holds both counts under one mutex, so the check is atomic. The ultra-light flag comes from the address the handshake parsed. The picker cannot apply this limit, because the address is not parsed yet.
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

### 6. An overdraw on a retrieval keeps the connection, in most cases (#596)

**One helper, used by two callers.** The overdraw check that `debit.Apply` makes today is moved into one function, unchanged. Today it is inline, in the part of `Apply` that computes the elapsed time and the refresh rate. The function computes:
- the balance after this debit, as `increaseBalance` computes it, with the surplus balance netted out;
- **without** the shadow reserved balance;
- compared against the disconnect limit plus the inline refresh due: `min(now - refreshReceivedTimestamp, 1)` seconds at the light or full refresh rate. The intent of that `min` is open in #603.

`debit.Apply` calls this function, so its behaviour does not change. The new method `CanDebit(peer, price) bool` calls the same function under the per-peer lock, taken with `TryLock(ctx)` as `PrepareDebit` does.

**Defined results:**
- If the accounting peer is not yet connected, `CanDebit` returns true and today's path applies.
- `CanDebit` reserves nothing, so it is **advisory**. Parallel requests from one light peer, or a push-sync debit in between, can still pass the check together, and the later ones then overdraw in `Apply` and disconnect as today.
- #599 measures how many overdraw disconnects remain. A reserving variant (a checked `PrepareDebit` with a release that adds no ghost balance) would need a deeper change in accounting. It is proposed only if #599 shows the advisory check is not enough.

**Interface:** following the fork's convention (see `pkg/retrieval/providercredit.go`), `CanDebit` is an optional interface asserted on the concrete accounting type, not a new method on `accounting.Interface`, which keeps upstream syncs cheap.

**Where it is called:** for light peers only, in the retrieval handler.
- The price is computed from the requested address (`s.pricer.Price(addr)`), which equals the chunk's address used today.
- The call comes before the local lookup and any forwarding, so the node never pays to forward a chunk it then refuses.
- If the check returns false, the handler returns a plain error. The deferred writer sends the existing error delivery (`pb.Delivery{Err: ...}`), and the connection stays.
- No `PrepareDebit` was made, so no `Cleanup` runs and no ghost overdraw is recorded.
- The cost per light retrieval is one more lock acquisition and two more statestore reads (balance and surplus).

**What clients see:**
- **A stock client** turns the error delivery into `ChunkDeliveryError`. It skips this node for that chunk for 1 minute and tries another peer. It does not blocklist. `Err` is free text, so the wire does not change (rule 6).
- **The ultra-light client used by the applications above** must be checked against the same response before this item is accepted.

**Unchanged:** push sync debits light peers too, and an overdraw on upload still disconnects; that is outside this item.

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
  - Concurrent reservations never exceed the limit.
  - Every return path between `TryReserve` and `Commit` releases the reservation (one test per path, through the deferred guard).
  - A disconnect of an overlay that is not in the container frees nothing.
  - `TestLightPeerLimit` (`connections_test.go`) and the container tests are rewritten.
- **Item 3:**
  - A refused peer gets no announcement.
  - A light peer that reconnects within 10 minutes gets none.
  - A failed announcement is not recorded.
  - `AnnounceTo` still reaches light peers.
  - The LRU map stays at its bound.
- **Item 4:**
  - An ultra-light peer is refused at its limit, while a light peer with a chequebook is accepted.
  - A value above `light-node-limit` is reduced to it.
- **Item 5:** the limit-building function returns the configured values.
- **Item 6:**
  - A light peer that would overdraw gets the error delivery and stays connected, with no ghost overdraw.
  - `CanDebit` and `Apply` agree at the boundary for serial requests, because they use the same function.
  - A peer not yet connected in accounting gets the old path.
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
