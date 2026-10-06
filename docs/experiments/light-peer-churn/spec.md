# Light-peer churn under many ultra-light clients

Epic: [#600](https://github.com/crtahlin/wasp/issues/600). Issues: #594, #593, #598, #595, #597, #596. Measurement: #599.
Type: optimization.

## Terms

- **Light peer:** a peer that declares `FullNode = false` in the handshake.
- **Ultra-light peer:** a light peer whose signed bzz address carries the zero chequebook address. It has no chequebook and cannot pay for bandwidth.
- **Churn:** connections opening and closing.

## Problem

Some applications run many ultra-light clients. Each keeps about 200 connections to full nodes, re-dials every 500 ms with no backoff, and overdraws its free allowance often.

With 1,000 such clients, about 200,000 connections are spread over 1,500 to 3,000 reachable full nodes: 67 to 133 per node, against the default `light-node-limit` of 100.

Holding a connection is cheap. Opening and closing one is not. Today, on `main` and on `upstream/v2.8.2`:

1. **Every light-peer disconnect does full-peer work.** Both disconnect paths in `pkg/p2p/libp2p/libp2p.go` call `notifier.Disconnected(peer)` for light peers too. `Kad.Disconnected` then:
   - recalculates the depth;
   - wakes the manage loop;
   - fires the topology signal, so the puller recalculates every sync peer.

   A light peer is never in `connectedPeers`, so none of this is needed. (#594)
2. **A full light limit evicts a random existing light peer instead of refusing the new one.** The evicted client re-dials and evicts another, so the loop never settles. The new peer has already passed the handshake, the `ConnectIn` hooks (pricing stream, pseudosettle, accounting writes) and a full hive announcement. (#593, #598)
3. **Ultra-light and paying light peers share one limit.** (#595)
4. **The per-IP limits are compiled in:** 200 connections per IPv4 address or IPv6 /56, and 10 new connections per second with a burst of 40. (#597)
5. **An overdraw disconnects.** When a light peer's debt passes its disconnect limit, `debit.Apply` returns a `BlockPeerError`, and the peer is blocklisted for about 4 s and disconnected. A client streaming faster than its allowance refreshes is dropped every time. (#596)

## Hypothesis

Under a client load above the light limit, items 1 to 3 are where a full node spends most of its churn work:
- If light disconnects skip kademlia work, and a full limit refuses new peers before the handshake work, the load settles: clients hold their slots, refused clients go elsewhere, and the node's CPU, handshake rate and puller recalculations fall.
- Keeping a connection on overdraw (item 5) cuts the remaining churn.

## Design

### 1. A light-peer disconnect skips kademlia work (#594)
In `Kad.Disconnected`, when `!peer.FullNode`, return after the light-node bookkeeping. Skip:
- `connectedPeers.Remove`;
- `waitNext`;
- `recalcDepth`;
- `notifyManageLoop`;
- `notifyPeerSig`.

A test checks that a light disconnect sends no topology signal, and that a full-node disconnect still does.

### 2. Refuse instead of evicting (#593)
- The libp2p service wraps the picker it gives to the handshake service. The wrapper refuses a light peer when `lightNodes.Count() >= lightNodeLimit`, and otherwise passes the decision to kademlia's `Pick`.
- The handshake service calls the picker before the signature recovery, so a refused peer costs only the noise handshake and the identify exchange.
- The refusal uses the existing `ErrPicker` path (`picker rejection`). The connection is closed. No blocklist entry is written, because a client should be able to connect again once a slot is free.
- The eviction branch after `notifier.Announce` is removed.
- To keep the change small, the "evict only an idle light peer" variant mentioned in #593 is not part of this spec. It can follow if #599 shows that refusing leaves slots held by idle clients.

### 3. The limit before the announcement, and a smaller announcement to light peers (#598)
- The count check of item 2 happens before the handshake completes, so a refused peer never receives an announcement.
- A light peer receives only neighbourhood peers in `Kad.Announce`, without the `BroadcastBinSize` peers per bin. Neighbourhood peers are the ones a client needs to reach the data near it; it finds the rest through its own discovery, as it does today after the first announcement.

### 4. A separate limit for ultra-light peers (#595)
- New setting `ultra-light-node-limit`, default 0, meaning "same as `light-node-limit`". That default makes no change for any existing node.
- After the handshake has verified the bzz address, and before the `ConnectIn` hooks, the libp2p service refuses an ultra-light peer when the number of connected ultra-light peers has reached the limit. An ultra-light peer is one with `FullNode = false` and a zero `ChequebookAddress`.
- The address is verified at that point; an unverified field could be faked.
- The light-node container keeps a count of ultra-light peers.
- **Raising it:** clients that cannot pay take more of the node's free bandwidth and more of its handshakes.
- **Lowering it:** fewer slots for such clients, and paying light peers keep theirs.

### 5. Settings for the per-IP limits (#597)
New settings, with today's values as defaults:
- `p2p-max-connections-per-ip`, default 200: the connection count per IPv4 address and per IPv6 /56;
- `p2p-connection-rate-per-ip`, default 10: new connections per second;
- `p2p-connection-burst-per-ip`, default 40: the burst.

They feed the resource-manager limits in `pkg/p2p/libp2p/libp2p.go`. Localhost stays exempt.

- **Raising them:** one address can use more of the node's handshakes and slots.
- **Lowering them:** users behind a shared carrier NAT can be shut out.

Rule 8 asks for the measurement first, and #599 provides it. The defaults change nothing.

### 6. An overdraw keeps the connection (#596)
- **For light peers only,** the retrieval handler checks, before serving, whether the debit for the chunk would pass the peer's disconnect limit.
- **If it would,** it refuses the request with the error response the protocol already uses for a failed retrieval, and keeps the connection. The client tries another peer, or waits for its allowance to refresh.
- **The post-serve path stays for full peers.** For light peers, the `BlockPeerError` path remains only as a safety net if the check and the debit disagree.
- **Wire compatibility:** no new message or field; the response is one stock clients already receive when a retrieval fails. This keeps the wire surface frozen (rule 6), and `make protocol-freeze` must stay unchanged.
- **Before it merges, the implementation must confirm two things:**
  - the stock client and the ultra-light client in use treat that response as "try elsewhere";
  - they do not treat it as a fault that blocklists us.

## Protocol impact

None. No message, field, stream or protocol version changes:
- items 1, 3 and 6 change local decisions and the content of an existing message (fewer peers in an announcement);
- item 2 uses the existing picker rejection;
- items 4 and 5 are local limits.

## Measurement

**Unit tests, one per item:**
1. A light disconnect sends no topology signal.
2. A full light limit refuses the next light peer at the picker, and no existing light peer is disconnected.
3. A refused peer receives no announcement, and a light peer's announcement contains only neighbourhood peers.
4. An ultra-light peer is refused at its limit, while a light peer with a chequebook is still accepted.
5. The per-IP settings reach the resource manager.
6. A light peer that would overdraw gets the error response and stays connected; a full peer behaves as today.

**Mutation checks:** each change is reverted in turn, and each reversal must fail its test.

**On a node:** the comparison in #599:
- the 24-hour baseline;
- three runs of a client load above the limit against one node, on `main`, on a build per item, and on a build with all items.

An item is accepted when its build shows less churn work at no loss for the clients: connections held, and retrievals served per second. Item 6 also needs the client-side check described above.

## Rollout and rollback

Items 1, 2, 3 and 6 are always on. Items 4 and 5 change nothing until they are set. Rolling back means running a build without the change. Nothing new is stored.

## Upstream portability

All six apply to `upstream/v2.8.2` as written. Item 1 is wasted work in unmodified upstream code. It gets `affects-upstream` once #599 has measured it, not before.
