# Stream refusal metrics

Issue: #636. This spec covers **metrics only**, the first item of #636, plus the per-peer view needed to size the limits in #638. It changes no behaviour: no limit, no stream and nothing on the wire.

## Terms

- **Stream refusal:** a new libp2p stream that the receiving node's resource manager does not admit. The receiver resets the stream with error code `0x1002` (`network.StreamResourceLimitExceeded`, go-libp2p v0.48.0 `core/network/mux.go:48`); the sender sees the reset as a remote `*network.StreamError` with that code.
- **Refused here:** a stream another peer opened to this node that this node refused.
- **Refused by a peer:** a stream this node opened that the peer refused.

## Problem

On sw-1 on 2026-10-09 (#622, scenario B), 121 peers refused pull-sync streams from our nodes with error code 4098 (`0x1002`), and one node stopped refilling its reserve at 75 % (#636). Two things could not be seen:

1. **Refusals by peers** appear only as a debug line of the puller (`syncWorker interval failed`, `pkg/puller/puller.go:493`), mixed with every other failure. The worker error counter (`bee_puller_worker_errors`) does not say why. Finding them took a debug logger on every node and a log parse.
2. **Refusals here** cannot be seen at all. The resource manager's blocked counters are not exported with the node's setup; only the open stream counts are (`libp2p_rcmgr_streams`, `libp2p_rcmgr_limit`). So an operator cannot tell whether their node is the one refusing its neighbours.

## Hypothesis

With one counter on each side, an operator can see whether their node is refused by its peers, by which protocol, and whether it refuses others. The 24-hour measurement in #636 needs both counters to decide between raising the stream caps (rule 8) and changing how pull-sync holds streams.

## Design

### 1. Refused here

Where wasp can observe it, verified in go-libp2p v0.48.0:

- **Opening the stream scope.** The swarm calls `ResourceManager().OpenStream(peer, network.DirInbound)` for every accepted inbound stream and resets it with `0x1002` when that fails (`p2p/net/swarm/swarm_conn.go:139-141`). The node already wraps the resource manager (`inboundLimiter`, `pkg/p2p/libp2p/inboundlimit.go:187`, installed with `libp2p.ResourceManager(inbound)`, `pkg/p2p/libp2p/libp2p.go:358`), so the wrapper adds `OpenStream(peer, dir)`: it calls the inner manager and, on an error, counts it.
- **Setting the protocol.** After negotiation the host calls `SetProtocol` on the stream (`p2p/host/basic/basic_host.go:350` for inbound; `:481` and `:510` for outbound), which calls the stream scope's `SetProtocol` (`p2p/net/swarm/swarm_stream.go:155`, `p2p/host/resource-manager/rcmgr.go:856`), and resets with `0x1002` on an error. The wrapper's `OpenStream` returns its own scope type around the inner `network.StreamManagementScope`; its `SetProtocol` counts an error. The node sets no protocol or service limits today, so this path cannot fail now; it is counted so that a later per-protocol limit is visible.

Metric:

- `bee_libp2p_streams_refused_total{direction, stage, reason}`, a counter.
  - `direction`: `inbound` or `outbound`. Outbound refusals here happen when this node's own outbound cap (10,000) is reached; they are counted too, since they also stop pull-sync.
  - `stage`: `open` (the scope could not be opened) or `protocol` (`SetProtocol` failed).
  - `reason`: `limit` when the error matches `network.ErrResourceLimitExceeded` with `errors.Is` (the rcmgr errors `ErrStreamOrConnLimitExceeded` and `ErrMemoryLimitExceeded` unwrap to it, `p2p/host/resource-manager/error.go:15`, `:62`), otherwise `other`.

No peer label: label values must stay bounded.

### 2. Refused by a peer

Every outbound Swarm stream goes through `Service.NewStream` (`pkg/p2p/libp2p/libp2p.go`, line 1447 to 1488): `newStreamForPeerID` negotiates the protocol, then `sendHeaders` exchanges headers. A refusal surfaces in either step as a wrapped remote `*network.StreamError`. The sw-1 failures read `send headers: read message: stream reset (remote): code: 0x1002`, so the wrapping keeps the error (`headers.go:26`, `:39` and `libp2p.go:1486` all use `%w`).

In `NewStream`, on an error from either step: if `errors.As` finds a `*network.StreamError` with `Remote` true and `ErrorCode == network.StreamResourceLimitExceeded`, count it.

Metric:

- `bee_libp2p_streams_refused_by_peer_total{protocol}`, a counter. `protocol` is the Swarm protocol name and version (`pullsync/1.4.0` and so on), which is bounded.

Optional, if cheap: a debug line `stream refused by peer` with the peer address and protocol, at the same verbosity as the puller's interval failure, so one peer can be followed without parsing other lines.

Streams that a peer resets later, after the headers, for example during a pull-sync exchange, are not counted: that is not where the sw-1 refusals happened, and other resets would mix in.

### 3. Inbound streams per peer

The two counters say that refusals happen, not who holds the streams. To tell a few peers holding many streams (a fault or misbehaviour on their side) from many peers holding the usual number (load), and to size the per-peer limit proposed in #638 from data (rule 8: measure first), the node also reports how inbound streams are spread over peers.

Every 30 s a goroutine walks `host.Network().Conns()`, and for each connection `conn.GetStreams()` (go-libp2p v0.48.0 `p2p/net/swarm/swarm_conn.go:287`, a copy of the stream set under the connection's lock), counts the streams whose `Stat().Direction` is inbound, and sums them per remote peer (a peer can hold more than one connection). From the per-peer counts it sets gauges, with no peer ID as a label:

- `bee_libp2p_inbound_streams_per_peer_max`: the largest count;
- `bee_libp2p_inbound_streams_per_peer_p99`: the 99th percentile (with fewer than 100 peers, the second-largest count);
- `bee_libp2p_inbound_stream_peers_over{threshold}`: the number of peers above 64, 256 and 1,000 inbound streams;
- the same three for pull-sync streams only (`Protocol()` equal to the pull-sync protocol ID), with the suffix `_pullsync`. This costs one string comparison per stream, so it is included.

When the largest count exceeds the highest threshold (1,000), a debug line `peer holds many inbound streams` names that peer, its count and its pull-sync count.

**Cost.** One pass is proportional to the number of streams: about 150 connections and, on a busy node, up to the cap of about 5,000 inbound streams plus the outbound ones, so at most about 15,000 stream records per pass, each a read of `Stat()` and a map update. Each `GetStreams` call holds one connection's stream lock only while copying its set. That is well under a millisecond of work every 30 s.

**Why 30 s.** Pull-sync streams live for minutes (a live sync request waits for new chunks), so a slower scan still sees the pattern, while a faster one adds lock traffic on every connection for no gain. A short burst between two scans is missed; the refusal counters above catch its effect.

### 4. Documentation

`docs/DIFFERENCES.md` gets the two metrics in the metrics table. Nothing else changes.

## Protocol impact

None. Both counters observe errors the node already receives or produces. No message, stream, protocol ID, limit or reset code changes. `make protocol-freeze` must report the wire surface unchanged.

## Configuration

None. No setting is added; this is observation only.

## Measurement

The change is accepted on tests plus one real-node check; the 24-hour measurement in #636 then uses the counters.

**Tests:**

1. Refused here, `open` stage: a service with an inner resource manager whose `OpenStream` returns `ErrStreamOrConnLimitExceeded` counts one inbound `open`/`limit` refusal per attempt; a non-limit error counts as `other`.
2. Refused here, `protocol` stage: a scope whose `SetProtocol` fails is counted as `protocol`.
3. Refused by a peer: a test peer whose resource manager refuses inbound streams (a stream cap of 0 on that peer) makes `NewStream` to it fail, and the counter for that protocol rises by one; a stream that fails for another reason (unknown protocol, peer gone) does not count.
4. The wrapper still forwards every other `ResourceManager` method unchanged (the existing inbound-limit tests keep passing).
5. Per-peer gauges: two test peers opening 3 and 70 inbound streams to the node give a maximum of 70, one peer over 64, none over 256, and the pull-sync gauges count only streams of that protocol; the debug line appears only above the highest threshold.

Mutation checks: drop the `Remote` check (a local reset would then count), drop the code check (any reset would count), drop the `errors.As` (nothing counts); each must fail a test.

**Real nodes:** deploy to the sw-1 nodes during the next refill, or to a bench node pulling from mainnet peers, and compare the new `refused_by_peer` counter for `pullsync/1.4.0` over 10 minutes with a puller debug log taken at the same time: the counts of `error code: 4098` lines and the counter must agree within the few intervals a log rotation or timing boundary can split. A negative result is a counter that stays at 0 while the log shows refusals (the error is not reaching `NewStream` as a `*network.StreamError`), or counts that differ by more than that margin.

## Rollout and rollback

Ships in the next build; there is nothing to turn on. Rollback is the previous build; the metrics disappear and nothing else changes.

## Upstream portability

Both parts port to upstream Bee: upstream builds its resource manager in the same place (`pkg/p2p/libp2p/libp2p.go:230-237` at `v2.8.2`) but has no wrapper, so the port adds a small wrapper around the resource manager with `OpenStream` only. The `NewStream` check is a few lines in the same function upstream.

## To check during implementation

- Whether the wrapper's own scope type must forward every `network.StreamManagementScope` method (`ProtocolScope`, `SetService`, `ServiceScope`, `PeerScope`, `Done` and the resource methods) and keep the same identity checks the swarm relies on, if any.
- Whether `errors.As` reaches the `*network.StreamError` through the protobuf reader's error in every libp2p version the repository pins (verify against `go.mod`).
