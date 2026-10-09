# Stream refusal metrics

Issue: #636. This spec covers **observation only**: the first item of #636, plus the per-peer view needed to size the limits in #638. It changes no behaviour: no limit, no stream and nothing on the wire.

## Terms

- **Stream refusal:** a new libp2p stream that the receiving node's resource manager does not admit. The receiver resets the stream with error code `0x1002` (`network.StreamResourceLimitExceeded`, go-libp2p v0.48.0 `core/network/mux.go:48`); the sender sees the reset as a remote `*network.StreamError` with that code.
- **Refused here:** a stream another peer opened to this node that this node refused.
- **Refused by a peer:** a stream this node opened that the peer refused.

## Problem

On sw-1 on 2026-10-09 (#622, scenario B), 121 peers refused pull-sync streams from our nodes with error code 4098 (`0x1002`), and one node stopped refilling its reserve at 75 % (#636). Two things were hard to see:

1. **Refusals by peers** appear only as a debug line of the puller (`syncWorker interval failed`, `pkg/puller/puller.go:493`), mixed with every other failure. The worker error counter (`bee_puller_worker_errors`) does not say why. Finding them took a debug logger on every node and a log parse.
2. **Who holds the streams.** The node already exports the resource manager's own metrics: `rcmgr.MustRegisterWith(o.Registry)` (`pkg/p2p/libp2p/libp2p.go:259-261`) registers the go-libp2p collectors (`p2p/host/resource-manager/stats.go`), fed by the trace reporter. They include `libp2p_rcmgr_blocked_resources{dir, scope, resource}` (`stats.go:124`), which counts refusals here, and the histograms `libp2p_rcmgr_peer_streams` and `libp2p_rcmgr_previous_peer_streams` (`stats.go:60`, `:69`), whose buckets end at 256. **Correction:** an earlier version of this spec, and #636, said refusals here are not observable; they are, through `libp2p_rcmgr_blocked_resources`, which shows a series only after the first block. What is missing is the size of the largest per-peer holdings above 256 and which peer holds them: on 2026-10-09 the histograms showed one peer holding about 4,500 inbound pull-sync streams on stake-1, and peers holding 700 to 1,100 on seven sw-1 nodes, with no way to tell which peer.

## Hypothesis

With a counter for refusals by peers and an exact per-peer view of inbound streams, an operator can see whether their node is refused, by which protocol, whether a few peers hold most of the streams, and which peers those are. The 24-hour measurement in #636 needs these to choose between raising the stream caps (rule 8), changing how pull-sync holds streams, and the per-peer limits in #638.

## Design

### 1. Refused here: use the existing metric

`libp2p_rcmgr_blocked_resources{dir="inbound", scope="system", resource="streams"}` is the "refused here" signal; no new counter is added. The documentation (`docs/DIFFERENCES.md` and the metrics description) says so, and that the series appears only after the first block.

If refusals at protocol negotiation (`SetProtocol`, `p2p/host/basic/basic_host.go:350`, `:481`, `:510`) need counting later, the place is `rcmgr.WithMetrics` with a `MetricsReporter` (`p2p/host/resource-manager/metrics.go:51`; `BlockStream` at `rcmgr.go:429`, `BlockProtocol` at `metrics.go:114`), not a wrapper around the stream scope. The node sets no protocol or service limits today, so that path cannot refuse anything now, and it is not added.

### 2. Refused by a peer

Every outbound Swarm stream goes through `Service.NewStream` (`pkg/p2p/libp2p/libp2p.go`, line 1447 to 1488): `newStreamForPeerID` negotiates the protocol, then `sendHeaders` exchanges headers. A refusal surfaces in either step as a wrapped remote `*network.StreamError`. The full error text seen on sw-1 was `send headers: read message: stream reset (remote): code: 0x1002: transport error: stream reset by remote, error code: 4098`; every layer wraps with `%w` (`headers.go:26`, `:39`, `libp2p.go:1486`), so `errors.As` reaches the `*network.StreamError` with `Remote` true.

In `NewStream`, on an error from either step: if `errors.As` finds a `*network.StreamError` with `Remote` true and `ErrorCode == network.StreamResourceLimitExceeded`, count it.

Metric:

- `bee_libp2p_streams_refused_by_peer_total{protocol, stream}`, a counter. `protocol` is the Swarm protocol name and version (`pullsync/1.4.0`), `stream` the stream name (`pullsync`, `cursors`); both come from code, so the label set is bounded.

Peers on older libp2p versions reset with code 0 and are not counted; the counter is a lower bound. Streams that a peer resets later, after the headers, are not counted either.

A debug line `stream refused by peer` with the peer overlay, protocol and stream name lets one peer be followed without parsing other lines.

### 3. Inbound streams per peer

The two signals above say that refusals happen, not who holds the streams. Every 30 s a goroutine reads the inner resource manager's own accounting: `ListPeers()` (`rcmgr.ResourceManagerState`, `p2p/host/resource-manager/extapi.go:23`, `:95`) and, for each peer, `ViewPeer(p, ...)` with `Stat().NumStreamsInbound`. This is the exact count the limits apply to, read without touching connections or stream locks. The node keeps a reference to the inner manager (it is wrapped by #617's `inboundLimiter`).

Gauges, with no peer ID as a label:

- `bee_libp2p_inbound_streams_per_peer_max`: the largest per-peer count;
- `bee_libp2p_inbound_stream_peers_over{threshold}`: the number of peers above 64, 256 and 1,000 inbound streams.

**Naming the peers (operator request).** When a peer holds more than 256 inbound streams, the node logs at **info** level, at most once per peer per hour, a line `peer holds many inbound streams` with:

- the libp2p peer ID;
- its overlay (from the peer registry, `pkg/p2p/libp2p/peer.go:197`);
- its Ethereum address, from its signed bzz address in the address book (`addressbook.Get(overlay)`, `pkg/addressbook/addressbook.go:124`; `bzz.Address.EthereumAddress`, `pkg/bzz/address.go:39`, recovered from the signature at line 122); empty if the address book has no record;
- its inbound stream count.

At **debug** level each scan logs the top three peers with the same fields. These are public network identifiers that every peer already exchanges in the handshake; no other data is logged. The once-per-hour limit keeps a persistent case from flooding the log: a map from peer ID to the time of its last line, pruned of peers not seen in the scan.

**Cost.** `ListPeers` returns the peers the manager tracks (about 150 on a busy node), and `ViewPeer` takes that peer scope's lock briefly to copy its counters, so one pass is about 150 short lock acquisitions every 30 s. The address book read happens only for a peer over the threshold, at most once per hour per peer.

**Why 30 s.** Pull-sync streams live for minutes (a live request waits for new chunks), so the pattern is visible at this interval, and a faster one adds lock traffic for no gain. A short burst between two scans is missed; the refusal counters show its effect.

### 4. Documentation

`docs/DIFFERENCES.md` gets the new metrics and the info line, and names `libp2p_rcmgr_blocked_resources` as the signal for refusals here. A note records that wrapping the resource manager in #617 hides `connmgr.GetConnLimiter`, so go-libp2p's startup warning about the connection manager's limits cannot fire; it has no effect on behaviour.

## Protocol impact

None. The counter observes errors the node already receives; the scan reads local accounting. No message, stream, protocol ID, limit or reset code changes. `make protocol-freeze` must report the wire surface unchanged.

## Configuration

None. No setting is added; the thresholds and the interval are compiled in for this observation step.

## Measurement

The change is accepted on tests plus one real-node check; the 24-hour measurement in #636 then uses these signals.

**Tests:**

1. Refused by a peer: a test peer whose resource manager refuses inbound streams (a stream cap of 0) makes `NewStream` to it fail and the counter for that protocol and stream rises by one; a stream that fails for another reason (unknown protocol, peer gone) does not count.
2. Per-peer gauges: two test peers opening 3 and 70 inbound streams give a maximum of 70, one peer over 64 and none over 256.
3. The info line: a peer over the threshold is logged once with peer ID, overlay and Ethereum address, and not again within the hour; a peer with no address-book record is logged with an empty Ethereum address.

Mutation checks: drop the `Remote` check, the code check, or the `errors.As` in item 1; drop the once-per-hour map in item 3; each must fail a test.

**Real nodes:** on stake-1 and the sw-1 nodes, the info line must name the peers the histograms show above 256 (the one at about 4,500 on stake-1 and those at 700 to 1,100 on sw-1), and `bee_libp2p_inbound_streams_per_peer_max` must agree with the highest bucket the histograms put them in. For refusals by peers, compare the counter for `pullsync/1.4.0` and stream `pullsync` over 10 minutes with a puller debug log taken at the same time (`error code: 4098` lines only); the two must agree within the intervals a timing boundary can split. A negative result is a counter at 0 while the log shows refusals, or a peer over the threshold in the histograms that the info line never names.

## Rollout and rollback

Ships in the next build; there is nothing to turn on. Rollback is the previous build; the metrics and the log line disappear and nothing else changes.

## Upstream portability

Both parts port to upstream Bee: the `NewStream` check is a few lines in the same function, and the scan needs only the resource manager upstream already builds (`pkg/p2p/libp2p/libp2p.go:230-237` at `v2.8.2`), which implements `ResourceManagerState`.

## To check during implementation

- That the inner manager returned by `rcmgr.NewResourceManager` satisfies `rcmgr.ResourceManagerState` in the pinned go-libp2p version, and that `ViewPeer` does not create a scope for a peer that has left.
- Whether `errors.As` reaches the `*network.StreamError` for a refusal during protocol negotiation as well as during `sendHeaders` (the sw-1 case was `sendHeaders`).
