// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package retrieval provides the retrieval protocol
// implementation. The protocol is used to retrieve
// chunks over the network using forwarding-kademlia
// routing.
package retrieval

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/accounting"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/pricer"
	"github.com/ethersphere/bee/v2/pkg/ratelimit"
	pb "github.com/ethersphere/bee/v2/pkg/retrieval/pb"
	"github.com/ethersphere/bee/v2/pkg/safe"
	"github.com/ethersphere/bee/v2/pkg/skippeers"
	"github.com/ethersphere/bee/v2/pkg/soc"
	storage "github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"resenje.org/singleflight"
)

// loggerName is the tree path name of the logger for this package.
const loggerName = "retrieval"

const (
	protocolName    = "retrieval"
	protocolVersion = "1.4.0"
	streamName      = "retrieval"
)

var _ Interface = (*Service)(nil)

type Interface interface {
	// RetrieveChunk retrieves a chunk from the network using the retrieval protocol.
	// it takes as parameters a context, a chunk address to retrieve (content-addressed or single-owner) and
	// a source peer address, for the case that we are requesting the chunk for another peer. In case the request
	// originates at the current node (i.e. no forwarding involved), the caller should use swarm.ZeroAddress
	// as the value for sourcePeerAddress.
	RetrieveChunk(ctx context.Context, address, sourcePeerAddr swarm.Address) (chunk swarm.Chunk, err error)
}

type retrievalResult struct {
	chunk     swarm.Chunk
	peer      swarm.Address
	err       error
	preferred bool
}

type Storer interface {
	Cache() storage.Putter
	Lookup() storage.Getter
}

type Service struct {
	addr          swarm.Address
	radiusFunc    func() (uint8, error)
	streamer      p2p.Streamer
	peerSuggester topology.ClosestPeerer
	storer        Storer
	singleflight  singleflight.Group[string, swarm.Chunk]
	logger        log.Logger
	accounting    accounting.Interface
	metrics       metrics
	pricer        pricer.Interface
	tracer        *tracing.Tracer
	caching       bool
	errSkip       *skippeers.List

	// providers turns on the content-providers behavior: a local-only request
	// from a peer is answered from the local store only, and origin retrievals
	// try the peers in a preferred set first.
	providers       atomic.Bool
	peerMissLimiter *ratelimit.Limiter
	nodeMissLimiter *ratelimit.Limiter

	// providerCredit grants a peer a larger payment threshold because it is
	// asking this node as a provider (#327). Nil unless wired in, and then the
	// handler grants nothing.
	providerCredit providerCreditor

	// providerWait is how long a preferred peer is retained on a chunk after
	// being refused credit for it. A field rather than a package variable so a
	// parallel test that shortens it cannot race another that reads it, which
	// is what happened with the #369 timeout.
	providerWait time.Duration
}

func New(
	addr swarm.Address,
	radiusFunc func() (uint8, error),
	storer Storer,
	streamer p2p.Streamer,
	chunkPeerer topology.ClosestPeerer,
	logger log.Logger,
	accounting accounting.Interface,
	pricer pricer.Interface,
	tracer *tracing.Tracer,
	forwarderCaching bool,
) *Service {
	return &Service{
		addr:          addr,
		radiusFunc:    radiusFunc,
		streamer:      streamer,
		peerSuggester: chunkPeerer,
		storer:        storer,
		logger:        logger.WithName(loggerName).Register(),
		accounting:    accounting,
		pricer:        pricer,
		metrics:       newMetrics(),
		tracer:        tracer,
		caching:       forwarderCaching,
		errSkip:       skippeers.NewList(time.Minute),
		// local-only misses: 100 per second per peer, 1000 per second in total
		peerMissLimiter: ratelimit.New(10*time.Millisecond, 100),
		nodeMissLimiter: ratelimit.New(time.Millisecond, 1000),
		providerWait:    providerCreditWait,
	}
}

// SetProvidersEnabled turns the content-providers behavior on or off. With it
// on, the handler answers a request carrying LocalOnlyHeader from the local
// store only, and origin retrievals try the peers of a preferred set first.
func (s *Service) SetProvidersEnabled(enabled bool) {
	s.providers.Store(enabled)
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
		},
	}
}

const (
	RetrieveChunkTimeout = time.Second * 30
	preemptiveInterval   = time.Second
	overDraftRefresh     = time.Millisecond * 600
	skiplistDur          = time.Minute
	originSuffix         = "_origin"
	maxOriginErrors      = 32
	// providerCreditWait is how long a preferred peer stays a candidate for a
	// chunk after it is first refused credit for it. See issue #392.
	//
	// A count of attempts cannot express the thing that matters, which is how
	// long credit takes to arrive: settlement runs at about one refreshment a
	// second and one cheque every 1.2 s, so eight tries can be spent while the
	// money is still in flight, and the peer was then dropped from a chunk no
	// other peer held.
	//
	// Generous on purpose. Retries here are paced by overDraftRefresh, 600 ms,
	// so five seconds would buy about eight of them, which is the count this
	// replaces and would change nothing. Thirty seconds covers several
	// settlement cycles. It is close to free: an overdraft falls through to
	// ordinary selection immediately and retention never blocks it, so a long
	// wait costs nothing whenever another peer can serve the chunk, and the
	// request context still bounds the whole retrieval.
	providerCreditWait   = 30 * time.Second
	maxMultiplexForwards = 2
)

func (s *Service) RetrieveChunk(ctx context.Context, chunkAddr, sourcePeerAddr swarm.Address) (swarm.Chunk, error) {
	loggerV1 := s.logger

	s.metrics.RequestCounter.Inc()

	origin := sourcePeerAddr.IsZero()

	if chunkAddr.IsZero() || chunkAddr.IsEmpty() || !chunkAddr.IsValidLength() {
		return nil, fmt.Errorf("invalid address queried")
	}

	// preferred peers, such as content providers, are tried first, on origin
	// requests only
	var (
		preferredSet   *PreferredSet
		preferredPeers []swarm.Address
	)
	if origin && s.providers.Load() {
		if preferredSet = PreferredPeers(ctx); preferredSet != nil {
			preferredPeers = preferredSet.Peers()
		}
	}

	flightRoute := chunkAddr.String()
	if origin {
		flightRoute = chunkAddr.String() + originSuffix
		if fp := fingerprint(preferredPeers); fp != "" {
			flightRoute += preferredSuffix + fp
		}
	}

	totalRetrieveAttempts := 0
	requestStartTime := time.Now()
	defer func() {
		s.metrics.RequestDurationTime.Observe(time.Since(requestStartTime).Seconds())
		s.metrics.RequestAttempts.Observe(float64(totalRetrieveAttempts))
	}()

	spanCtx := context.WithoutCancel(ctx)

	v, _, err := s.singleflight.Do(ctx, flightRoute, func(ctx context.Context) (swarm.Chunk, error) {
		skip := skippeers.NewList(0)
		defer skip.Close()

		var preemptiveTicker <-chan time.Time

		// preferred candidates for this chunk, tried one after the other before
		// normal peer selection, and the timer that starts the next attempt
		// when a preferred attempt is slow
		candidates := s.preferredCandidates(preferredPeers, chunkAddr, s.errSkip.ChunkPeers(chunkAddr))
		// Once per flight whose candidate list is not empty, which is the only
		// observable that answers #299: every existing counter is either
		// credit-capped or inflated by the #324 readmit path. Here rather than
		// in the loop, because a flight is what the question is about.
		if len(candidates) > 0 {
			s.metrics.PreferredCandidatesSelected.Inc()
		}
		// wasp #435: every preferred peer this flight has offered for this
		// chunk, so a rebuild below can never offer one twice.
		//
		// This is what makes the rebuild terminate, and it is deliberately
		// not left to the skip lists. A peer that is DISPATCHED is recorded
		// by skip.Forever before it goes out (preferred.go), so the skip list
		// would exclude it; but a peer refused credit is recorded only for
		// overDraftRefresh, so once that expires it would become offerable
		// again, and a peer dropped past its providerWait window would then
		// be offered and dropped on a cycle. With this set the rebuild is
		// monotone: at most one entry per peer in the preferred set ever
		// enters the loop.
		offered := make(map[string]struct{}, len(candidates))
		for _, p := range candidates {
			offered[p.ByteString()] = struct{}{}
		}
		var (
			preferredTimer  *time.Timer
			preferredTimerC <-chan time.Time
		)
		defer func() {
			if preferredTimer != nil {
				preferredTimer.Stop()
			}
		}()

		if !sourcePeerAddr.IsZero() {
			skip.Forever(chunkAddr, sourcePeerAddr)
		}

		quit := make(chan struct{})
		defer close(quit)

		forwards := maxMultiplexForwards

		// if we are the origin node, allow many preemptive retries to speed up the retrieval of the chunk.
		errorsLeft := 1
		if origin {
			ticker := time.NewTicker(preemptiveInterval)
			defer ticker.Stop()
			preemptiveTicker = ticker.C
			errorsLeft = maxOriginErrors
		}

		resultC := make(chan retrievalResult, 1)
		retryC := make(chan struct{}, forwards+1)

		retry := func() {
			select {
			case retryC <- struct{}{}:
			case <-ctx.Done():
			default:
			}
		}

		retry()

		inflight := 0
		// outstanding requests to preferred peers, see wasp #438
		preferredInflight := 0

		// wasp #392: when each preferred peer was first refused credit for this
		// chunk. Retention is bounded by elapsed time from that moment, not by
		// a count of attempts, because what decides whether the peer is worth
		// keeping is how long its credit takes to arrive.
		overdraftSince := make(map[string]time.Time, len(candidates))

		for errorsLeft > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-preemptiveTicker:
				retry()
			case <-preferredTimerC:
				preferredTimerC = nil
				retry()
			case <-retryC:

				totalRetrieveAttempts++
				s.metrics.PeerRequestCounter.Inc()

				// wasp #435: the list above is built once, before this loop,
				// and keeps only peers already connected in KADEMLIA. A
				// hinted download starts its first chunk's flight before the
				// dial the same request began has landed, measured at 0.38 s
				// against a 2.34 s request, so that list is empty and stays
				// empty for the chunk's whole retry budget. For sole-source
				// content the first chunk is the root, so the download
				// returns nothing while the provider sits connected and
				// holding it.
				//
				// Rebuilding from preferredSet.Peers() rather than from the
				// pre-flight snapshot is also what lets a provider that
				// Discover appended part way through a download be used by
				// chunks already in flight.
				//
				// The set is read only when it has peers. withProviders
				// attaches one to most origin downloads, usually empty, so a
				// presence check alone would run this on every iteration of
				// every unhinted download; an empty set costs a mutex and a
				// zero-length slice, a non-empty one costs a Kademlia lookup
				// per peer.
				//
				// Most, not all: withProviders returns early when providers
				// are disabled, and /pins, /soc and the access-control paths
				// reach here as origin with no set at all. So the nil test is
				// load bearing and not decoration.
				// The rebuild does NOT raise the per-chunk cap. Without this
				// test it would: maxPreferredAttempts bounds the FIRST build
				// only, so rebuilding until the set runs out turns a bound of
				// two into a bound of however many providers are known.
				// Measured at six attempts for a six-peer set against two
				// without the rebuild, and every one of those is an outbound
				// local-only request that the provider pays for, in a handler
				// invocation, a miss-limiter slot and a debit attempt.
				//
				// Keeping the cap still fixes the defect, because the case it
				// is about starts with an EMPTY list: nothing has been offered
				// when the provider connects, so the whole budget is available
				// to it.
				if preferredSet != nil && len(candidates) == 0 && len(offered) < maxPreferredAttempts {
					if peers := preferredSet.Peers(); len(peers) > 0 {
						fresh := s.preferredCandidates(peers, chunkAddr,
							append(skip.ChunkPeers(chunkAddr), s.errSkip.ChunkPeers(chunkAddr)...))
						for _, p := range fresh {
							if len(offered) >= maxPreferredAttempts {
								break
							}
							if _, seen := offered[p.ByteString()]; seen {
								continue
							}
							offered[p.ByteString()] = struct{}{}
							candidates = append(candidates, p)
						}
						if len(candidates) > 0 {
							s.metrics.PreferredRebuilds.Inc()
						}
					}
				}

				if len(candidates) > 0 {
					peer := candidates[0]
					err := s.retrievePreferred(ctx, spanCtx, quit, chunkAddr, peer, skip, resultC)
					switch {
					case err == nil:
						// started, fall past this block to the bookkeeping below
					case errors.Is(err, accounting.ErrOverdraft) && time.Since(firstOverdraft(overdraftSince, peer, time.Now())) < s.providerWait:
						// Keep the peer for a later attempt, but do NOT wait for
						// its credit here. Ordinary selection is tried straight
						// away instead: where other peers hold the chunk that
						// costs nothing, and where none do it exhausts and the
						// loop returns to this peer once its overDraftRefresh
						// skip has expired.
						//
						// Waiting here instead cost about 3x on content the
						// network also holds, because the wait was paid on every
						// chunk while another peer could have served it at once.
						// Consuming the candidate, which is what the code did
						// before #324, made the refusal permanent and stopped
						// downloads of content only this peer held.
						//
						// wasp #392: bounded by elapsed time rather than by a
						// count of tries. Settlement arrives at about one
						// refreshment a second and one cheque every 1.2 s, so a
						// count of 8 could be spent in well under a second while
						// the money was still in flight, and the peer was then
						// dropped from a chunk nobody else held.
						//
						// The peer keeps its place among the candidates but
						// goes behind the others, so a second provider that
						// CAN be paid is asked on the next round instead of
						// waiting out this peer's window. Holding the head of
						// the list would make the worst case two windows
						// rather than one, and would leave a funded provider
						// holding the chunk unasked for the whole of the
						// first. With one candidate this rotation changes
						// nothing, which is the usual case for a hint.
						candidates = append(candidates[1:], peer)
						s.metrics.PreferredReadmits.Inc()
					default:
						// Drop the peer for this chunk. Two cases reach here:
						// an error that will not clear by itself, such as a
						// peer that is not connected, and an overdraft whose
						// retention window has passed, which after #392 is the
						// ordinary way a provider is let go.
						candidates = candidates[1:]
						retry()
						continue
					}
					if err == nil {
						candidates = candidates[1:]
						inflight++
						preferredInflight++
						if preferredTimer == nil {
							preferredTimer = time.NewTimer(preferredWait)
						} else {
							preferredTimer.Reset(preferredWait)
						}
						preferredTimerC = preferredTimer.C
						continue
					}
					// overdrafted and kept: no continue, so ordinary selection
					// below runs now rather than after a wait
				}

				fullSkip := append(skip.ChunkPeers(chunkAddr), s.errSkip.ChunkPeers(chunkAddr)...)
				peer, err := s.closestPeer(chunkAddr, fullSkip, origin)

				if errors.Is(err, topology.ErrNotFound) {
					if skip.PruneExpiresAfter(chunkAddr, overDraftRefresh) == 0 { // no overdraft peers, we have depleted ALL peers
						if inflight == 0 {
							loggerV1.Debug("no peers left", "chunk_address", chunkAddr, "errors_left", errorsLeft, "isOrigin", origin, "own_proximity", swarm.Proximity(s.addr.Bytes(), chunkAddr.Bytes()), "error", err)
							return nil, err
						}
						continue // there is still an inflight request, wait for it's result
					}

					loggerV1.Debug("sleeping to refresh overdraft balance", "chunk_address", chunkAddr)

					select {
					case <-time.After(overDraftRefresh):
						retry()
						continue
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}

				if err != nil {
					if inflight == 0 {
						loggerV1.Debug("peer selection", "chunk_address", chunkAddr, "error", err)
						return nil, err
					}
					continue
				}

				// since we can reach into the neighborhood of the chunk
				// act as the multiplexer and push the chunk in parallel to multiple peers.
				// neighbor peers will also have multiple retries, which means almost the entire neighborhood
				// will be scanned for the chunk, starting from the closest to the furthest peer in the neighborhood.
				if radius, err := s.radiusFunc(); err == nil && swarm.Proximity(peer.Bytes(), chunkAddr.Bytes()) >= radius {
					for ; forwards > 0; forwards-- {
						retry()
						errorsLeft++
					}
				}

				action, err := s.prepareCredit(ctx, peer, chunkAddr, origin)
				if err != nil {
					skip.Add(chunkAddr, peer, overDraftRefresh)
					retry()
					continue
				}
				skip.Forever(chunkAddr, peer)

				inflight++

				safe.Go(loggerV1, "retrieval-retrieve-chunk", func() {
					span, _, ctx := s.tracer.FollowSpanFromContext(spanCtx, "retrieve-chunk", s.logger, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
						attribute.String("swarm.chunk.address", chunkAddr.String()),
					))
					defer span.End()
					s.retrieveChunk(ctx, quit, chunkAddr, peer, resultC, action, span, nil, false)
				})

			case res := <-resultC:

				inflight--

				if res.preferred {
					preferredInflight--
					s.preferredResult(preferredSet, res)
					// the answer is in, so the timer armed for this attempt
					// must not start another one
					if preferredTimer != nil {
						preferredTimer.Stop()
					}
					preferredTimerC = nil
				}

				if res.err == nil {
					loggerV1.Debug("retrieved chunk", "chunk_address", chunkAddr, "peer_address", res.peer, "peer_proximity", swarm.Proximity(res.peer.Bytes(), chunkAddr.Bytes()))
					return res.chunk, nil
				}

				loggerV1.Debug("failed to get chunk", "chunk_address", chunkAddr, "peer_address", res.peer,
					"peer_proximity", swarm.Proximity(res.peer.Bytes(), chunkAddr.Bytes()), "error", res.err)

				if res.preferred {
					// a miss at a preferred peer is not a peer error: it does
					// not use up an allowed error, and it does not skip the
					// peer for other chunks
					retry()
					continue
				}

				// wasp #392: the error budget exists to end a hopeless search
				// among ordinary peers. While a verified provider is still a
				// candidate for this chunk the search is not hopeless, only
				// unfunded, and spending the budget here ends the download
				// before the provider's credit arrives.
				//
				// wasp #438: the same applies while a provider is still
				// answering. A preferred peer is consumed from the list the
				// moment it is dispatched, so without the second test the
				// budget resumes under an outstanding request, and reaching
				// zero returns storage.ErrNotFound and closes quit, throwing
				// away the delivery on its way back.
				//
				// Both tests are about preferred peers, so a forwarder and a
				// download carrying no hint spend the budget exactly as Bee
				// does. The cost of holding it is that ordinary selection keeps
				// walking: a chunk can be asked of the whole connected set
				// rather than of 32 peers, and the flight then ends through
				// peer depletion with topology.ErrNotFound rather than
				// storage.ErrNotFound. See docs/DIFFERENCES.md.
				if len(candidates) == 0 && preferredInflight == 0 {
					errorsLeft--
				}
				s.errSkip.Add(chunkAddr, res.peer, skiplistDur)
				retry()
			}
		}

		return nil, storage.ErrNotFound
	})
	if err != nil {
		s.metrics.RequestFailureCounter.Inc()
		s.logger.Debug("retrieval failed", "chunk_address", chunkAddr, "error", err)
		return nil, err
	}

	s.metrics.RequestSuccessCounter.Inc()

	return v, nil
}

func (s *Service) retrieveChunk(ctx context.Context, quit chan struct{}, chunkAddr, peer swarm.Address, result chan retrievalResult, action accounting.Action, span trace.Span, headers p2p.Headers, preferred bool) {
	var (
		startTime = time.Now()
		err       error
		chunk     swarm.Chunk
	)

	defer func() {
		action.Cleanup()
		if err != nil {
			tracing.RecordError(span, err)
			s.metrics.TotalErrors.Inc()
		} else {
			span.SetAttributes(attribute.Bool("swarm.operation.success", true))
		}
		select {
		case result <- retrievalResult{err: err, chunk: chunk, peer: peer, preferred: preferred}:
		case <-quit:
			return
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, RetrieveChunkTimeout)
	defer cancel()

	stream, err := s.streamer.NewStream(ctx, peer, headers, protocolName, protocolVersion, streamName)
	if err != nil {
		err = fmt.Errorf("new stream: %w", err)
		return
	}

	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	w, r := protobuf.NewWriterAndReader(stream)
	err = w.WriteMsgWithContext(ctx, &pb.Request{Addr: chunkAddr.Bytes()})
	if err != nil {
		err = fmt.Errorf("write request: %w peer %s", err, peer.String())
		return
	}

	var d pb.Delivery
	if err = r.ReadMsgWithContext(ctx, &d); err != nil {
		err = fmt.Errorf("read delivery: %w peer %s", err, peer.String())
		return
	}
	if d.Err != "" {
		err = p2p.NewChunkDeliveryError(d.Err)
		return
	}

	s.metrics.ChunkRetrieveTime.Observe(time.Since(startTime).Seconds())
	s.metrics.TotalRetrieved.Inc()

	chunk = swarm.NewChunk(chunkAddr, d.Data)
	if !cac.Valid(chunk) {
		if !soc.Valid(chunk) {
			s.metrics.InvalidChunkRetrieved.Inc()
			err = swarm.ErrInvalidChunk
			return
		}
	}

	err = action.Apply()
}

// firstOverdraft records when this peer was first refused credit for the chunk
// of the flight that owns the map, and returns that moment. It never returns
// the zero value, so the caller's elapsed comparison is meaningful on the very
// first refusal. See issue #392.
func firstOverdraft(since map[string]time.Time, peer swarm.Address, now time.Time) time.Time {
	if t, ok := since[peer.ByteString()]; ok {
		return t
	}
	since[peer.ByteString()] = now
	return now
}

func (s *Service) prepareCredit(ctx context.Context, peer, chunk swarm.Address, origin bool) (accounting.Action, error) {
	price := s.pricer.PeerPrice(peer, chunk)
	s.metrics.ChunkPrice.Observe(float64(price))

	creditAction, err := s.accounting.PrepareCredit(ctx, peer, price, origin)
	if err != nil {
		return nil, err
	}

	return creditAction, nil
}

// closestPeer returns address of the peer that is closest to the chunk with
// provided address addr. This function will ignore peers with addresses
// provided in skipPeers and if allowUpstream is true, peers that are further of
// the chunk than this node is, could also be returned, allowing the upstream
// retrieve request.
func (s *Service) closestPeer(addr swarm.Address, skipPeers []swarm.Address, allowUpstream bool) (swarm.Address, error) {
	var (
		closest swarm.Address
		err     error
	)

	closest, err = s.peerSuggester.ClosestPeer(addr, false, topology.Select{Reachable: true, Healthy: true}, skipPeers...)
	if errors.Is(err, topology.ErrNotFound) {
		closest, err = s.peerSuggester.ClosestPeer(addr, false, topology.Select{Reachable: true}, skipPeers...)
		if errors.Is(err, topology.ErrNotFound) {
			closest, err = s.peerSuggester.ClosestPeer(addr, false, topology.Select{}, skipPeers...)
		}
	}

	if err != nil {
		return swarm.Address{}, err
	}

	if allowUpstream {
		return closest, nil
	}

	closer, err := closest.Closer(addr, s.addr)
	if err != nil {
		return swarm.Address{}, fmt.Errorf("distance compare addr %s closest %s base address %s: %w", addr.String(), closest.String(), s.addr.String(), err)
	}
	if !closer {
		return swarm.Address{}, topology.ErrNotFound
	}

	return closest, nil
}

func (s *Service) handler(p2pctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	ctx, cancel := context.WithTimeout(p2pctx, RetrieveChunkTimeout)
	defer cancel()

	w, r := protobuf.NewWriterAndReader(stream)
	var attemptedWrite bool

	defer func() {
		if err != nil {
			if !attemptedWrite {
				if writeErr := w.WriteMsgWithContext(ctx, &pb.Delivery{Err: err.Error()}); writeErr == nil {
					_ = stream.FullClose()
					return
				}
			}
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	var req pb.Request
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		return fmt.Errorf("read request: %w peer %s", err, p.Address.String())
	}

	addr := swarm.NewAddress(req.Addr)

	if addr.IsZero() || addr.IsEmpty() || !addr.IsValidLength() {
		return fmt.Errorf("invalid address queried by peer %s", p.Address.String())
	}

	var forwarded bool

	span, _, ctx := s.tracer.StartSpanFromContext(ctx, "handle-retrieve-chunk", s.logger, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
		attribute.String("swarm.chunk.address", addr.String()),
	))
	defer func() {
		if err != nil {
			tracing.RecordError(span, err)
		} else {
			span.SetAttributes(attribute.Bool("swarm.operation.success", true))
		}
		span.SetAttributes(attribute.Bool("swarm.chunk.forwarded", forwarded))
		span.End()
	}()

	// wasp #596: a light peer whose debit would overdraw it is refused here,
	// before the lookup and any forwarding, with an error delivery rather than
	// a disconnect after serving. Full peers are not checked.
	if err := s.checkLightPeerDebit(ctx, p, addr); err != nil {
		return err
	}

	chunk, err := s.storer.Lookup().Get(ctx, addr)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// a local-only request is answered from the local store only
			if _, ok := stream.Headers()[LocalOnlyHeader]; ok && s.providers.Load() {
				return s.localOnlyMiss(p.Address)
			}
			// forward the request
			chunk, err = s.RetrieveChunk(ctx, addr, p.Address)
			if err != nil {
				return fmt.Errorf("retrieve chunk: %w", err)
			}
			forwarded = true
		} else {
			return fmt.Errorf("get from store: %w", err)
		}
	}

	// The peer asked as a provider and this node had the chunk, so this is the
	// path where a provider actually serves and credit is consumed. Until now
	// the local-only header was read only on a miss, because nothing on the hit
	// path needed it. Granting is cheap and does nothing after the first call
	// for a connection.
	_, localOnly := stream.Headers()[LocalOnlyHeader]
	s.grantProviderCredit(peerInfo{address: p.Address, fullNode: p.FullNode}, localOnly)

	chunkPrice := s.pricer.Price(chunk.Address())
	debit, err := s.accounting.PrepareDebit(ctx, p.Address, chunkPrice)
	if err != nil {
		return fmt.Errorf("prepare debit to peer %s before writeback: %w", p.Address.String(), err)
	}
	defer debit.Cleanup()

	attemptedWrite = true

	if err := w.WriteMsgWithContext(ctx, &pb.Delivery{
		Data: chunk.Data(),
	}); err != nil {
		return fmt.Errorf("write delivery: %w peer %s", err, p.Address.String())
	}

	// debit price from p's balance
	if err := debit.Apply(); err != nil {
		return fmt.Errorf("apply debit: %w", err)
	}

	// cache the request last, so that putting to the localstore does not slow down the request flow
	if s.caching && forwarded {
		if err := s.storer.Cache().Put(p2pctx, chunk); err != nil {
			s.logger.Debug("retrieve cache put", "error", err)
		}
	}

	return nil
}

func (s *Service) Close() error {
	return s.errSkip.Close()
}
