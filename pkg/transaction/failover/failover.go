// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package failover routes chain calls across an ordered list of RPC endpoints,
// so that losing one does not take the node down.
//
// A single endpoint is what bee has today, and when it fails the node does not
// degrade — it stops. Writes to the chain fail, the postage listener stalls,
// and ten minutes later the stall timeout shuts the node down; if the endpoint
// is still unreachable it will not start again. See issue #109 and
// docs/experiments/rpc-endpoint-failover/spec.md.
//
// Per-call routing is enough because every chain call bee makes is
// request/response: transaction.Backend embeds backend.Geth, which is sixteen
// methods with no subscriptions — FilterLogs polls. There is no streaming state
// to migrate when the active endpoint changes.
package failover

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/transaction"
)

// DefaultMaxBlockLag is how far behind the highest block seen a candidate
// endpoint may be before it is skipped.
//
// A secondary being a block or two behind is normal and harmless. A large
// backwards step is not: the postage listener derives batch state from chain
// events, and a sudden regression is the shape of a reorg. Bounding it is what
// keeps a failover from looking like one.
const DefaultMaxBlockLag = 8

// Endpoint describes one configured RPC endpoint.
type Endpoint struct {
	Name string
	// Backend is the live connection, or nil for an endpoint that did not
	// answer when the node started. A nil backend is not the same as a missing
	// endpoint: it keeps its place in the priority order and is reconnected by
	// Recover if it comes back.
	Backend transaction.Backend
	// Dial opens a fresh connection, and is what lets an endpoint that was
	// down at startup be adopted later. It must verify that the endpoint is on
	// the expected chain and refuse otherwise, because nothing above it will:
	// startup checks the chain once, and an endpoint adopted an hour later is
	// never seen by that check.
	//
	// Nil means the endpoint cannot be reconnected, so once it is lost it stays
	// lost. That is the old behaviour, and it is what a caller gets by saying
	// nothing.
	Dial func(context.Context) (transaction.Backend, error)
}

// endpoint is an Endpoint with its connection made replaceable.
type endpoint struct {
	name string
	dial func(context.Context) (transaction.Backend, error)
	// live is nil until the endpoint has answered at least once. Read on every
	// chain call and written by Recover, so it is atomic rather than guarded.
	live atomic.Pointer[conn]
}

// conn boxes the interface so it can be stored in an atomic.Pointer.
type conn struct{ transaction.Backend }

type Backend struct {
	endpoints []*endpoint
	logger    log.Logger
	maxLag    uint64

	active  atomic.Int32  // index into endpoints
	highest atomic.Uint64 // highest block number observed anywhere
}

var _ transaction.Backend = (*Backend)(nil)

// New returns a Backend routing to the first endpoint, failing over in order.
//
// Order is priority, not round-robin: operators generally have a preferred
// provider and a worse standby, and spreading load across both permanently
// would double the exposure to the worse one.
func New(logger log.Logger, maxLag uint64, endpoints ...Endpoint) (*Backend, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("failover: no endpoints")
	}
	if maxLag == 0 {
		maxLag = DefaultMaxBlockLag
	}

	b := &Backend{logger: logger, maxLag: maxLag}
	first := -1
	for i, e := range endpoints {
		ep := &endpoint{name: e.Name, dial: e.Dial}
		if e.Backend != nil {
			ep.live.Store(&conn{e.Backend})
			if first < 0 {
				first = i
			}
		}
		b.endpoints = append(b.endpoints, ep)
	}
	if first < 0 {
		return nil, errors.New("failover: no endpoint is connected")
	}

	// Start on the best endpoint that answered, not necessarily the first
	// configured one. Starting on a dead endpoint would make the first chain
	// call fail before failing over, which is a worse first impression than
	// starting one place down the list and recovering upwards.
	b.active.Store(int32(first))
	return b, nil
}

func (b *Backend) current() (int, *endpoint, transaction.Backend) {
	i := int(b.active.Load())
	ep := b.endpoints[i]
	return i, ep, ep.live.Load().Backend
}

// advance moves to the next connected endpoint and reports whether one was
// found.
//
// Endpoints with no connection are skipped rather than tried: they are known to
// be unreachable, and only Recover reconnects them. Trying one here would spend
// a chain call's latency discovering what is already known, on the path that is
// already handling a failure.
func (b *Backend) advance(from int, cause error) bool {
	for next := from + 1; next < len(b.endpoints); next++ {
		if b.endpoints[next].live.Load() == nil {
			continue
		}
		// Only the goroutine that observed this failure advances; a concurrent
		// caller that already moved on wins and this one follows it.
		if b.active.CompareAndSwap(int32(from), int32(next)) {
			b.logger.Warning("blockchain rpc endpoint failed, moving to the next",
				"from", b.endpoints[from].name, "to", b.endpoints[next].name, "cause", cause)
		}
		return true
	}
	return false
}

// call runs op against the active endpoint, moving on when an endpoint fails to
// answer. An answer, including an error answer, is returned as-is.
func call[T any](b *Backend, op func(transaction.Backend) (T, error)) (T, error) {
	var zero T
	for attempts := 0; attempts < len(b.endpoints); attempts++ {
		i, ep, live := b.current()
		v, err := op(live)
		if err == nil {
			return v, nil
		}
		if !isTransportFailure(err) {
			return v, err
		}
		if !b.advance(i, err) {
			return zero, fmt.Errorf("failover: all %d endpoints failed, last was %s: %w",
				len(b.endpoints), ep.name, err)
		}
	}
	return zero, errors.New("failover: exhausted endpoints")
}

// --- transaction.Backend -----------------------------------------------------

// BlockNumber also maintains the high-water mark used to bound how far behind a
// failover target may be.
func (b *Backend) BlockNumber(ctx context.Context) (uint64, error) {
	n, err := call(b, func(be transaction.Backend) (uint64, error) {
		return be.BlockNumber(ctx)
	})
	if err != nil {
		return 0, err
	}
	for {
		prev := b.highest.Load()
		if n <= prev {
			// A step backwards within the bound is ordinary endpoint skew. Past
			// it, say so: the listener may be about to see what looks like a
			// reorg, and an operator reading logs should find this here.
			if prev-n > b.maxLag {
				_, ep, _ := b.current()
				b.logger.Warning("blockchain rpc endpoint is behind the highest block seen",
					"endpoint", ep.name, "reported", n, "highest", prev, "lag", prev-n)
			}
			break
		}
		if b.highest.CompareAndSwap(prev, n) {
			break
		}
	}
	return n, nil
}

func (b *Backend) BalanceAt(ctx context.Context, a common.Address, n *big.Int) (*big.Int, error) {
	return call(b, func(be transaction.Backend) (*big.Int, error) { return be.BalanceAt(ctx, a, n) })
}

func (b *Backend) CallContract(ctx context.Context, m ethereum.CallMsg, n *big.Int) ([]byte, error) {
	return call(b, func(be transaction.Backend) ([]byte, error) { return be.CallContract(ctx, m, n) })
}

func (b *Backend) ChainID(ctx context.Context) (*big.Int, error) {
	return call(b, func(be transaction.Backend) (*big.Int, error) { return be.ChainID(ctx) })
}

func (b *Backend) CodeAt(ctx context.Context, c common.Address, n *big.Int) ([]byte, error) {
	return call(b, func(be transaction.Backend) ([]byte, error) { return be.CodeAt(ctx, c, n) })
}

func (b *Backend) EstimateGas(ctx context.Context, m ethereum.CallMsg) (uint64, error) {
	return call(b, func(be transaction.Backend) (uint64, error) { return be.EstimateGas(ctx, m) })
}

func (b *Backend) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	return call(b, func(be transaction.Backend) ([]types.Log, error) { return be.FilterLogs(ctx, q) })
}

func (b *Backend) HeaderByNumber(ctx context.Context, n *big.Int) (*types.Header, error) {
	return call(b, func(be transaction.Backend) (*types.Header, error) { return be.HeaderByNumber(ctx, n) })
}

func (b *Backend) NonceAt(ctx context.Context, a common.Address, n *big.Int) (uint64, error) {
	return call(b, func(be transaction.Backend) (uint64, error) { return be.NonceAt(ctx, a, n) })
}

func (b *Backend) PendingNonceAt(ctx context.Context, a common.Address) (uint64, error) {
	return call(b, func(be transaction.Backend) (uint64, error) { return be.PendingNonceAt(ctx, a) })
}

func (b *Backend) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	return call(b, func(be transaction.Backend) (*big.Int, error) { return be.SuggestGasTipCap(ctx) })
}

func (b *Backend) TransactionByHash(ctx context.Context, h common.Hash) (*types.Transaction, bool, error) {
	type res struct {
		tx      *types.Transaction
		pending bool
	}
	r, err := call(b, func(be transaction.Backend) (res, error) {
		tx, pending, err := be.TransactionByHash(ctx, h)
		return res{tx, pending}, err
	})
	return r.tx, r.pending, err
}

func (b *Backend) TransactionReceipt(ctx context.Context, h common.Hash) (*types.Receipt, error) {
	return call(b, func(be transaction.Backend) (*types.Receipt, error) { return be.TransactionReceipt(ctx, h) })
}

func (b *Backend) SuggestedFeeAndTip(ctx context.Context, gasPrice *big.Int, boost int) (*big.Int, *big.Int, error) {
	type res struct{ fee, tip *big.Int }
	r, err := call(b, func(be transaction.Backend) (res, error) {
		f, t, err := be.SuggestedFeeAndTip(ctx, gasPrice, boost)
		return res{f, t}, err
	})
	return r.fee, r.tip, err
}

// SendTransaction deliberately does NOT fail over.
//
// If the endpoint accepted the transaction and the response was lost, resending
// elsewhere risks a duplicate; and two endpoints can hold different mempools, so
// the nonce this was built against may not be the nonce the next one expects.
// bee already persists pending transactions through transaction.Service and
// watches them to confirmation, so the retry belongs there, with the state to do
// it safely. Reads fail over; writes do not.
func (b *Backend) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	_, ep, live := b.current()
	if err := live.SendTransaction(ctx, tx); err != nil {
		if isTransportFailure(err) {
			b.logger.Warning("send transaction failed at the transport level; not retrying "+
				"elsewhere, because a lost response may mean it was accepted",
				"endpoint", ep.name, "tx", tx.Hash())
		}
		return err
	}
	return nil
}

// Close closes every connected endpoint, not only the active one. Endpoints
// that were never reachable have nothing to close.
func (b *Backend) Close() {
	for _, ep := range b.endpoints {
		if c := ep.live.Load(); c != nil {
			c.Close()
		}
	}
}

// Active reports which endpoint is currently serving, for tests and logging.
// AverageBlockTime returns the block time observed by the endpoint in use, or 0
// when it observes none. See transaction.BlockTimer.
func (b *Backend) AverageBlockTime() time.Duration {
	_, _, be := b.current()
	if timer, ok := be.(transaction.BlockTimer); ok {
		return timer.AverageBlockTime()
	}
	return 0
}

func (b *Backend) Active() string {
	_, ep, _ := b.current()
	return ep.name
}

// Recover periodically re-checks endpoints ahead of the active one and moves
// back when a better one is healthy again. It blocks until ctx is done.
//
// Bias to the front of the list is deliberate: the order is the operator's
// preference, so staying on a standby after the primary recovers is a slow
// failure of its own. The backoff exists because the opposite mistake — hopping
// back to an endpoint that is up but unwell, failing, and hopping back again —
// costs a failed request every cycle.
func (b *Backend) Recover(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	// Per-endpoint: how many consecutive probes have failed. Used to space out
	// retries of an endpoint that keeps disappointing.
	misses := make([]int, len(b.endpoints))

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		active := int(b.active.Load())
		if active == 0 {
			continue // already on the preferred endpoint
		}
		for i := 0; i < active; i++ {
			if misses[i] > 0 {
				misses[i]--
				continue
			}
			if b.probe(ctx, i) {
				if b.active.CompareAndSwap(int32(active), int32(i)) {
					b.logger.Info("blockchain rpc endpoint recovered, moving back",
						"from", b.endpoints[active].name, "to", b.endpoints[i].name)
				}
				break
			}
			// Skip this endpoint for a few rounds, growing to a cap, so a
			// half-broken endpoint is not probed every interval forever.
			if misses[i] = misses[i]*2 + 1; misses[i] > 16 {
				misses[i] = 16
			}
		}
	}
}

// probe reports whether an endpoint is both reachable and close enough to the
// highest block seen to be worth switching to.
func (b *Backend) probe(ctx context.Context, i int) bool {
	ep := b.endpoints[i]
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	live := b.connect(ctx, ep)
	if live == nil {
		return false
	}

	n, err := live.BlockNumber(ctx)
	if err != nil {
		return false
	}
	if highest := b.highest.Load(); highest > n && highest-n > b.maxLag {
		b.logger.Debug("candidate rpc endpoint is too far behind to switch to",
			"endpoint", ep.name, "reported", n, "highest", highest, "max_lag", b.maxLag)
		return false
	}
	return true
}

// connect returns the endpoint's connection, dialling it first if it has never
// had one. It returns nil when the endpoint cannot be reached or has no way to
// be dialled.
//
// A connection made here is kept even if the endpoint then turns out to be too
// far behind to switch to. Being behind is a reason not to use an endpoint now,
// not a reason to throw away a working connection and pay to make it again on
// the next probe.
func (b *Backend) connect(ctx context.Context, ep *endpoint) transaction.Backend {
	if c := ep.live.Load(); c != nil {
		return c.Backend
	}
	if ep.dial == nil {
		return nil
	}

	backend, err := ep.dial(ctx)
	if err != nil {
		b.logger.Debug("could not reconnect to a blockchain rpc endpoint",
			"endpoint", ep.name, "error", err)
		return nil
	}

	// Another probe may have connected the same endpoint in the meantime.
	// Whoever loses closes their connection rather than leaking it.
	if !ep.live.CompareAndSwap(nil, &conn{backend}) {
		backend.Close()
		return ep.live.Load().Backend
	}
	b.logger.Info("reconnected to a blockchain rpc endpoint", "endpoint", ep.name)
	return backend
}
