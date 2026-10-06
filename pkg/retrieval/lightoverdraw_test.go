// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package retrieval_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/accounting"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	p2pmock "github.com/ethersphere/bee/v2/pkg/p2p/mock"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	pricermock "github.com/ethersphere/bee/v2/pkg/pricer/mock"
	"github.com/ethersphere/bee/v2/pkg/retrieval"
	"github.com/ethersphere/bee/v2/pkg/retrieval/pb"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// With these settings a light peer is disconnected by debit.Apply at a balance
// of 1,200: a light threshold of 1,000 (10,000 over the light factor of 10),
// plus 10 per cent tolerance, plus a refresh due of 100. A full peer is
// disconnected at 12,000.
const (
	lightOverdrawAt = uint64(1200)
	fullOverdrawAt  = uint64(12000)
)

type announceNothing struct{}

func (announceNothing) AnnouncePaymentThreshold(context.Context, swarm.Address, *big.Int) error {
	return nil
}

// blocklistRecorder records every blocklisting accounting asks the p2p layer
// for, which includes a ghost overdraw.
type blocklistRecorder struct {
	mu      sync.Mutex
	reasons []string
}

func (b *blocklistRecorder) blocklist(_ swarm.Address, _ time.Duration, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reasons = append(b.reasons, reason)
	return nil
}

func (b *blocklistRecorder) all() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.reasons...)
}

// countingStorer counts lookups, so a test can tell whether the handler did
// any work for a request before refusing it.
type countingStorer struct {
	*testStorer
	gets atomic.Int64
}

func (c *countingStorer) Lookup() storage.Getter { return countingGetter{c} }

type countingGetter struct{ c *countingStorer }

func (g countingGetter) Get(ctx context.Context, addr swarm.Address) (swarm.Chunk, error) {
	g.c.gets.Add(1)
	return g.c.Get(ctx, addr)
}

type overdrawCase struct {
	// fullNode is how the client connects, both in accounting and on the
	// stream.
	fullNode bool
	// connect is false for a client that accounting has not seen connect yet.
	connect bool
	// balance is the client's debt before the request.
	balance uint64
}

type overdrawResult struct {
	delivery   pb.Delivery
	handlerErr error
	blocklists []string
	balance    *big.Int
	info       accounting.PeerInfo
	lookups    int64
	refusals   float64
}

// serveToClient runs one retrieval of a chunk the holder has, from a client
// in the given state, against a real accounting.
func serveToClient(t *testing.T, tc overdrawCase) overdrawResult {
	t.Helper()

	store := statestore.NewStateStore()
	t.Cleanup(func() { _ = store.Close() })

	blocklists := &blocklistRecorder{}
	acc, err := accounting.NewAccounting(big.NewInt(10000), 10, 10, log.Noop, store, announceNothing{},
		big.NewInt(1000), 10, p2pmock.New(p2pmock.WithBlocklistFunc(blocklists.blocklist)))
	if err != nil {
		t.Fatal(err)
	}

	clientAddr := swarm.RandAddress(t)
	holderAddr := swarm.RandAddress(t)

	if tc.connect {
		acc.Connect(clientAddr, tc.fullNode)
		if tc.balance > 0 {
			action, err := acc.PrepareDebit(context.Background(), clientAddr, tc.balance)
			if err != nil {
				t.Fatal(err)
			}
			if err := action.Apply(); err != nil {
				t.Fatalf("setting up a balance of %d: %v", tc.balance, err)
			}
			action.Cleanup()
		}
	}

	chunk := testingc.FixtureChunk("0033")
	st := &countingStorer{testStorer: &testStorer{ChunkStore: inmemchunkstore.New()}}
	if err := st.Put(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}

	pricer := pricermock.NewMockService(defaultPrice, defaultPrice)
	holder := createRetrieval(t, holderAddr, st, nil, nil, log.Noop, acc, pricer, nil, false)

	opts := []streamtest.Option{
		streamtest.WithBaseAddr(clientAddr),
		streamtest.WithPeerProtocols(map[string]p2p.ProtocolSpec{holderAddr.String(): holder.Protocol()}),
	}
	if !tc.fullNode {
		opts = append(opts, streamtest.WithLightNode())
	}
	recorder := streamtest.New(opts...)

	stream, err := recorder.NewStream(context.Background(), holderAddr, nil, "retrieval", "1.4.0", "retrieval")
	if err != nil {
		t.Fatal(err)
	}
	if err := protobuf.NewWriter(stream).WriteMsgWithContext(context.Background(), &pb.Request{Addr: chunk.Address().Bytes()}); err != nil {
		t.Fatal(err)
	}

	var res overdrawResult
	if err := protobuf.NewReader(stream).ReadMsgWithContext(context.Background(), &res.delivery); err != nil {
		t.Fatal(err)
	}
	_ = stream.FullClose()

	// Records waits for the handler to return.
	records, err := recorder.Records(holderAddr, "retrieval", "1.4.0", "retrieval")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("%d streams recorded, want 1", len(records))
	}
	res.handlerErr = records[0].Err()

	res.blocklists = blocklists.all()
	res.balance, err = acc.Balance(clientAddr)
	if err != nil && !errors.Is(err, accounting.ErrPeerNoBalance) {
		t.Fatal(err)
	}
	infos, err := acc.PeerAccounting()
	if err != nil {
		t.Fatal(err)
	}
	res.info = infos[clientAddr.String()]
	res.lookups = st.gets.Load()
	res.refusals = holder.LightPeerDebitRefusalsForTest(t)
	return res
}

// isDisconnect reports whether the p2p layer would drop the connection for
// this handler error.
func isDisconnect(err error) bool {
	var bpe *p2p.BlockPeerError
	var de *p2p.DisconnectError
	return errors.As(err, &bpe) || errors.As(err, &de)
}

// TestLightPeerOverdrawRefusedKeepsConnection is the change. A light peer
// whose request would put it at its disconnect limit gets the error delivery
// before any work is done, and the handler returns an error the p2p layer does
// not disconnect on. No debit was prepared, so no ghost overdraw is recorded.
func TestLightPeerOverdrawRefusedKeepsConnection(t *testing.T) {
	t.Parallel()

	start := lightOverdrawAt - defaultPrice
	res := serveToClient(t, overdrawCase{connect: true, balance: start})

	if res.delivery.Err == "" {
		t.Fatal("the light peer was served; want an error delivery")
	}
	if len(res.delivery.Data) != 0 {
		t.Fatal("the refusal carried chunk data")
	}
	if !errors.Is(res.handlerErr, retrieval.ErrLightPeerDebtLimit) {
		t.Fatalf("handler returned %v, want the debt-limit refusal", res.handlerErr)
	}
	if isDisconnect(res.handlerErr) {
		t.Fatalf("handler returned %v, which disconnects the peer", res.handlerErr)
	}
	if len(res.blocklists) != 0 {
		t.Fatalf("accounting blocklisted the peer: %v", res.blocklists)
	}
	if res.balance.Uint64() != start {
		t.Fatalf("balance %d after the refusal, want %d unchanged", res.balance, start)
	}
	if res.info.GhostBalance.Sign() != 0 || res.info.ShadowReservedBalance.Sign() != 0 {
		t.Fatalf("ghost balance %d, shadow reserve %d after the refusal, want 0 and 0", res.info.GhostBalance, res.info.ShadowReservedBalance)
	}
	if res.lookups != 0 {
		t.Fatalf("%d lookups for a refused request; the check must come before any work", res.lookups)
	}
	if res.refusals != 1 {
		t.Fatalf("refusal counter %v, want 1", res.refusals)
	}
}

// TestLightPeerWithinLimitServed. One unit below the boundary the light peer
// is served and debited as before.
func TestLightPeerWithinLimitServed(t *testing.T) {
	t.Parallel()

	start := lightOverdrawAt - defaultPrice - 1
	res := serveToClient(t, overdrawCase{connect: true, balance: start})

	if res.delivery.Err != "" {
		t.Fatalf("the light peer was refused below its limit: %s", res.delivery.Err)
	}
	if res.handlerErr != nil {
		t.Fatalf("handler returned %v", res.handlerErr)
	}
	if res.balance.Uint64() != start+defaultPrice {
		t.Fatalf("balance %d, want %d", res.balance, start+defaultPrice)
	}
	if res.refusals != 0 {
		t.Fatalf("refusal counter %v, want 0", res.refusals)
	}
}

// TestLightPeerNotYetConnectedOldPath. Before accounting has seen the peer
// connect, the check allows the request and PrepareDebit refuses it, exactly
// as before the check existed.
func TestLightPeerNotYetConnectedOldPath(t *testing.T) {
	t.Parallel()

	res := serveToClient(t, overdrawCase{connect: false})

	if res.delivery.Err == "" {
		t.Fatal("served a peer accounting has not seen connect")
	}
	if errors.Is(res.handlerErr, retrieval.ErrLightPeerDebtLimit) {
		t.Fatal("refused by the new check; want the old PrepareDebit error")
	}
	if res.lookups != 1 {
		t.Fatalf("%d lookups, want 1: the old path looks the chunk up before preparing the debit", res.lookups)
	}
	if res.refusals != 0 {
		t.Fatalf("refusal counter %v, want 0", res.refusals)
	}
}

// TestFullPeerOverdrawUnchanged. A full peer is not checked. At its
// boundary it is served and then disconnected by Apply, as in Bee.
func TestFullPeerOverdrawUnchanged(t *testing.T) {
	t.Parallel()

	res := serveToClient(t, overdrawCase{fullNode: true, connect: true, balance: fullOverdrawAt - defaultPrice})

	if res.delivery.Err != "" || len(res.delivery.Data) == 0 {
		t.Fatalf("the full peer was not served: %q", res.delivery.Err)
	}
	var bpe *p2p.BlockPeerError
	if !errors.As(res.handlerErr, &bpe) {
		t.Fatalf("handler returned %v, want the BlockPeerError Apply has always returned", res.handlerErr)
	}
	if res.refusals != 0 {
		t.Fatalf("refusal counter %v, want 0", res.refusals)
	}
}
