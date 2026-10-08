// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pushsync_test

import (
	"context"
	"sync"
	"testing"

	accountingmock "github.com/ethersphere/bee/v2/pkg/accounting/mock"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/postage"
	pricermock "github.com/ethersphere/bee/v2/pkg/pricer/mock"
	"github.com/ethersphere/bee/v2/pkg/pushsync"
	"github.com/ethersphere/bee/v2/pkg/soc"
	stabilmock "github.com/ethersphere/bee/v2/pkg/stabilization/mock"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology"
	"github.com/ethersphere/bee/v2/pkg/topology/mock"
)

// pushHolder records the chunks it is asked to hold.
type pushHolder struct {
	mu   sync.Mutex
	held []swarm.Address
}

func (h *pushHolder) HoldUnvalidated(_ context.Context, ch swarm.Chunk, cause error) (bool, error) {
	if !postage.HoldsUnvalidated(cause) {
		return false, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.held = append(h.held, ch.Address())
	return true, nil
}

func (h *pushHolder) HeldFull() bool { return false }

func (h *pushHolder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.held)
}

// TestHeldChunkNoReceipt checks that a storer whose batch store does not
// know the chunk's batch holds the chunk, stores nothing in the reserve,
// sends no receipt and charges nothing (#583).
func TestHeldChunkNoReceipt(t *testing.T) {
	t.Parallel()

	chunk := testingc.FixtureChunk("7000")
	pivotNode := swarm.MustParseHexAddress("0000000000000000000000000000000000000000000000000000000000000000")
	storerNode := swarm.MustParseHexAddress("6000000000000000000000000000000000000000000000000000000000000000")

	unknownBatch := func(swarm.Chunk) (swarm.Chunk, error) { return nil, postage.ErrNotFound }

	storer := &testStorer{chunksPut: make(map[string]swarm.Chunk), chunksReported: make(map[string]int)}
	storerAccounting := accountingmock.NewAccounting()
	psStorer := pushsync.New(storerNode, 1, blockHash.Bytes(), streamtest.NewRecorderDisconnecter(streamtest.New()), storer,
		func() (uint8, error) { return 0, nil }, mock.NewTopologyDriver(mock.WithClosestPeerErr(topology.ErrWantSelf)), true,
		func(swarm.Chunk) {}, func(*soc.SOC) {}, unknownBatch, log.Noop, storerAccounting,
		pricermock.NewMockService(defaultPrices.price, defaultPrices.peerPrice), defaultSigner(chunk), nil, stabilmock.NewSubscriber(true), 0, 0)
	t.Cleanup(func() { psStorer.Close() })
	holder := &pushHolder{}
	psStorer.SetChunkHolder(holder)

	recorder := streamtest.New(streamtest.WithProtocols(psStorer.Protocol()), streamtest.WithBaseAddr(pivotNode))
	psPivot, _, _ := createPushSyncNode(t, pivotNode, defaultPrices, recorder, nil, defaultSigner(chunk), mock.WithClosestPeer(storerNode))

	receipt, err := psPivot.PushChunkToClosest(context.Background(), chunk)
	if err == nil {
		t.Fatalf("got receipt %v for a held chunk, want an error", receipt)
	}
	if holder.count() != 1 {
		t.Fatalf("held %d chunks, want 1", holder.count())
	}
	if storer.hasChunk(t, chunk.Address()) {
		t.Fatal("held chunk was put into the reserve")
	}
	balance, err := storerAccounting.Balance(pivotNode)
	if err != nil {
		t.Fatal(err)
	}
	if balance.Int64() != 0 {
		t.Fatalf("storer charged %d for a held chunk, want 0", balance)
	}
}
