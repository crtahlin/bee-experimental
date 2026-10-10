// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package retrieval_test

import (
	"context"
	"sync"
	"testing"
	"time"

	accountingmock "github.com/ethersphere/bee/v2/pkg/accounting/mock"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	pricermock "github.com/ethersphere/bee/v2/pkg/pricer/mock"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	testingc "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	topologymock "github.com/ethersphere/bee/v2/pkg/topology/mock"
)

// TestRetrievalLeaveWhileJoin reaches the race in resenje.org/singleflight
// v0.4.0 (#647) through retrieval, which shares one flight per chunk between
// its callers: one caller keeps the flight open, a second joins and leaves
// (its context is cancelled), and a third joins as it leaves. All three use
// the same kind of key (no source peer). It only fails under -race, on v0.4.0;
// probabilistic, so it repeats.
func TestRetrievalLeaveWhileJoin(t *testing.T) {
	chunk := testingc.FixtureChunk("7000")
	serverAddr := swarm.MustParseHexAddress("5000000000000000000000000000000000000000000000000000000000000000")
	clientAddr := swarm.MustParseHexAddress("9ee7add8")
	pricer := pricermock.NewMockService(defaultPrice, defaultPrice)

	serverStorer := &testStorer{ChunkStore: inmemchunkstore.New()}
	if err := serverStorer.Put(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	server := createRetrieval(t, serverAddr, serverStorer, nil, nil, log.Noop, accountingmock.NewAccounting(), pricer, nil, false)

	for range 100 {
		release := make(chan struct{})
		recorder := streamtest.New(
			streamtest.WithBaseAddr(clientAddr),
			streamtest.WithProtocols(server.Protocol()),
			streamtest.WithMiddlewares(func(h p2p.HandlerFunc) p2p.HandlerFunc {
				return func(ctx context.Context, p p2p.Peer, s p2p.Stream) error {
					<-release
					if err := h(ctx, p, s); err != nil {
						return err
					}
					return s.Close()
				}
			}),
		)
		topology := topologymock.NewTopologyDriver(topologymock.WithPeers(serverAddr))
		client := createRetrieval(t, clientAddr, &testStorer{ChunkStore: inmemchunkstore.New()}, recorder, topology, log.Noop, accountingmock.NewAccounting(), pricer, nil, false)

		var wg sync.WaitGroup
		wg.Go(func() { _, _ = client.RetrieveChunk(context.Background(), chunk.Address(), swarm.ZeroAddress) })
		time.Sleep(time.Millisecond)

		leave, leaveCancel := context.WithCancel(context.Background())
		wg.Go(func() { _, _ = client.RetrieveChunk(leave, chunk.Address(), swarm.ZeroAddress) })
		time.Sleep(time.Millisecond)

		wg.Go(leaveCancel)
		wg.Go(func() { _, _ = client.RetrieveChunk(context.Background(), chunk.Address(), swarm.ZeroAddress) })
		time.Sleep(time.Millisecond)

		close(release)
		wg.Wait()
	}
}
