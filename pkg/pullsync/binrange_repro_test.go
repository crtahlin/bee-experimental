// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestBinOutOfRangeRefused: a Get for a bin the reserve cannot hold (40 >=
// swarm.MaxBins) must be refused at once, not left waiting for a first chunk
// that can never arrive. Uses only upstream signals.
func TestBinOutOfRangeRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5, mock.WithSubscribeResp(nil, nil))
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()),
			streamtest.WithBaseAddr(swarm.MustParseHexAddress("aa00000000000000000000000000000000000000000000000000000000000000")))
		client, _ := newPullSync(t, recorder, 0)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		res := make(chan error, 1)
		go func() { _, _, err := client.Sync(ctx, swarm.ZeroAddress, 40, 1); res <- err }()
		synctest.Wait()
		select {
		case err := <-res:
			if err == nil {
				t.Fatal("request for bin 40 returned without error, want it refused")
			}
		case <-time.After(time.Minute):
			cancel()
			<-res
			t.Fatal("request for bin 40: still waiting after 1m0s, want it refused at once")
		}
	})
}
