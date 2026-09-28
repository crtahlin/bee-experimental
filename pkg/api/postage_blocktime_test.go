// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api_test

import (
	"encoding/hex"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/api"
	"github.com/ethersphere/bee/v2/pkg/jsonhttp/jsonhttptest"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
)

// TestBatchTTLFollowsBlockTime: the TTL is remaining blocks times the block
// time the node reports now, not a fixed one, and a block time that is not a
// whole number of seconds is not truncated (#540).
func TestBatchTTLFollowsBlockTime(t *testing.T) {
	t.Parallel()

	b := postagetesting.MustNewBatch()
	b.Value = big.NewInt(105)
	// (105 - 5) / 2 = 50 blocks remaining
	cs := &postage.ChainState{Block: 10, TotalAmount: big.NewInt(5), CurrentPrice: big.NewInt(2)}

	for _, tc := range []struct {
		name      string
		blockTime time.Duration
		ttl       int64
	}{
		{"5 s blocks", 5 * time.Second, 250},
		{"2 s blocks", 2 * time.Second, 100},
		{"2.5 s blocks", 2500 * time.Millisecond, 125},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bs := mock.New(mock.WithChainState(cs), mock.WithBatch(b))
			ts, _, _, _ := newTestServer(t, testServerOptions{
				BatchStore:    bs,
				BlockTime:     time.Hour, // must be ignored when BlockTimeFunc is set
				BlockTimeFunc: func() time.Duration { return tc.blockTime },
			})

			var got api.PostageBatchResponse
			jsonhttptest.Request(t, ts, http.MethodGet, "/batches/"+hex.EncodeToString(b.ID), http.StatusOK,
				jsonhttptest.WithUnmarshalJSONResponse(&got),
			)
			if got.BatchTTL != tc.ttl {
				t.Fatalf("got TTL %d, want %d", got.BatchTTL, tc.ttl)
			}
		})
	}
}
