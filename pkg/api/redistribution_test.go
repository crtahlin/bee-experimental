// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api_test

import (
	"context"
	"math/big"
	"net/http"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/api"
	"github.com/ethersphere/bee/v2/pkg/jsonhttp"
	"github.com/ethersphere/bee/v2/pkg/jsonhttp/jsonhttptest"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storageincentives"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	"github.com/ethersphere/bee/v2/pkg/transaction/mock"
)

func TestRedistributionStatus(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		store := statestore.NewStateStore()
		err := store.Put("redistribution_state", storageincentives.Status{
			Phase: storageincentives.PhaseType(1),
			Round: 1,
			Block: 12,
		})
		if err != nil {
			t.Errorf("redistribution put state: %v", err)
		}
		srv, _, _, _ := newTestServer(t, testServerOptions{
			StateStorer: store,
			TransactionOpts: []mock.Option{
				mock.WithTransactionFeeFunc(func(ctx context.Context, txHash common.Hash) (*big.Int, error) {
					return big.NewInt(1000), nil
				}),
			},
			BackendOpts: []backendmock.Option{
				backendmock.WithBalanceAt(func(ctx context.Context, address common.Address, block *big.Int) (*big.Int, error) {
					return big.NewInt(100000000), nil
				}),
				backendmock.WithSuggestedFeeAndTipFunc(func(ctx context.Context, gasPrice *big.Int, boostPercent int) (*big.Int, *big.Int, error) {
					return big.NewInt(1), big.NewInt(2), nil
				}),
			},
		})
		jsonhttptest.Request(t, srv, http.MethodGet, "/redistributionstate", http.StatusOK,
			jsonhttptest.WithRequestHeader(api.ContentTypeHeader, "application/json; charset=utf-8"),
		)
	})

	// The reveal fields and the persisted stop counts (#725); the old
	// fields keep their values.
	t.Run("reveal pending and stop counts", func(t *testing.T) {
		t.Parallel()

		store := statestore.NewStateStore()
		err := store.Put("redistribution_state", storageincentives.Status{
			Phase: storageincentives.PhaseType(2), // reveal
			Round: 0,
			Block: 5, // the reveal phase of round 0 is blocks 4-7
			RoundData: map[uint64]storageincentives.RoundData{
				0: {CommitKey: []byte("key")},
			},
			StopWaitedForReveal: 3,
			RevealMissedOnStop:  1,
			RevealAfterRestart:  2,
		})
		if err != nil {
			t.Fatal(err)
		}
		srv, _, _, _ := newTestServer(t, testServerOptions{
			StateStorer: store,
			BackendOpts: []backendmock.Option{
				backendmock.WithBalanceAt(func(ctx context.Context, address common.Address, block *big.Int) (*big.Int, error) {
					return big.NewInt(100000000), nil
				}),
				backendmock.WithSuggestedFeeAndTipFunc(func(ctx context.Context, gasPrice *big.Int, boostPercent int) (*big.Int, *big.Int, error) {
					return big.NewInt(1), big.NewInt(2), nil
				}),
			},
		})

		var got map[string]any
		jsonhttptest.Request(t, srv, http.MethodGet, "/redistributionstate", http.StatusOK,
			jsonhttptest.WithUnmarshalJSONResponse(&got),
		)
		for field, want := range map[string]any{
			"revealPending":           true,
			"safeToRestartAfterBlock": float64(7),
			"stopWaitedForReveal":     float64(3),
			"revealMissedOnStop":      float64(1),
			"revealAfterRestart":      float64(2),
			"phase":                   "reveal",
			"round":                   float64(0),
			"block":                   float64(5),
			"hasSufficientFunds":      true,
		} {
			if got[field] != want {
				t.Errorf("%s: got %v, want %v", field, got[field], want)
			}
		}
	})

	t.Run("bad request", func(t *testing.T) {
		t.Parallel()

		srv, _, _, _ := newTestServer(t, testServerOptions{
			BeeMode:     api.LightMode,
			StateStorer: statestore.NewStateStore(),
			TransactionOpts: []mock.Option{
				mock.WithTransactionFeeFunc(func(ctx context.Context, txHash common.Hash) (*big.Int, error) {
					return big.NewInt(1000), nil
				}),
			},
		})
		jsonhttptest.Request(t, srv, http.MethodGet, "/redistributionstate", http.StatusBadRequest,
			jsonhttptest.WithExpectedJSONResponse(jsonhttp.StatusResponse{
				Message: api.ErrOperationSupportedOnlyInFullMode.Error(),
				Code:    http.StatusBadRequest,
			}),
		)
	})
}
