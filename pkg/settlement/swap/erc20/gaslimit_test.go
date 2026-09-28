// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package erc20_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
)

// TestTransferEstimatesGas: a token transfer is estimated, with the fixed limit
// it used before as the floor (#541). A transfer to an address holding no
// tokens creates a balance slot, which Glamsterdam's EIP-8037 prices above the
// old fixed limit.
func TestTransferEstimatesGas(t *testing.T) {
	t.Parallel()

	var got *transaction.TxRequest
	service := erc20.New(
		transactionmock.New(transactionmock.WithSendFunc(func(_ context.Context, request *transaction.TxRequest, _ int) (common.Hash, error) {
			got = request
			return common.HexToHash("0x1"), nil
		})),
		common.HexToAddress("0xabcd"),
	)

	if _, err := service.Transfer(context.Background(), common.HexToAddress("0xffff"), big.NewInt(10)); err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("no transaction was sent")
	}
	if got.GasLimit != 0 {
		t.Fatalf("got fixed gas limit %d, want 0 so the gas is estimated", got.GasLimit)
	}
	if got.MinEstimatedGasLimit != erc20.TransferGasLimitFloor {
		t.Fatalf("got gas floor %d, want %d", got.MinEstimatedGasLimit, erc20.TransferGasLimitFloor)
	}
}
