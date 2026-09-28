// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package chequebook_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/sctx"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/chequebook"
	chequestoremock "github.com/ethersphere/bee/v2/pkg/settlement/swap/chequestore/mock"
	erc20mock "github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20/mock"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
)

// The chequebook's transactions are estimated, with the fixed limit they used
// before as the floor, so a change in the gas schedule (Glamsterdam's
// EIP-8037, #541) does not make them run out of gas.

// recordSend returns a transaction mock that keeps the last request sent.
func recordSend(got **transaction.TxRequest) transactionmock.Option {
	return transactionmock.WithSendFunc(func(_ context.Context, request *transaction.TxRequest, _ int) (common.Hash, error) {
		*got = request
		return common.HexToHash("0x1"), nil
	})
}

func expectEstimated(t *testing.T, request *transaction.TxRequest, floor uint64) {
	t.Helper()
	if request == nil {
		t.Fatal("no transaction was sent")
	}
	if request.GasLimit != 0 {
		t.Fatalf("got fixed gas limit %d, want 0 so the gas is estimated", request.GasLimit)
	}
	if request.MinEstimatedGasLimit != floor {
		t.Fatalf("got gas floor %d, want %d", request.MinEstimatedGasLimit, floor)
	}
}

func TestDeployEstimatesGas(t *testing.T) {
	t.Parallel()

	var got *transaction.TxRequest
	factory := chequebook.NewFactory(backendmock.New(), transactionmock.New(recordSend(&got)), common.HexToAddress("0xabcd"))

	if _, err := factory.Deploy(context.Background(), common.HexToAddress("0xefff"), big.NewInt(0), common.HexToHash("0x2")); err != nil {
		t.Fatal(err)
	}
	expectEstimated(t, got, chequebook.DeployGasLimitFloor)
}

func TestWithdrawEstimatesGas(t *testing.T) {
	t.Parallel()

	var got *transaction.TxRequest
	address := common.HexToAddress("0xabcd")
	store := statestore.NewStateStore()
	service, err := chequebook.New(
		transactionmock.New(
			transactionmock.WithABICallSequence(
				transactionmock.ABICall(&chequebookABI, address, big.NewInt(100).FillBytes(make([]byte, 32)), "balance"),
				transactionmock.ABICall(&chequebookABI, address, big.NewInt(0).FillBytes(make([]byte, 32)), "totalPaidOut"),
			),
			recordSend(&got),
		),
		address,
		common.HexToAddress("0xfff"),
		store,
		&chequeSignerMock{},
		erc20mock.New(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Withdraw(context.Background(), big.NewInt(50)); err != nil {
		t.Fatal(err)
	}
	expectEstimated(t, got, chequebook.WithdrawGasLimitFloor)
}

func TestCashoutGasLimit(t *testing.T) {
	t.Parallel()

	chequebookAddress := common.HexToAddress("0xcfec")
	cheque := &chequebook.SignedCheque{
		Cheque: chequebook.Cheque{
			Beneficiary:      common.HexToAddress("0xfff"),
			CumulativePayout: big.NewInt(500),
			Chequebook:       chequebookAddress,
		},
		Signature: []byte{0, 1, 2, 3},
	}
	store := statestore.NewStateStore()
	chequeStore := chequestoremock.NewChequeStore(
		chequestoremock.WithLastChequeFunc(func(common.Address) (*chequebook.SignedCheque, error) { return cheque, nil }),
	)

	for _, tc := range []struct {
		name  string
		ctx   context.Context
		fixed uint64
	}{
		{name: "estimated without a header", ctx: context.Background()},
		{name: "header sets a fixed limit", ctx: sctx.SetGasLimit(context.Background(), 123_456), fixed: 123_456},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got *transaction.TxRequest
			cashout := chequebook.NewCashoutService(store, backendmock.New(), transactionmock.New(recordSend(&got)), chequeStore)
			if _, err := cashout.CashCheque(tc.ctx, chequebookAddress, common.HexToAddress("0xbbbb")); err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("no transaction was sent")
			}
			if got.GasLimit != tc.fixed {
				t.Fatalf("got gas limit %d, want %d", got.GasLimit, tc.fixed)
			}
			if got.MinEstimatedGasLimit != chequebook.CashoutGasLimitFloor {
				t.Fatalf("got gas floor %d, want %d", got.MinEstimatedGasLimit, chequebook.CashoutGasLimitFloor)
			}
		})
	}
}

// fakeFactory stands in for the chequebook factory in Init.
type fakeFactory struct {
	mu        sync.Mutex
	waitErr   error
	deployed  []common.Hash
	deployTx  common.Hash
	address   common.Address
	waitedFor []common.Hash
}

func (f *fakeFactory) ERC20Address(context.Context) (common.Address, error) {
	return common.Address{}, nil
}

func (f *fakeFactory) Deploy(context.Context, common.Address, *big.Int, common.Hash) (common.Hash, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deployed = append(f.deployed, f.deployTx)
	return f.deployTx, nil
}

func (f *fakeFactory) WaitDeployed(_ context.Context, txHash common.Hash) (common.Address, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waitedFor = append(f.waitedFor, txHash)
	if f.waitErr != nil && txHash != f.deployTx {
		return common.Address{}, f.waitErr
	}
	return f.address, nil
}

func (f *fakeFactory) VerifyChequebook(context.Context, common.Address) error { return nil }

func TestInitClearsRevertedDeployment(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	savedTx := common.HexToHash("0xdead")
	newTx := common.HexToHash("0xbeef")

	store := statestore.NewStateStore()
	if err := store.Put(chequebook.ChequebookDeploymentKey, savedTx); err != nil {
		t.Fatal(err)
	}

	factory := &fakeFactory{
		waitErr:  fmt.Errorf("contract deployment failed: %w", transaction.ErrTransactionReverted),
		deployTx: newTx,
		address:  common.HexToAddress("0xcbcb"),
	}
	backend := backendmock.New(
		backendmock.WithBalanceAt(func(context.Context, common.Address, *big.Int) (*big.Int, error) {
			return big.NewInt(1_000_000_000_000_000_000), nil
		}),
		backendmock.WithSuggestedFeeAndTipFunc(func(context.Context, *big.Int, int) (*big.Int, *big.Int, error) {
			return big.NewInt(1), big.NewInt(1), nil
		}),
	)
	erc20 := erc20mock.New(erc20mock.WithBalanceOfFunc(func(context.Context, common.Address) (*big.Int, error) {
		return big.NewInt(0), nil
	}))
	initOnce := func() error {
		_, err := chequebook.Init(ctx, factory, store, log.Noop, big.NewInt(0), transactionmock.New(), backend, 0, common.HexToAddress("0xfff"), &chequeSignerMock{}, erc20)
		return err
	}

	// The saved deployment reverted: the error is returned, the hash is
	// cleared, and nothing is deployed in this start.
	if err := initOnce(); !errors.Is(err, transaction.ErrTransactionReverted) {
		t.Fatalf("got %v, want %v", err, transaction.ErrTransactionReverted)
	}
	var stored common.Hash
	if err := store.Get(chequebook.ChequebookDeploymentKey, &stored); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("saved deployment still there (%x, %v), want it cleared", stored, err)
	}
	if len(factory.deployed) != 0 {
		t.Fatalf("deployed %d chequebooks in the failing start, want none", len(factory.deployed))
	}

	// The next start deploys a new chequebook.
	if err := initOnce(); err != nil {
		t.Fatal(err)
	}
	if len(factory.deployed) != 1 {
		t.Fatalf("got %d deployments on the next start, want 1", len(factory.deployed))
	}
}

// TestInitKeepsDeploymentOnOtherErrors: a wait that fails for any reason other
// than a revert (here an RPC error) keeps the saved deployment, so a deployment
// that is only slow or unreachable for now is never sent twice.
func TestInitKeepsDeploymentOnOtherErrors(t *testing.T) {
	t.Parallel()

	savedTx := common.HexToHash("0xdead")
	store := statestore.NewStateStore()
	if err := store.Put(chequebook.ChequebookDeploymentKey, savedTx); err != nil {
		t.Fatal(err)
	}
	rpcErr := errors.New("rpc: connection refused")
	factory := &fakeFactory{waitErr: rpcErr}

	_, err := chequebook.Init(context.Background(), factory, store, log.Noop, big.NewInt(0), transactionmock.New(), backendmock.New(), 0, common.HexToAddress("0xfff"), &chequeSignerMock{}, erc20mock.New())
	if !errors.Is(err, rpcErr) {
		t.Fatalf("got %v, want %v", err, rpcErr)
	}

	var stored common.Hash
	if err := store.Get(chequebook.ChequebookDeploymentKey, &stored); err != nil || stored != savedTx {
		t.Fatalf("got saved deployment %x (%v), want %x kept", stored, err, savedTx)
	}
	if len(factory.deployed) != 0 {
		t.Fatalf("deployed %d chequebooks, want none", len(factory.deployed))
	}
}
