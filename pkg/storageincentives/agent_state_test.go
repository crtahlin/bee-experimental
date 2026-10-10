// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives_test

import (
	"context"
	"math/big"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage"
	contractMock "github.com/ethersphere/bee/v2/pkg/postage/postagecontract/mock"
	erc20mock "github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20/mock"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storageincentives"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/staking/mock"
	"github.com/ethersphere/bee/v2/pkg/storer"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// The agent runs on the Revealer's state, not on its own: a reveal recorded
// through the Revealer survives the agent's writes of the current block
// (#725, test 11; two instances each wrote the whole status).
func TestAgentSharesTheRevealersState(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		store := statestore.NewStateStore()
		backend := &mockchainBackend{limit: 1_000_000, block: 5, balance: big.NewInt(0), incrementBy: 1}
		contract := &mockContract{t: t, expectedRadius: 0}
		blockTime := func() time.Duration { return 100 * time.Millisecond }
		revealer, err := newRevealer(store, contract, transactionmock.New(), backend, blockTime, 12, 4)
		if err != nil {
			t.Fatal(err)
		}

		agent, err := createServiceWithRevealer(t, backend, contract, blockTime, revealer)
		if err != nil {
			t.Fatal(err)
		}
		testutil.CleanupCloser(t, agent)
		synctest.Wait()

		revealer.State().SetHasRevealed(0)
		time.Sleep(20 * blockTime()) // the agent writes the block several times
		synctest.Wait()

		reloaded, err := storageincentives.NewRedistributionState(log.Noop, common.Address{}, store, erc20mock.New(), transactionmock.New())
		if err != nil {
			t.Fatal(err)
		}
		if !reloaded.HasRevealed(0) {
			t.Fatal("the agent's write of the block erased HasRevealed")
		}
	})
}

func createServiceWithRevealer(t *testing.T, backend storageincentives.ChainBackend, contract *mockContract, blockTime func() time.Duration, revealer *storageincentives.Revealer) (*storageincentives.Agent, error) {
	t.Helper()
	return storageincentives.New(
		swarm.RandAddress(t),
		backend,
		contract,
		contractMock.New(contractMock.WithExpiresBatchesFunc(func(context.Context) error { return nil })),
		mock.New(mock.WithIsFrozen(func(context.Context, uint64) (bool, error) { return false, nil })),
		resMock.NewReserve(resMock.WithSample(storer.RandSample(t, nil))),
		func() bool { return true },
		blockTime,
		12, 4,
		revealer,
		&postage.NoOpBatchStore{},
		&mockHealth{},
		log.Noop,
		storer.ReserveProofModeClassic,
	)
}
