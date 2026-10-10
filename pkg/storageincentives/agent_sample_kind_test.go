// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives_test

import (
	"context"
	"math/big"
	"sync"
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
	stakingmock "github.com/ethersphere/bee/v2/pkg/storageincentives/staking/mock"
	"github.com/ethersphere/bee/v2/pkg/storer"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// The lottery's sample is marked as such, so it pauses eviction for its
// whole duration, and /rchash, which goes through the same agent path, is
// not, so it uses the shared budget (wasp #659).
func TestAgentMarksTheLotterySample(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const (
			blocksPerRound = 9
			blocksPerPhase = 3
		)
		var radius uint8 = 8
		wait := make(chan struct{}, 1)
		backend := &mockchainBackend{
			limit:         blocksPerRound * 6,
			limitCallback: func() { wait <- struct{}{} },
			incrementBy:   1,
			block:         blocksPerRound,
			balance:       big.NewInt(4_000_000_000),
		}
		mc := &mockContract{t: t, expectedRadius: radius}

		var (
			mu    sync.Mutex
			kinds []bool
		)
		reserve := resMock.NewReserve(
			resMock.WithRadius(radius),
			resMock.WithSample(storer.RandSample(t, nil)),
			resMock.WithSampleContextHook(func(ctx context.Context) {
				mu.Lock()
				kinds = append(kinds, storer.IsLotterySample(ctx))
				mu.Unlock()
			}),
		)

		agent, err := storageincentives.New(
			swarm.RandAddress(t), common.Address{},
			backend, mc, contractMock.New(contractMock.WithExpiresBatchesFunc(func(context.Context) error { return nil })),
			stakingmock.New(stakingmock.WithIsFrozen(func(context.Context, uint64) (bool, error) { return false, nil })),
			reserve,
			func() bool { return true },
			func() time.Duration { return 100 * time.Millisecond },
			blocksPerRound, blocksPerPhase,
			statestore.NewStateStore(), &postage.NoOpBatchStore{}, erc20mock.New(), transactionmock.New(),
			&mockHealth{}, log.Noop, storer.ReserveProofModeClassic,
		)
		if err != nil {
			t.Fatal(err)
		}
		testutil.CleanupCloser(t, agent)

		<-wait
		synctest.Wait()

		// An /rchash call through the same agent.
		if _, err := agent.SampleWithProofs(context.Background(), make([]byte, 32), make([]byte, 32), radius); err != nil {
			t.Fatal(err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(kinds) < 2 {
			t.Fatalf("%d samples, want at least a lottery sample and the /rchash call", len(kinds))
		}
		if !kinds[0] {
			t.Error("the lottery's sample is not marked as the lottery's")
		}
		if kinds[len(kinds)-1] {
			t.Error("the /rchash sample is marked as the lottery's")
		}
	})
}

// A lottery sample and an /rchash call with the same anchor and depth do
// not share one singleflight call (wasp #659).
func TestSampleFlightKeyIncludesKind(t *testing.T) {
	t.Parallel()
	anchor := []byte{1, 2, 3}
	lottery := storageincentives.SampleFlightKey(storer.WithLotterySample(context.Background()), anchor, 9)
	other := storageincentives.SampleFlightKey(context.Background(), anchor, 9)
	if lottery == other {
		t.Fatalf("lottery and /rchash samples share the key %q", lottery)
	}
}
