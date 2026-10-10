// Copyright 2022 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage"
	contractMock "github.com/ethersphere/bee/v2/pkg/postage/postagecontract/mock"
	erc20mock "github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20/mock"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storageincentives"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/staking/mock"
	"github.com/ethersphere/bee/v2/pkg/storer"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

func TestAgent(t *testing.T) {
	t.Parallel()

	bigBalance := big.NewInt(4_000_000_000)
	tests := []struct {
		name           string
		blocksPerRound uint64
		blocksPerPhase uint64
		incrementBy    uint64
		limit          uint64
		expectedCalls  bool
		balance        *big.Int
		doubling       uint8
	}{
		{
			name:           "3 blocks per phase, same block number returns twice",
			blocksPerRound: 9,
			blocksPerPhase: 3,
			incrementBy:    1,
			expectedCalls:  true,
			limit:          108, // computed with blocksPerRound * (exptectedCalls + 2)
			balance:        bigBalance,
			doubling:       1,
		}, {
			name:           "3 blocks per phase, block number returns every block",
			blocksPerRound: 9,
			blocksPerPhase: 3,
			incrementBy:    1,
			expectedCalls:  true,
			limit:          108,
			balance:        bigBalance,
			doubling:       0,
		}, {
			name:           "no expected calls - block number returns late after each phase",
			blocksPerRound: 9,
			blocksPerPhase: 3,
			incrementBy:    6,
			expectedCalls:  false,
			limit:          108,
			balance:        bigBalance,
			doubling:       0,
		}, {
			name:           "4 blocks per phase, block number returns every other block",
			blocksPerRound: 12,
			blocksPerPhase: 4,
			incrementBy:    2,
			expectedCalls:  true,
			limit:          144,
			balance:        bigBalance,
			doubling:       1,
		}, {
			// This test case is based on previous, but this time agent will not have enough
			// balance to participate in the game so no calls are going to be made.
			name:           "no expected calls - insufficient balance",
			blocksPerRound: 12,
			blocksPerPhase: 4,
			incrementBy:    2,
			expectedCalls:  false,
			limit:          144,
			balance:        big.NewInt(0),
			doubling:       1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				wait := make(chan struct{}, 1)
				addr := swarm.RandAddress(t)

				backend := &mockchainBackend{
					limit: tc.limit,
					limitCallback: func() {
						wait <- struct{}{}
					},
					incrementBy: tc.incrementBy,
					block:       tc.blocksPerRound,
					balance:     tc.balance,
				}

				var radius uint8 = 8

				contract := &mockContract{t: t, expectedRadius: radius + tc.doubling}

				service, _ := createService(t, addr, backend, contract, tc.blocksPerRound, tc.blocksPerPhase, radius, tc.doubling, storer.ReserveProofModeClassic)
				testutil.CleanupCloser(t, service)

				<-wait

				synctest.Wait()

				calls := contract.getCalls()

				if !tc.expectedCalls {
					if len(calls) > 0 {
						t.Fatal("got unexpected calls")
					}
					return
				}

				if len(calls) == 0 {
					t.Fatal("expected calls but got none")
				}

				assertOrder := func(t *testing.T, want, got contractCall) {
					t.Helper()
					if want != got {
						t.Fatalf("expected call %s, got %s", want, got)
					}
				}

				prevCall := calls[0]

				for i := 1; i < len(calls); i++ {
					switch calls[i] {
					case isWinnerCall:
						assertOrder(t, revealCall, prevCall)
					case revealCall:
						assertOrder(t, commitCall, prevCall)
					case commitCall:
						assertOrder(t, isWinnerCall, prevCall)
					}

					prevCall = calls[i]
				}
			})
		})
	}
}

func createService(
	t *testing.T,
	addr swarm.Address,
	backend storageincentives.ChainBackend,
	contract redistribution.Contract,
	blocksPerRound uint64,
	blocksPerPhase uint64,
	radius uint8,
	doubling uint8,
	reserveProofMode string,
	reserveOpts ...resMock.Option,
) (*storageincentives.Agent, error) {
	t.Helper()
	return createServiceWithBlockTime(t, addr, backend, contract, func() time.Duration { return time.Millisecond * 100 },
		blocksPerRound, blocksPerPhase, radius, doubling, reserveProofMode, reserveOpts...)
}

func createServiceWithBlockTime(
	t *testing.T,
	addr swarm.Address,
	backend storageincentives.ChainBackend,
	contract redistribution.Contract,
	blockTime func() time.Duration,
	blocksPerRound uint64,
	blocksPerPhase uint64,
	radius uint8,
	doubling uint8,
	reserveProofMode string,
	reserveOpts ...resMock.Option,
) (*storageincentives.Agent, error) {
	t.Helper()

	postageContract := contractMock.New(contractMock.WithExpiresBatchesFunc(func(context.Context) error {
		return nil
	}),
	)
	stakingContract := mock.New(mock.WithIsFrozen(func(context.Context, uint64) (bool, error) {
		return false, nil
	}))

	reserveOpts = append([]resMock.Option{
		resMock.WithRadius(radius),
		resMock.WithSample(storer.RandSample(t, nil)),
		resMock.WithCapacityDoubling(int(doubling)),
	}, reserveOpts...)
	reserve := resMock.NewReserve(reserveOpts...)

	revealer, err := newRevealer(statestore.NewStateStore(), contract, transactionmock.New(), backend, blockTime, blocksPerRound, blocksPerPhase)
	if err != nil {
		return nil, err
	}

	return storageincentives.New(
		addr,
		backend,
		contract,
		postageContract,
		stakingContract,
		reserve,
		func() bool { return true },
		blockTime,
		blocksPerRound,
		blocksPerPhase,
		revealer,
		&postage.NoOpBatchStore{},
		&mockHealth{},
		log.Noop,
		reserveProofMode,
	)
}

// newRevealer builds the shared state and Revealer an agent runs on.
func newRevealer(store storage.StateStorer, contract redistribution.Contract, txService transaction.Service, backend storageincentives.RevealBackend, blockTime func() time.Duration, blocksPerRound, blocksPerPhase uint64) (*storageincentives.Revealer, error) {
	state, err := storageincentives.NewRedistributionState(log.Noop, common.Address{}, store, erc20mock.New(), txService)
	if err != nil {
		return nil, err
	}
	return storageincentives.NewRevealer(state, contract, txService, backend, blockTime, blocksPerRound, blocksPerPhase, log.Noop), nil
}

type mockchainBackend struct {
	mu            sync.Mutex
	incrementBy   uint64
	block         uint64
	limit         uint64
	limitCallback func()
	balance       *big.Int
}

func (m *mockchainBackend) BlockNumber(context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := m.block
	lim := m.limit
	inc := m.incrementBy

	if lim == 0 || ret+inc < lim {
		m.block += inc
	} else if m.limitCallback != nil {
		m.limitCallback()
		return 0, errors.New("reached limit")
	}

	return ret, nil
}

func (m *mockchainBackend) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return &types.Header{
		Time: uint64(time.Now().Unix()),
	}, nil
}

func (m *mockchainBackend) BalanceAt(ctx context.Context, address common.Address, block *big.Int) (*big.Int, error) {
	return m.balance, nil
}

func (m *mockchainBackend) SuggestedFeeAndTip(ctx context.Context, gasPrice *big.Int, boostPercent int) (*big.Int, *big.Int, error) {
	return big.NewInt(4), big.NewInt(5), nil
}

type contractCall int

func (c contractCall) String() string {
	switch c {
	case isWinnerCall:
		return "isWinnerCall"
	case revealCall:
		return "revealCall"
	case commitCall:
		return "commitCall"
	case claimCall:
		return "claimCall"
	}
	return "unknown"
}

const (
	isWinnerCall contractCall = iota
	revealCall
	commitCall
	claimCall
)

type mockContract struct {
	callsList      []contractCall
	mtx            sync.Mutex
	expectedRadius uint8
	t              *testing.T
	// currentRound answers CurrentRound. When nil the call fails, and the
	// agent commits on its own round, as it did before the round check.
	currentRound func(ctx context.Context) (uint64, error)
}

func (m *mockContract) CurrentRound(ctx context.Context) (uint64, error) {
	if m.currentRound == nil {
		return 0, errors.New("mock: currentRound not set")
	}
	return m.currentRound(ctx)
}

// getCalls returns a snapshot of the calls list
// even after synctest.Wait() all goroutines are blocked, we still should use locking
// for defensive programming.
func (m *mockContract) getCalls() []contractCall {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	// return a copy to avoid external modifications
	calls := make([]contractCall, len(m.callsList))
	copy(calls, m.callsList)
	return calls
}

func (m *mockContract) ReserveSalt(context.Context) ([]byte, error) {
	return nil, nil
}

func (m *mockContract) IsPlaying(_ context.Context, r uint8) (bool, error) {
	if r != m.expectedRadius {
		m.t.Fatalf("isPlaying: expected radius %d, got %d", m.expectedRadius, r)
	}
	return true, nil
}

func (m *mockContract) IsWinner(context.Context) (bool, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.callsList = append(m.callsList, isWinnerCall)
	return false, nil
}

func (m *mockContract) Claim(context.Context, redistribution.ChunkInclusionProofs) (common.Hash, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.callsList = append(m.callsList, claimCall)
	return common.Hash{}, nil
}

func (m *mockContract) Commit(context.Context, []byte, uint64, transaction.BeforeBroadcastFunc) (common.Hash, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.callsList = append(m.callsList, commitCall)
	return common.Hash{}, nil
}

func (m *mockContract) Reveal(_ context.Context, r uint8, _ []byte, _ []byte, _ transaction.BeforeBroadcastFunc) (common.Hash, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if r != m.expectedRadius {
		m.t.Fatalf("reveal: expected radius %d, got %d", m.expectedRadius, r)
	}

	m.callsList = append(m.callsList, revealCall)
	return common.Hash{}, nil
}

type mockHealth struct{}

func (m *mockHealth) IsHealthy() bool { return true }

// TestAgentReserveProofModeRouting checks that the agent takes the windowed
// sample in windowed mode and the classic sample otherwise. The reserve mock is
// given a different sample for each path, so the two modes must yield different
// sample hashes through the shared, format-identical proof pipeline (#273).
func TestAgentReserveProofModeRouting(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		addr := swarm.RandAddress(t)
		anchor1 := testutil.RandBytes(t, 32)
		anchor2 := testutil.RandBytes(t, 32)
		var radius uint8 = 8

		classicSample := storer.RandSample(t, anchor1)
		windowedSample := storer.RandSample(t, anchor1)

		newAgent := func(mode string) *storageincentives.Agent {
			backend := &mockchainBackend{
				limit:       1_000_000,
				block:       12,
				balance:     big.NewInt(0), // cannot play, so the round loop stays quiet
				incrementBy: 1,
			}
			contract := &mockContract{t: t, expectedRadius: radius}
			svc, err := createService(t, addr, backend, contract, 12, 4, radius, 0, mode,
				resMock.WithSample(classicSample), resMock.WithWindowedSample(windowedSample))
			if err != nil {
				t.Fatal(err)
			}
			testutil.CleanupCloser(t, svc)
			return svc
		}

		classicRes, err := newAgent(storer.ReserveProofModeClassic).SampleWithProofs(context.Background(), anchor1, anchor2, radius)
		if err != nil {
			t.Fatalf("classic sample with proofs: %v", err)
		}
		windowedRes, err := newAgent(storer.ReserveProofModeWindowed).SampleWithProofs(context.Background(), anchor1, anchor2, radius)
		if err != nil {
			t.Fatalf("windowed sample with proofs: %v", err)
		}

		if classicRes.Hash.Equal(windowedRes.Hash) {
			t.Fatal("classic and windowed produced the same sample hash; the agent did not route by reserve-proof-mode")
		}
	})
}
