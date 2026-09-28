// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives_test

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// countingBackend answers a fixed block and counts BlockNumber calls, which the
// agent makes once per phase check.
type countingBackend struct {
	calls atomic.Int64
}

func (b *countingBackend) BlockNumber(context.Context) (uint64, error) {
	b.calls.Add(1)
	return 1, nil
}

func (b *countingBackend) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return &types.Header{Time: uint64(time.Now().Unix())}, nil
}

func (b *countingBackend) BalanceAt(context.Context, common.Address, *big.Int) (*big.Int, error) {
	return big.NewInt(0), nil
}

func (b *countingBackend) SuggestedFeeAndTip(context.Context, *big.Int, int) (*big.Int, *big.Int, error) {
	return big.NewInt(4), big.NewInt(5), nil
}

// TestAgentPhaseCheckFollowsBlockTime: the agent checks the phase every five
// blocks at the block time it is given now, so when the chain's block time
// changes the interval changes with it, without a restart (#540).
func TestAgentPhaseCheckFollowsBlockTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var blockTime atomic.Int64
		blockTime.Store(int64(5 * time.Second))

		backend := &countingBackend{}
		contract := &mockContract{t: t}
		service, _ := createServiceWithBlockTime(t, swarm.RandAddress(t), backend, contract,
			func() time.Duration { return time.Duration(blockTime.Load()) },
			152, 38, 8, 0, storer.ReserveProofModeClassic)
		testutil.CleanupCloser(t, service)

		// 5 s blocks: one initial check, then one every 25 s.
		time.Sleep(100 * time.Second)
		synctest.Wait()
		if got := backend.calls.Load(); got != 1+4 {
			t.Fatalf("at 5 s blocks got %d phase checks in 100 s, want 5", got)
		}

		// The chain moves to 2 s blocks. The wait already running keeps its
		// 25 s; every wait after it is 10 s: checks at 125, 135, ... 195 s.
		blockTime.Store(int64(2 * time.Second))
		before := backend.calls.Load()
		time.Sleep(100 * time.Second)
		synctest.Wait()
		if got := backend.calls.Load() - before; got != 8 {
			t.Fatalf("at 2 s blocks got %d phase checks in the next 100 s, want 8", got)
		}
	})
}

// TestAgentRoundCheck: the agent commits only when the contract's round
// agrees with its own, and plays as before when the contract cannot be asked
// (#540).
func TestAgentRoundCheck(t *testing.T) {
	t.Parallel()

	const blocksPerRound, blocksPerPhase = 9, 3

	for _, tc := range []struct {
		name       string
		round      func(b *mockchainBackend) func(context.Context) (uint64, error)
		wantCommit bool
	}{
		{
			name: "contract agrees",
			round: func(b *mockchainBackend) func(context.Context) (uint64, error) {
				return func(context.Context) (uint64, error) {
					b.mu.Lock()
					defer b.mu.Unlock()
					// the agent read block-1 for this phase; the backend has
					// since moved on by one
					return (b.block - 1) / blocksPerRound, nil
				}
			},
			wantCommit: true,
		},
		{
			name: "contract disagrees",
			round: func(*mockchainBackend) func(context.Context) (uint64, error) {
				return func(context.Context) (uint64, error) { return 1 << 40, nil }
			},
			wantCommit: false,
		},
		{
			name:       "contract cannot be asked",
			round:      func(*mockchainBackend) func(context.Context) (uint64, error) { return nil },
			wantCommit: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				wait := make(chan struct{}, 1)
				backend := &mockchainBackend{
					limit:         108,
					limitCallback: func() { wait <- struct{}{} },
					incrementBy:   1,
					block:         blocksPerRound,
					balance:       big.NewInt(4_000_000_000),
				}
				var radius uint8 = 8
				contract := &mockContract{t: t, expectedRadius: radius, currentRound: tc.round(backend)}

				service, _ := createService(t, swarm.RandAddress(t), backend, contract, blocksPerRound, blocksPerPhase, radius, 0, storer.ReserveProofModeClassic)
				testutil.CleanupCloser(t, service)

				<-wait
				synctest.Wait()

				committed := false
				for _, c := range contract.getCalls() {
					if c == commitCall {
						committed = true
					}
				}
				if committed != tc.wantCommit {
					t.Fatalf("committed %v, want %v (calls %v)", committed, tc.wantCommit, contract.getCalls())
				}
			})
		})
	}
}
