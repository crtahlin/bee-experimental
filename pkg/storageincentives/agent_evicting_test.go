// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives_test

import (
	"context"
	"math/big"
	"strings"
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
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	stakingmock "github.com/ethersphere/bee/v2/pkg/storageincentives/staking/mock"
	"github.com/ethersphere/bee/v2/pkg/storer"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// notSelectedContract is a mockContract whose neighbourhood is never
// selected.
type notSelectedContract struct{ *mockContract }

func (notSelectedContract) IsPlaying(context.Context, uint8) (bool, error) { return false, nil }

// anyDepthContract is a mockContract that selects the node at any depth,
// for a test in which the depth changes.
type anyDepthContract struct{ *mockContract }

func (anyDepthContract) IsPlaying(context.Context, uint8) (bool, error) { return true, nil }

// skippedWhileEvicting reads bee_storageincentives_skipped_while_evicting.
func skippedWhileEvicting(t *testing.T, a *storageincentives.Agent) float64 {
	t.Helper()
	for _, c := range a.Metrics() {
		ch := make(chan *prometheus.Desc, 4)
		c.Describe(ch)
		close(ch)
		for d := range ch {
			if !strings.Contains(d.String(), "skipped_while_evicting") {
				continue
			}
			m, ok := c.(prometheus.Metric)
			if !ok {
				t.Fatal("skipped_while_evicting is not a single metric")
			}
			var v dto.Metric
			if err := m.Write(&v); err != nil {
				t.Fatal(err)
			}
			return v.GetCounter().GetValue()
		}
	}
	t.Fatal("skipped_while_evicting not found")
	return 0
}

type evictingCase struct {
	evictingFor  time.Duration
	fullySynced  bool
	frozen       bool
	notSelected  bool
	stepInSample bool // the radius steps while the sample runs
}

// runEvictingAgent runs an agent that can afford to play through several
// rounds and returns the contract calls it made and the skip counter.
func runEvictingAgent(t *testing.T, tc evictingCase) ([]contractCall, float64) {
	t.Helper()
	var (
		calls   []contractCall
		skipped float64
	)
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
		var contract redistribution.Contract = mc
		switch {
		case tc.notSelected:
			contract = notSelectedContract{mc}
		case tc.stepInSample:
			contract = anyDepthContract{mc}
		}

		var reserve *resMock.ReserveStore
		opts := []resMock.Option{
			resMock.WithRadius(radius),
			resMock.WithSample(storer.RandSample(t, nil)),
			resMock.WithEvictingFor(tc.evictingFor),
		}
		if tc.stepInSample {
			opts = append(opts, resMock.WithSampleHook(func() {
				reserve.SetStorageRadius(reserve.StorageRadius() + 1)
			}))
		}
		reserve = resMock.NewReserve(opts...)

		postageContract := contractMock.New(contractMock.WithExpiresBatchesFunc(func(context.Context) error { return nil }))
		stakingContract := stakingmock.New(stakingmock.WithIsFrozen(func(context.Context, uint64) (bool, error) { return tc.frozen, nil }))

		agent, err := storageincentives.New(
			swarm.RandAddress(t), common.Address{},
			backend, contract, postageContract, stakingContract, reserve,
			func() bool { return tc.fullySynced },
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
		calls = mc.getCalls()
		skipped = skippedWhileEvicting(t, agent)
	})
	return calls, skipped
}

func hasCommit(calls []contractCall) bool {
	for _, c := range calls {
		if c == commitCall {
			return true
		}
	}
	return false
}

// TestAgentSkipsRoundWhileEvicting checks the gate of #649: a selected node
// that has been evicting for at least storer.EvictingMinAge commits nothing
// and counts the skip; a shorter eviction, or none, does not stop it.
func TestAgentSkipsRoundWhileEvicting(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		evictingFor time.Duration
		wantPlay    bool
	}{
		{"evicting two minutes", 2 * time.Minute, false},
		{"evicting exactly the minimum", storer.EvictingMinAge, false},
		{"evicting thirty seconds", 30 * time.Second, true},
		{"not evicting", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls, skipped := runEvictingAgent(t, evictingCase{evictingFor: tc.evictingFor, fullySynced: true})
			if got := hasCommit(calls); got != tc.wantPlay {
				t.Fatalf("committed %v, want %v (calls %v)", got, tc.wantPlay, calls)
			}
			if tc.wantPlay && skipped != 0 {
				t.Fatalf("skipped_while_evicting %v, want 0", skipped)
			}
			if !tc.wantPlay && skipped < 1 {
				t.Fatalf("skipped_while_evicting %v, want at least 1", skipped)
			}
		})
	}
}

// TestAgentEvictingGateOrder checks where the gate sits in handleSample:
// after the frozen and selection checks, before the fully-synced check.
func TestAgentEvictingGateOrder(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		c           evictingCase
		wantSkipped bool
	}{
		{"frozen returns first", evictingCase{evictingFor: 2 * time.Minute, fullySynced: true, frozen: true}, false},
		{"not selected returns first", evictingCase{evictingFor: 2 * time.Minute, fullySynced: true, notSelected: true}, false},
		{"evicting and not synced counts as evicting", evictingCase{evictingFor: 2 * time.Minute, fullySynced: false}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls, skipped := runEvictingAgent(t, tc.c)
			if hasCommit(calls) {
				t.Fatalf("committed, want no commit (calls %v)", calls)
			}
			if got := skipped >= 1; got != tc.wantSkipped {
				t.Fatalf("skipped_while_evicting %v, want counted %v", skipped, tc.wantSkipped)
			}
		})
	}
}

// TestAgentSkipsRoundWhenDepthChangesInSample checks that a sample whose
// committed depth changed while it ran is not committed (#649).
func TestAgentSkipsRoundWhenDepthChangesInSample(t *testing.T) {
	t.Parallel()

	calls, _ := runEvictingAgent(t, evictingCase{fullySynced: true, stepInSample: true})
	if hasCommit(calls) {
		t.Fatalf("committed a sample whose depth changed, calls %v", calls)
	}
}
