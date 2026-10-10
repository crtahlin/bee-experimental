// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storageincentives_test

import (
	"context"
	"math/big"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage"
	contractMock "github.com/ethersphere/bee/v2/pkg/postage/postagecontract/mock"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storageincentives"
	"github.com/ethersphere/bee/v2/pkg/storageincentives/redistribution"
	stakingmock "github.com/ethersphere/bee/v2/pkg/storageincentives/staking/mock"
	"github.com/ethersphere/bee/v2/pkg/storer"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/transaction"
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

// Reveal records the call without checking the depth, which changes in
// these tests.
func (c anyDepthContract) Reveal(context.Context, uint8, []byte, []byte, transaction.BeforeBroadcastFunc) (common.Hash, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.callsList = append(c.callsList, revealCall)
	return common.Hash{}, nil
}

// counterValue reads the agent counter whose name contains name.
func counterValue(t *testing.T, a *storageincentives.Agent, name string) float64 {
	t.Helper()
	for _, c := range a.Metrics() {
		ch := make(chan *prometheus.Desc, 4)
		c.Describe(ch)
		close(ch)
		for d := range ch {
			if !strings.Contains(d.String(), `"bee_storageincentives_`+name+`"`) {
				continue
			}
			m, ok := c.(prometheus.Metric)
			if !ok {
				t.Fatalf("%s is not a single metric", name)
			}
			var v dto.Metric
			if err := m.Write(&v); err != nil {
				t.Fatal(err)
			}
			return v.GetCounter().GetValue()
		}
	}
	t.Fatalf("%s not found", name)
	return 0
}

// skippedWhileEvicting reads bee_storageincentives_skipped_while_evicting.
func skippedWhileEvicting(t *testing.T, a *storageincentives.Agent) float64 {
	t.Helper()
	return counterValue(t, a, "skipped_while_evicting")
}

// radiusStep is a change of the storage radius made while the agent's
// first sample runs.
type radiusStep int

const (
	stepNone radiusStep = iota
	stepUp
	stepDown
	stepUpDown // up by one and back down again
)

// agentCounters are the agent's gate counters after a run.
type agentCounters struct {
	skippedWhileEvicting  float64
	skippedRadiusIncrease float64
	playedAfterDecrease   float64
}

type evictingCase struct {
	evictingFor  time.Duration
	fullySynced  bool
	frozen       bool
	notSelected  bool
	stepInSample radiusStep // how the radius changes while the first sample runs
}

// runEvictingAgent runs an agent that can afford to play through several
// rounds and returns the contract calls it made and the skip counter.
func runEvictingAgent(t *testing.T, tc evictingCase) ([]contractCall, float64) {
	t.Helper()
	calls, counters := runEvictingAgentCounters(t, tc)
	return calls, counters.skippedWhileEvicting
}

// runEvictingAgentCounters is runEvictingAgent returning all gate counters.
func runEvictingAgentCounters(t *testing.T, tc evictingCase) ([]contractCall, agentCounters) {
	t.Helper()
	var (
		calls    []contractCall
		counters agentCounters
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
		case tc.stepInSample != stepNone:
			contract = anyDepthContract{mc}
		}

		var reserve *resMock.ReserveStore
		opts := []resMock.Option{
			resMock.WithRadius(radius),
			resMock.WithSample(storer.RandSample(t, nil)),
			resMock.WithEvictingFor(tc.evictingFor),
		}
		if tc.stepInSample != stepNone {
			var once sync.Once
			opts = append(opts, resMock.WithSampleHook(func() {
				once.Do(func() {
					r := reserve.StorageRadius()
					switch tc.stepInSample {
					case stepUp:
						reserve.SetStorageRadius(r + 1)
					case stepDown:
						reserve.SetStorageRadius(r - 1)
					case stepUpDown:
						reserve.SetStorageRadius(r + 1)
						reserve.SetStorageRadius(r)
					}
				})
			}))
		}
		reserve = resMock.NewReserve(opts...)

		postageContract := contractMock.New(contractMock.WithExpiresBatchesFunc(func(context.Context) error { return nil }))
		stakingContract := stakingmock.New(stakingmock.WithIsFrozen(func(context.Context, uint64) (bool, error) { return tc.frozen, nil }))

		revealer, err := newRevealer(statestore.NewStateStore(), contract, transactionmock.New(), backend, func() time.Duration { return 100 * time.Millisecond }, blocksPerRound, blocksPerPhase)
		if err != nil {
			t.Fatal(err)
		}
		agent, err := storageincentives.New(
			swarm.RandAddress(t),
			backend, contract, postageContract, stakingContract, reserve,
			func() bool { return tc.fullySynced },
			func() time.Duration { return 100 * time.Millisecond },
			blocksPerRound, blocksPerPhase,
			revealer, &postage.NoOpBatchStore{},
			&mockHealth{}, log.Noop, storer.ReserveProofModeClassic,
		)
		if err != nil {
			t.Fatal(err)
		}
		testutil.CleanupCloser(t, agent)

		<-wait
		synctest.Wait()
		calls = mc.getCalls()
		counters = agentCounters{
			skippedWhileEvicting:  skippedWhileEvicting(t, agent),
			skippedRadiusIncrease: counterValue(t, agent, "skipped_radius_increase_total"),
			playedAfterDecrease:   counterValue(t, agent, "played_after_radius_decrease_total"),
		}
	})
	return calls, counters
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

// TestAgentSkipsRoundWhenDepthChangesInSample checks that a sample during
// which the radius increased is not committed and is counted (#649, #658).
func TestAgentSkipsRoundWhenDepthChangesInSample(t *testing.T) {
	t.Parallel()

	calls, c := runEvictingAgentCounters(t, evictingCase{fullySynced: true, stepInSample: stepUp})
	// The round whose sample saw the increase is not committed; later
	// rounds read the new radius and play.
	base, _ := runEvictingAgentCounters(t, evictingCase{fullySynced: true})
	if got, want := countCalls(calls, commitCall), countCalls(base, commitCall)-1; got != want {
		t.Fatalf("commits %d, want %d (one round fewer than with no radius change), calls %v", got, want, calls)
	}
	if c.skippedRadiusIncrease != 1 {
		t.Fatalf("skipped_radius_increase_total %v, want 1", c.skippedRadiusIncrease)
	}
	if c.playedAfterDecrease != 0 {
		t.Fatalf("played_after_radius_decrease_total %v, want 0", c.playedAfterDecrease)
	}
}

// TestAgentPlaysAfterRadiusDecreaseInSample checks that a decrease during
// the sample does not skip the round: it cannot change the sampled range
// (#658).
func TestAgentPlaysAfterRadiusDecreaseInSample(t *testing.T) {
	t.Parallel()

	calls, c := runEvictingAgentCounters(t, evictingCase{fullySynced: true, stepInSample: stepDown})
	if c.skippedRadiusIncrease != 0 {
		t.Fatalf("skipped_radius_increase_total %v, want 0", c.skippedRadiusIncrease)
	}
	if c.playedAfterDecrease != 1 {
		t.Fatalf("played_after_radius_decrease_total %v, want 1", c.playedAfterDecrease)
	}
	// As many commits as a run with no radius change: the round whose
	// sample saw the decrease was played.
	base, _ := runEvictingAgentCounters(t, evictingCase{fullySynced: true})
	if got, want := countCalls(calls, commitCall), countCalls(base, commitCall); got != want || want == 0 {
		t.Fatalf("commits %d, want %d as with no radius change, calls %v", got, want, calls)
	}
}

// TestAgentSkipsRoundAfterIncreaseThenDecrease checks that an increase
// followed by a decrease back to the depth read still skips the round:
// the chunks unreserve deleted are gone although the depth is unchanged
// (#658).
func TestAgentSkipsRoundAfterIncreaseThenDecrease(t *testing.T) {
	t.Parallel()

	_, c := runEvictingAgentCounters(t, evictingCase{fullySynced: true, stepInSample: stepUpDown})
	if c.skippedRadiusIncrease != 1 {
		t.Fatalf("skipped_radius_increase_total %v, want 1", c.skippedRadiusIncrease)
	}
	if c.playedAfterDecrease != 0 {
		t.Fatalf("played_after_radius_decrease_total %v, want 0", c.playedAfterDecrease)
	}
}

// TestAgentRadiusCountersExportedAtZero checks that both #658 counters are
// exported before any skip or decrease.
func TestAgentRadiusCountersExportedAtZero(t *testing.T) {
	t.Parallel()

	_, c := runEvictingAgentCounters(t, evictingCase{fullySynced: true})
	if c.skippedRadiusIncrease != 0 || c.playedAfterDecrease != 0 {
		t.Fatalf("counters %+v, want both 0", c)
	}
}

func countCalls(calls []contractCall, want contractCall) int {
	n := 0
	for _, c := range calls {
		if c == want {
			n++
		}
	}
	return n
}
