// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// actedSyncer is a storer syncer whose sync rate and reported acted-on radius
// a test sets. On a tree without #696 ActedOnRadius is simply never called.
type actedSyncer struct {
	mu     sync.Mutex
	rate   float64
	ok     bool
	radius uint8
	since  time.Time
}

func (s *actedSyncer) SyncRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rate
}

func (s *actedSyncer) Start(context.Context) {}

func (s *actedSyncer) ActedOnRadius() (uint8, time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.radius, s.since, s.ok
}

func (s *actedSyncer) set(ok bool, radius uint8, since time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ok, s.radius, s.since = ok, radius, since
}

// TestRadiusDoesNotDropTwiceWithoutSync reproduces #696: with the reserve
// under half capacity and a sync rate of 0, the reserve worker lowered the
// radius at every check, although the puller had not synced at the current
// radius at all. The syncer here reports that it has not acted on any
// radius; the radius must not fall more than one step below where it
// started.
func TestRadiusDoesNotDropTwiceWithoutSync(t *testing.T) {
	t.Parallel()

	const start = 4
	baseAddr := swarm.RandAddress(t)
	st, err := memStorer(t, dbTestOps(baseAddr, 10, nil, nil, 50*time.Millisecond))()
	if err != nil {
		t.Fatal(err)
	}

	syncer := &actedSyncer{}
	readyC := make(chan struct{})
	st.StartReserveWorker(context.Background(), syncer, networkRadiusFunc(start), readyC)
	<-readyC

	time.Sleep(time.Second) // about 20 checks
	if r := st.Reserve().Radius(); r < start-1 {
		t.Fatalf("radius %d after 20 checks with no sync at any radius, want at least %d", r, start-1)
	}
}
