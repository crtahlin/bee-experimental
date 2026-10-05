// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mock

import (
	"context"
	"sync"
)

// Syncer is a mock of the puller as the reserve worker sees it: a
// historical sync rate, and whether the historical sync for a radius is
// done, with a signal when that may have changed.
type Syncer struct {
	mtx       sync.Mutex
	rate      float64
	done      bool
	noNeigh   bool
	doneCalls int
	changed   chan struct{}
}

// NewMockSyncer returns a Syncer that reports the given historical sync
// rate and the given answer to HistoricalSyncDone for every radius.
func NewMockSyncer(rate float64, historicalSyncDone bool) *Syncer {
	return &Syncer{rate: rate, done: historicalSyncDone, changed: make(chan struct{}, 1)}
}

func (m *Syncer) SyncRate() float64 {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.rate
}

func (m *Syncer) Start(context.Context) {}

func (m *Syncer) HistoricalSyncDone(uint8) bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.doneCalls++
	return m.done
}

func (m *Syncer) HistoricalSyncChanged() <-chan struct{} { return m.changed }

// HasNeighbour reports true unless SetHasNeighbour(false) was called.
func (m *Syncer) HasNeighbour(uint8) bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return !m.noNeigh
}

// SetHasNeighbour changes the answer of HasNeighbour.
func (m *Syncer) SetHasNeighbour(has bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.noNeigh = !has
}

// SetSyncRate changes the reported historical sync rate.
func (m *Syncer) SetSyncRate(r float64) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.rate = r
}

// SetHistoricalSyncDone changes the answer of HistoricalSyncDone. It does
// not signal; call Signal for that.
func (m *Syncer) SetHistoricalSyncDone(done bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.done = done
}

// Signal sends on the HistoricalSyncChanged channel without blocking, as
// the puller does.
func (m *Syncer) Signal() {
	select {
	case m.changed <- struct{}{}:
	default:
	}
}

// HistoricalSyncDoneCalls returns how often HistoricalSyncDone was called.
func (m *Syncer) HistoricalSyncDoneCalls() int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.doneCalls
}
