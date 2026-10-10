// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transaction

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// ErrClosed is returned by a store call made after the store stopped
// admitting work. The storer exports it as ErrDBQuit, so every caller that
// already treats a shutdown as such handles it (wasp #634).
var ErrClosed = errors.New("db quit")

// gate admits units of store work and lets Close wait until the units
// already admitted have left, without a lock: store calls nest inside
// iteration callbacks, and a lock with a waiting writer would block the
// nested reader (wasp #634).
//
// A caller increments inside and then re-checks closing; if closing is set
// it leaves at once with ErrClosed. Close sets closing and waits until
// inside reaches zero. Nothing ever waits to enter, so nothing can deadlock.
type gate struct {
	closing atomic.Bool
	inside  atomic.Int64
	idle    chan struct{}
	once    sync.Once
}

func newGate() *gate {
	return &gate{idle: make(chan struct{})}
}

// enter admits one unit of work. It returns false when the store is
// closing; the caller must not touch the store then.
func (g *gate) enter() bool {
	g.inside.Add(1)
	if g.closing.Load() {
		g.leave()
		return false
	}
	return true
}

// leave ends a unit admitted by enter (or refused by it).
func (g *gate) leave() {
	if g.inside.Add(-1) == 0 && g.closing.Load() {
		g.once.Do(func() { close(g.idle) })
	}
}

// stopped reports whether the store stopped admitting work. Iteration
// callbacks check it, so a long scan ends at its next item.
func (g *gate) stopped() bool {
	return g.closing.Load()
}

// gateWaitLog is how long close waits before it logs that it is still
// waiting for admitted work. A variable so a test can shorten it.
var gateWaitLog = 3 * time.Second

// close stops admitting work and waits until every admitted unit has left.
// still is called once, with the number of units inside, if the wait lasts
// longer than gateWaitLog.
func (g *gate) close(still func(inside int64)) {
	g.closing.Store(true)
	if g.inside.Load() == 0 {
		g.once.Do(func() { close(g.idle) })
	}
	t := time.NewTimer(gateWaitLog)
	defer t.Stop()
	select {
	case <-g.idle:
		return
	case <-t.C:
		if still != nil {
			still(g.inside.Load())
		}
	}
	<-g.idle
}

// closedTransaction is what NewTransaction returns once the store stopped
// admitting work: every call returns ErrClosed and nothing reaches the
// store.
type closedTransaction struct{}

func (closedTransaction) ChunkStore() storage.ChunkStore { return closedChunkStore{} }
func (closedTransaction) IndexStore() storage.IndexStore { return closedIndexStore{} }
func (closedTransaction) Commit() error                  { return ErrClosed }

type closedIndexStore struct{}

func (closedIndexStore) Get(storage.Item) error                         { return ErrClosed }
func (closedIndexStore) Has(storage.Key) (bool, error)                  { return false, ErrClosed }
func (closedIndexStore) GetSize(storage.Key) (int, error)               { return 0, ErrClosed }
func (closedIndexStore) Iterate(storage.Query, storage.IterateFn) error { return ErrClosed }
func (closedIndexStore) Count(storage.Key) (int, error)                 { return 0, ErrClosed }
func (closedIndexStore) Put(storage.Item) error                         { return ErrClosed }
func (closedIndexStore) Delete(storage.Item) error                      { return ErrClosed }

type closedChunkStore struct{}

func (closedChunkStore) Get(context.Context, swarm.Address) (swarm.Chunk, error) {
	return nil, ErrClosed
}
func (closedChunkStore) Has(context.Context, swarm.Address) (bool, error)      { return false, ErrClosed }
func (closedChunkStore) Put(context.Context, swarm.Chunk) error                { return ErrClosed }
func (closedChunkStore) Delete(context.Context, swarm.Address) error           { return ErrClosed }
func (closedChunkStore) Iterate(context.Context, storage.IterateChunkFn) error { return ErrClosed }
func (closedChunkStore) Replace(context.Context, swarm.Chunk, bool) error      { return ErrClosed }
