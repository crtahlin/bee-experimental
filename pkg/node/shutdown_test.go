// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
)

// shutdownLog records the order in which Shutdown reaches things.
type shutdownLog struct {
	mu     sync.Mutex
	events []string
}

func (l *shutdownLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *shutdownLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

type recordingCloser struct {
	log    *shutdownLog
	name   string
	closed chan struct{}
}

func (c *recordingCloser) Close() error {
	c.log.add(c.name)
	if c.closed != nil {
		close(c.closed)
	}
	return nil
}

// fakeRevealGuard has a pending reveal whose wait lasts until the
// localstore is closed, so a localstore closed only after the wait shows
// as a wait timeout.
type fakeRevealGuard struct {
	log             *shutdownLog
	localstoreClose <-chan struct{}
}

func (g *fakeRevealGuard) BeginShutdown()            { g.log.add("refuse commits") }
func (g *fakeRevealGuard) WaitBudget() time.Duration { return time.Minute }
func (g *fakeRevealGuard) Close() error              { g.log.add("revealer"); return nil }
func (g *fakeRevealGuard) Pending() (uint64, uint64, bool) {
	return 1, 2, true
}

func (g *fakeRevealGuard) Wait(ctx context.Context) error {
	g.log.add("wait start")
	select {
	case <-g.localstoreClose:
	case <-time.After(2 * time.Second):
		g.log.add("wait timeout")
	}
	g.log.add("wait end")
	return nil
}

func newShutdownTestBee(l *shutdownLog, waitForReveal bool) *Bee {
	rec := func(name string) *recordingCloser { return &recordingCloser{log: l, name: name} }
	localstore := rec("localstore")
	localstore.closed = make(chan struct{})
	return &Bee{
		logger:                   log.Noop,
		ctxCancel:                func() {},
		apiCloser:                rec("api"),
		pusherCloser:             rec("pusher"),
		p2pService:               rec("p2p"),
		storageIncetivesCloser:   rec("agent"),
		localstoreCloser:         localstore,
		transactionMonitorCloser: rec("monitor"),
		transactionCloser:        rec("transaction"),
		listenerCloser:           rec("listener"),
		ethClientCloser:          func() { l.add("chain client") },
		tracerCloser:             rec("tracer"),
		stateStoreCloser:         rec("statestore"),
		revealGuard:              &fakeRevealGuard{log: l, localstoreClose: localstore.closed},
		stopWaitForReveal:        waitForReveal,
	}
}

func indexOf(t *testing.T, events []string, e string) int {
	t.Helper()
	i := slices.Index(events, e)
	if i < 0 {
		t.Fatalf("%q not reached; events %v", e, events)
	}
	return i
}

// Shutdown order (test 6): commits refused first; the localstore closes
// while the reveal is awaited; the transaction monitor, the transaction
// service and the chain client close after the wait; the state store last.
func TestShutdownOrder(t *testing.T) {
	t.Parallel()

	l := &shutdownLog{}
	b := newShutdownTestBee(l, true)
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev := l.list()

	if ev[0] != "refuse commits" {
		t.Fatalf("first step %q, want commits refused; events %v", ev[0], ev)
	}
	if slices.Contains(ev, "wait timeout") {
		t.Fatalf("the localstore was not closed during the wait; events %v", ev)
	}
	waitStart, waitEnd := indexOf(t, ev, "wait start"), indexOf(t, ev, "wait end")
	if ls := indexOf(t, ev, "localstore"); ls > waitEnd {
		t.Fatalf("localstore closed after the wait; events %v", ev)
	}
	for _, e := range []string{"api", "pusher", "p2p"} {
		if indexOf(t, ev, e) > waitStart {
			t.Fatalf("%s closed after the wait started; events %v", e, ev)
		}
	}
	for _, e := range []string{"revealer", "monitor", "transaction", "chain client"} {
		if indexOf(t, ev, e) < waitEnd {
			t.Fatalf("%s closed before the wait ended; events %v", e, ev)
		}
	}
	if ev[len(ev)-1] != "statestore" {
		t.Fatalf("last step %q, want the state store; events %v", ev[len(ev)-1], ev)
	}
}

// The setting off: no wait; the rest as with it on (test 14).
func TestShutdownWithoutRevealWait(t *testing.T) {
	t.Parallel()

	l := &shutdownLog{}
	b := newShutdownTestBee(l, false)
	if err := b.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	ev := l.list()
	if slices.Contains(ev, "wait start") {
		t.Fatalf("waited with the setting off; events %v", ev)
	}
	if ev[0] != "refuse commits" || ev[len(ev)-1] != "statestore" {
		t.Fatalf("events %v", ev)
	}
}
