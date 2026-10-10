// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmd

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
)

type fakeNode struct {
	stopped  chan struct{}
	shutdown atomic.Int32
}

func newFakeNode() *fakeNode { return &fakeNode{stopped: make(chan struct{})} }

func (n *fakeNode) SyncingStopped() chan struct{} { return n.stopped }
func (n *fakeNode) Shutdown() error {
	n.shutdown.Add(1)
	return nil
}

func newTestLifecycle(build func(ctx context.Context) (runningNode, error)) (*lifecycle, chan os.Signal) {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	return &lifecycle{
		ctx:        ctx,
		cancel:     cancel,
		build:      awaitBuild(func() (runningNode, error) { return build(ctx) }),
		interrupts: sig,
		logger:     log.Noop,
	}, sig
}

// runStartStop runs start and stop as the command does on a terminal: in
// order, on one goroutine, with the first signal cancelling the context.
func runStartStop(t *testing.T, l *lifecycle, signalAfter time.Duration) {
	t.Helper()
	if signalAfter >= 0 {
		go func() {
			time.Sleep(signalAfter)
			l.cancel()
		}()
	}
	done := make(chan struct{})
	go func() {
		l.start()
		l.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("start and stop did not return")
	}
}

// A stop while the node is still being built waits for the build, and the
// cancelled build closes what it opened (wasp #635). The cancellation is not
// the command's error: the exit status stays 0.
func TestLifecycleStopDuringBuildWaitsForIt(t *testing.T) {
	var closed atomic.Bool
	l, _ := newTestLifecycle(func(ctx context.Context) (runningNode, error) {
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond) // the build's own cleanup
		closed.Store(true)
		return nil, ctx.Err()
	})
	runStartStop(t, l, 50*time.Millisecond)

	if !closed.Load() {
		t.Fatal("stop returned before the build closed the store")
	}
	if err := l.err(); err != nil {
		t.Fatalf("command error %v after a stop during startup, want none (exit 0)", err)
	}
}

// A build that returns a node just as the stop arrives: the node is shut
// down.
func TestLifecycleStopDuringBuildShutsDownALateNode(t *testing.T) {
	n := newFakeNode()
	l, _ := newTestLifecycle(func(ctx context.Context) (runningNode, error) {
		<-ctx.Done()
		return n, nil
	})
	runStartStop(t, l, 50*time.Millisecond)

	if got := n.shutdown.Load(); got != 1 {
		t.Fatalf("node shut down %d times, want 1", got)
	}
}

// A build that fails before any signal: start takes the error, stop returns
// at once, and the error is the command's result, so a configuration error
// keeps its own exit status (#490).
func TestLifecycleBuildFailsBeforeAnySignal(t *testing.T) {
	cfgErr := errors.New("bad configuration")
	l, _ := newTestLifecycle(func(ctx context.Context) (runningNode, error) {
		return nil, cfgErr
	})
	runStartStop(t, l, -1)

	if err := l.err(); !errors.Is(err, cfgErr) {
		t.Fatalf("command error %v, want the build's error", err)
	}
}

// A running node is shut down on a stop, as before.
func TestLifecycleStopShutsDownARunningNode(t *testing.T) {
	n := newFakeNode()
	l, _ := newTestLifecycle(func(ctx context.Context) (runningNode, error) { return n, nil })
	runStartStop(t, l, 100*time.Millisecond)

	if got := n.shutdown.Load(); got != 1 {
		t.Fatalf("node shut down %d times, want 1", got)
	}
	if err := l.err(); err != nil {
		t.Fatalf("command error %v, want none", err)
	}
}

// A second signal ends the wait for a build that does not finish.
func TestLifecycleSecondSignalEndsTheWait(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	l, sig := newTestLifecycle(func(ctx context.Context) (runningNode, error) {
		<-block
		return nil, ctx.Err()
	})
	go func() {
		time.Sleep(50 * time.Millisecond)
		l.cancel()
		time.Sleep(100 * time.Millisecond)
		sig <- syscall.SIGTERM
	}()
	runStartStop(t, l, -1)
}
