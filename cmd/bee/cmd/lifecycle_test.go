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
	// shutdownFn, if set, is the shutdown's work
	shutdownFn func(ctx context.Context)
}

func newFakeNode() *fakeNode { return &fakeNode{stopped: make(chan struct{})} }

func (n *fakeNode) SyncingStopped() chan struct{} { return n.stopped }
func (n *fakeNode) Shutdown(ctx context.Context) error {
	n.shutdown.Add(1)
	if n.shutdownFn != nil {
		n.shutdownFn(ctx)
	}
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

// stopWithSignals runs stop on a built node and sends the given number of
// extra signals, the first after the shutdown started. It returns how long
// stop took.
func stopWithSignals(t *testing.T, n *fakeNode, signals int, hardStop time.Duration) time.Duration {
	t.Helper()
	l, sig := newTestLifecycle(func(ctx context.Context) (runningNode, error) { return n, nil })
	l.hardStop = hardStop
	<-l.build.ready
	l.node.Store(l.build.node)

	started := make(chan struct{})
	inner := n.shutdownFn
	n.shutdownFn = func(ctx context.Context) {
		close(started)
		inner(ctx)
	}

	start := time.Now()
	done := make(chan struct{})
	go func() {
		l.stop()
		close(done)
	}()
	<-started
	for range signals {
		time.Sleep(20 * time.Millisecond)
		sig <- syscall.SIGTERM
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return")
	}
	return time.Since(start)
}

// The second signal cancels the shutdown's context: the reveal wait ends
// and the rest of the close, the state store included, still runs (#725).
func TestLifecycleSecondSignalEndsTheRevealWait(t *testing.T) {
	var stateStoreClosed atomic.Bool
	n := newFakeNode()
	n.shutdownFn = func(ctx context.Context) {
		<-ctx.Done() // the reveal wait
		time.Sleep(50 * time.Millisecond)
		stateStoreClosed.Store(true)
	}
	stopWithSignals(t, n, 1, time.Minute)

	if !stateStoreClosed.Load() {
		t.Fatal("stop returned before the state store was closed")
	}
}

// A third signal ends stop at once, for a close that hangs.
func TestLifecycleThirdSignalExits(t *testing.T) {
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	n := newFakeNode()
	n.shutdownFn = func(ctx context.Context) {
		<-ctx.Done()
		<-hang
	}
	if took := stopWithSignals(t, n, 2, time.Minute); took > 2*time.Second {
		t.Fatalf("stop took %s after the third signal", took)
	}
}

// A close still running hardStop after the second signal ends stop.
func TestLifecycleHungCloseExitsAfterSecondSignal(t *testing.T) {
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	n := newFakeNode()
	n.shutdownFn = func(ctx context.Context) {
		<-ctx.Done()
		<-hang
	}
	took := stopWithSignals(t, n, 1, 200*time.Millisecond)
	if took < 200*time.Millisecond || took > 2*time.Second {
		t.Fatalf("stop took %s, want about the hard-stop bound", took)
	}
}

// Without a signal the shutdown is not cancelled: a pending reveal is
// waited for.
func TestLifecycleOneSignalDoesNotCancelTheWait(t *testing.T) {
	n := newFakeNode()
	var cancelled atomic.Bool
	n.shutdownFn = func(ctx context.Context) {
		select {
		case <-ctx.Done():
			cancelled.Store(true)
		case <-time.After(200 * time.Millisecond): // the reveal mined
		}
	}
	stopWithSignals(t, n, 0, time.Minute)
	if cancelled.Load() {
		t.Fatal("the shutdown context was cancelled without a second signal")
	}
}
