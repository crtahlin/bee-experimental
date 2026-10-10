// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmd

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
)

// runningNode is what the start command needs of a built node. *node.Bee
// implements it; tests use a fake.
type runningNode interface {
	SyncingStopped() chan struct{}
	Shutdown(ctx context.Context) error
}

// defaultHardStop is how long the close may run after the second signal
// before the process exits anyway.
const defaultHardStop = 30 * time.Second

// buildResult owns the single value the background build sends. A
// goroutine receives it once and publishes it, so start and stop can both
// wait for it without competing for the channel: whoever waits second still
// sees it, and nobody waits on a value already taken (wasp #635; the
// Windows service runs start and stop on different goroutines).
type buildResult struct {
	ready chan struct{}
	node  runningNode
	err   error
}

// awaitBuild starts the goroutine that receives the build's result once,
// with recv, and publishes it.
func awaitBuild(recv func() (runningNode, error)) *buildResult {
	r := &buildResult{ready: make(chan struct{})}
	go func() {
		r.node, r.err = recv()
		if r.err != nil {
			r.node = nil
		}
		close(r.ready)
	}()
	return r
}

// lifecycle is the start command's start and stop, separated from the
// command so the order of events is testable.
type lifecycle struct {
	ctx        context.Context
	cancel     context.CancelFunc
	build      *buildResult
	interrupts <-chan os.Signal
	logger     log.Logger
	// hardStop is how long the close may run after the second signal;
	// defaultHardStop when zero.
	hardStop time.Duration

	node     atomic.Value // runningNode, once built
	buildErr atomic.Value // error of a build that failed before any stop
}

// start waits for the build and then for the node to stop or a signal.
func (l *lifecycle) start() {
	select {
	case <-l.build.ready:
		if l.build.err != nil {
			// A build that failed before any stop: its error is the
			// command's result, so main can map a configuration error to
			// its own exit status (#490).
			l.logger.Error(l.build.err, "failed to build bee node")
			l.buildErr.Store(l.build.err)
			return
		}
		l.node.Store(l.build.node)
	case <-l.ctx.Done():
		return
	}

	select {
	case <-l.ctx.Done():
	case <-l.node.Load().(runningNode).SyncingStopped():
		l.logger.Debug("syncing has stopped")
	}

	l.logger.Info("shutting down...")
}

// stop cancels the context and shuts the node down. A stop that arrives
// while the node is still being built waits for the build: the cancelled
// build then closes what it opened, or returns a node that is shut down
// here. Without the wait the process exited with the store open and its
// dirty marker in place, and the next start ran a full recovery (wasp
// #635). A second signal during the build ends the wait at once.
//
// Once the node is built, the second signal cancels the shutdown's
// context: a wait for a storage-lottery reveal ends and the rest still
// closes, the state store included. A third signal, or a close still
// running hardStop after the second, ends stop at once (#725).
func (l *lifecycle) stop() {
	l.cancel()

	if l.node.Load() == nil {
		select {
		case <-l.build.ready:
		default:
			l.logger.Info("stopped during startup; waiting for the node build to close the store")
			select {
			case <-l.build.ready:
			case <-l.interrupts:
				l.logger.Info("node shutdown terminated")
				return
			}
		}
		if l.build.err != nil {
			// The stop is the cause of the build's end, or the start
			// already stored a real failure; either way the stop does not
			// turn it into the command's error.
			if l.buildErr.Load() == nil {
				l.logger.Info("node build ended during the stop", "error", l.build.err)
			}
			return
		}
		l.node.Store(l.build.node)
	}

	n := l.node.Load().(runningNode)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := n.Shutdown(ctx); err != nil {
			l.logger.Error(err, "shutdown failed")
		}
	}()

	select {
	case <-done:
		l.logger.Info("node shutdown")
		return
	case <-l.interrupts:
		l.logger.Info("second signal: not waiting for a storage-lottery reveal; closing the rest")
		cancel()
	}

	hardStop := l.hardStop
	if hardStop == 0 {
		hardStop = defaultHardStop
	}
	select {
	case <-done:
		l.logger.Info("node shutdown")
	case <-l.interrupts:
		l.logger.Info("node shutdown terminated")
	case <-time.After(hardStop):
		l.logger.Info("node shutdown terminated: the close is still running", "after", hardStop)
	}
}

// err returns the error of a build that failed before any stop.
func (l *lifecycle) err() error {
	if err, ok := l.buildErr.Load().(error); ok {
		return err
	}
	return nil
}

// built returns the node, or nil if it was never built.
func (l *lifecycle) built() runningNode {
	if n, ok := l.node.Load().(runningNode); ok {
		return n
	}
	return nil
}
