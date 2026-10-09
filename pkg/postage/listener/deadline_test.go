// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// hangingFilterer accepts every call and answers none until the call's own
// context ends, like an endpoint that never responds. It records how each
// call ended.
type hangingFilterer struct {
	mu    sync.Mutex
	ended []error
	hangF bool // FilterLogs hangs too; BlockNumber answers when false
}

func (h *hangingFilterer) record(err error) {
	h.mu.Lock()
	h.ended = append(h.ended, err)
	h.mu.Unlock()
}

func (h *hangingFilterer) BlockNumber(ctx context.Context) (uint64, error) {
	if h.hangF {
		return 100_000, nil
	}
	<-ctx.Done()
	h.record(ctx.Err())
	return 0, ctx.Err()
}

func (h *hangingFilterer) FilterLogs(ctx context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
	<-ctx.Done()
	h.record(ctx.Err())
	return nil, ctx.Err()
}

func (h *hangingFilterer) endings() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]error(nil), h.ended...)
}

func runHanging(t *testing.T, h *hangingFilterer) {
	t.Helper()
	defer listener.SetCallTimeouts(20*time.Millisecond, 20*time.Millisecond)()

	// Cleanups run last first: the listener closes before the reader below
	// stops, so a listener reporting on its synced channel is still read.
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })

	l := listener.New(
		nil,
		log.Noop,
		h,
		postageStampContractAddress,
		postageStampContractABI,
		func() time.Duration { return time.Millisecond },
		listener.DefaultConfirmationDepth,
		time.Hour,
		time.Millisecond,
	)
	testutil.CleanupCloser(t, l)
	synced := l.Listen(context.Background(), 0, quietUpdater{})
	go func() {
		select {
		case <-synced:
		case <-done:
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for len(h.endings()) < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("a hanging call was ended %d times in 5 s, want at least 3", len(h.endings()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i, err := range h.endings() {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("call %d ended with %v, want its own deadline", i, err)
		}
	}
}

// TestBlockNumberCallHasDeadline checks that a BlockNumber call that never
// answers ends at its deadline and the listener tries again, instead of
// blocking the loop (#583).
func TestBlockNumberCallHasDeadline(t *testing.T) {
	runHanging(t, &hangingFilterer{})
}

// TestFilterLogsCallHasDeadline checks the same for FilterLogs.
func TestFilterLogsCallHasDeadline(t *testing.T) {
	runHanging(t, &hangingFilterer{hangF: true})
}
