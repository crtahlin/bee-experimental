// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// runCancelled starts a listener on h, cancels its context while a backend
// call hangs, and returns what the synced channel reports. The calls' own
// deadlines are an hour, so only the cancel can end them.
func runCancelled(t *testing.T, h *hangingFilterer) error {
	t.Helper()
	defer listener.SetCallTimeouts(time.Hour, time.Hour)()

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

	ctx, cancel := context.WithCancel(context.Background())
	synced := l.Listen(ctx, 0, quietUpdater{})
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-synced:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("synced channel silent 5 s after the cancel: a caller waiting for the startup sync would hang (#757)")
		return nil
	}
}

// TestCancelDuringBlockNumberReportsCancel checks that a cancel during a
// BlockNumber call reaches the synced channel as a cancellation, never as
// nil, which would report the stopped sync as synced (#757).
func TestCancelDuringBlockNumberReportsCancel(t *testing.T) {
	err := runCancelled(t, &hangingFilterer{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("synced reported %v, want context.Canceled", err)
	}
}

// TestCancelDuringFilterLogsReportsCancel checks the same for a cancel
// during a FilterLogs call.
func TestCancelDuringFilterLogsReportsCancel(t *testing.T) {
	err := runCancelled(t, &hangingFilterer{hangF: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("synced reported %v, want context.Canceled", err)
	}
}
