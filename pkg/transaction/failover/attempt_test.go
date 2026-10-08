// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package failover_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	"github.com/ethersphere/bee/v2/pkg/transaction/failover"
)

// hang blocks until the attempt's context ends, like an endpoint that accepts
// the request and never answers.
func hang(ctx context.Context) (uint64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestAttemptDeadlineReachesNextEndpoint checks that a caller deadline is
// shared out: the first endpoint hangs and uses its share, and the second is
// still asked and answers within the caller's deadline (#583).
func TestAttemptDeadlineReachesNextEndpoint(t *testing.T) {
	defer failover.SetMinAttemptTimeout(10 * time.Millisecond)()

	var secondCalls atomic.Int32
	b := newBackend(t,
		endpoint("first", backendmock.WithBlockNumberFunc(hang)),
		endpoint("second", backendmock.WithBlockNumberFunc(func(context.Context) (uint64, error) {
			secondCalls.Add(1)
			return 7, nil
		})),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	n, err := b.BlockNumber(ctx)
	if err != nil {
		t.Fatalf("got error %v, want the second endpoint's answer", err)
	}
	if n != 7 || secondCalls.Load() != 1 {
		t.Fatalf("got block %d after %d calls to the second endpoint, want 7 after 1", n, secondCalls.Load())
	}
	if took := time.Since(start); took >= 2*time.Second {
		t.Fatalf("took %v, the caller's whole deadline; the first attempt should use only its share", took)
	}
	if b.Active() != "second" {
		t.Fatalf("active endpoint is %q, want second", b.Active())
	}
}

// TestNoAdvanceAfterCallerDeadline checks that an attempt ended by the
// caller's own deadline does not move the shared active endpoint (#583).
func TestNoAdvanceAfterCallerDeadline(t *testing.T) {
	defer failover.SetMinAttemptTimeout(10 * time.Second)()

	var secondCalls atomic.Int32
	b := newBackend(t,
		endpoint("first", backendmock.WithBlockNumberFunc(hang)),
		endpoint("second", backendmock.WithBlockNumberFunc(func(context.Context) (uint64, error) {
			secondCalls.Add(1)
			return 7, nil
		})),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := b.BlockNumber(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got error %v, want the caller's deadline", err)
	}
	if b.Active() != "first" {
		t.Fatalf("active endpoint is %q, want first: the caller's deadline is not the endpoint's failure", b.Active())
	}
	if secondCalls.Load() != 0 {
		t.Fatalf("second endpoint was called %d times after the caller's deadline", secondCalls.Load())
	}
}

// TestAttemptWithoutCallerDeadline checks that a caller without a deadline
// keeps today's behaviour: the attempt runs on the caller's context.
func TestAttemptWithoutCallerDeadline(t *testing.T) {
	t.Parallel()

	var sawDeadline atomic.Bool
	b := newBackend(t,
		endpoint("only", backendmock.WithBlockNumberFunc(func(ctx context.Context) (uint64, error) {
			_, ok := ctx.Deadline()
			sawDeadline.Store(ok)
			return 1, nil
		})),
	)
	if _, err := b.BlockNumber(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sawDeadline.Load() {
		t.Fatal("an attempt for a caller without a deadline got one")
	}
}
