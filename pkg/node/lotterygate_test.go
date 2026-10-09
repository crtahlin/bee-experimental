// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node_test

import (
	"context"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/node"
	"github.com/ethersphere/bee/v2/pkg/postage"
)

// listenerHealth is a postage.Listener that also reports sync health.
type listenerHealth struct{ stale bool }

func (listenerHealth) Close() error { return nil }
func (listenerHealth) Listen(context.Context, uint64, postage.EventUpdater) <-chan error {
	return nil
}
func (l listenerHealth) Stale() bool                { return l.stale }
func (listenerHealth) SinceProgress() time.Duration { return 0 }
func (listenerHealth) StaleEndedAt() time.Time      { return time.Time{} }
func (l listenerHealth) CaughtUp() bool             { return !l.stale }

// plainListener reports no sync health.
type plainListener struct{}

func (plainListener) Close() error { return nil }
func (plainListener) Listen(context.Context, uint64, postage.EventUpdater) <-chan error {
	return nil
}

// TestPostageReadyForLottery checks that the lottery gate closes while the
// batch store is stale (#583), and stays open for a listener that reports
// no sync health and when there is no listener.
func TestPostageReadyForLottery(t *testing.T) {
	t.Parallel()

	if node.PostageReadyForLottery(listenerHealth{stale: true}) {
		t.Fatal("lottery gate open while the batch store is stale")
	}
	if !node.PostageReadyForLottery(listenerHealth{stale: false}) {
		t.Fatal("lottery gate closed while the batch store is current")
	}
	if !node.PostageReadyForLottery(plainListener{}) {
		t.Fatal("lottery gate closed for a listener without sync health")
	}
	if !node.PostageReadyForLottery(nil) {
		t.Fatal("lottery gate closed without a listener")
	}
}
