// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"resenje.org/singleflight"
)

// TestSingleflightLeaveWhileJoin reproduces the race in
// resenje.org/singleflight v0.4.0 that #647 fixes by upgrading to v0.4.1:
// a caller returning from wait read the shared flag after unlocking, while a
// caller joining the same key wrote it under the lock. One caller keeps the
// call open, a second joins and then leaves, and a third joins as it leaves.
// It only fails under -race, on v0.4.0; repeated so one run is enough.
func TestSingleflightLeaveWhileJoin(t *testing.T) {
	for range 200 {
		var g singleflight.Group[string, int]
		block := make(chan struct{})
		fn := func(context.Context) (int, error) { <-block; return 1, nil }
		var wg sync.WaitGroup

		keep, keepCancel := context.WithCancel(context.Background())
		wg.Go(func() { _, _, _ = g.Do(keep, "k", fn) })
		time.Sleep(time.Millisecond)

		leave, leaveCancel := context.WithCancel(context.Background())
		wg.Go(func() { _, _, _ = g.Do(leave, "k", fn) })
		time.Sleep(time.Millisecond)

		wg.Go(leaveCancel)
		wg.Go(func() { _, _, _ = g.Do(context.Background(), "k", fn) })
		time.Sleep(time.Millisecond)

		close(block)
		keepCancel()
		wg.Wait()
	}
}

var peerC = swarm.MustParseHexAddress("cc00000000000000000000000000000000000000000000000000000000000000")

// TestPullsyncLeaveWhileJoinBetweenPeers reproduces the same race through
// pull-sync, between different peers sharing one collection for a bin and
// start: peer A's request leaves (its requester goes away, #641) while peer
// C's request joins, and peer B's request keeps the collection open. There
// is deliberately no synctest.Wait between the leave and the join: with one,
// the race cannot show. Probabilistic: on v0.4.0 one iteration caught it in
// about 1 run in 10, so it repeats 100 times.
func TestPullsyncLeaveWhileJoinBetweenPeers(t *testing.T) {
	for range 100 {
		synctest.Test(t, func(t *testing.T) {
			server, _ := newPullSync(t, nil, 5, mock.WithSubscribeResp(nil, nil))
			clientA := newClient(t, server, peerA)
			clientB := newClient(t, server, peerB)
			clientC := newClient(t, server, peerC)
			ctxA, cancelA := context.WithCancel(context.Background())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			a := startSync(ctxA, clientA, 22, 240)
			b := startSync(ctx, clientB, 22, 240)
			synctest.Wait()

			cancelA()
			c := startSync(ctx, clientC, 22, 240)
			<-a
			synctest.Wait()

			cancel()
			<-b
			<-c
		})
	}
}
