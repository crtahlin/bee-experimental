// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// TestListenerConfirmationDepth: the listener syncs up to the configured
// number of blocks behind the head (rounded down to its step), not a fixed 4
// (#545).
func TestListenerConfirmationDepth(t *testing.T) {
	t.Parallel()

	const head = uint64(100)

	for _, tc := range []struct {
		depth uint64
		want  uint64
	}{
		{depth: listener.DefaultConfirmationDepth, want: 95}, // 100 - 4 = 96, down to 95
		{depth: 12, want: 85},                                // 100 - 12 = 88, down to 85
	} {
		t.Run(fmt.Sprintf("depth %d", tc.depth), func(t *testing.T) {
			t.Parallel()

			ev := newEventUpdaterMock()
			mf := newMockFilterer(WithBlockNumber(head))
			l := listener.New(nil, log.Noop, mf, postageStampContractAddress, postageStampContractABI,
				func() time.Duration { return 1 }, tc.depth, stallingTimeout, backoffTime)
			testutil.CleanupCloser(t, l)
			<-l.Listen(context.Background(), 0, ev)

			select {
			case e := <-ev.eventC:
				e.(blockNumberCall).compareF(t, tc.want)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for block number update")
			}
		})
	}
}
