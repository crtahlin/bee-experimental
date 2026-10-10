// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// TestGetCursorsDeadline checks that a cursors request to a peer that never
// answers returns at the caller's deadline, so the deadline the puller sets
// (#695) bounds the whole request, the read included.
func TestGetCursorsDeadline(t *testing.T) {
	t.Parallel()

	server, _ := newPullSync(t, nil, 0)
	spec := server.Protocol()

	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	for i := range spec.StreamSpecs {
		if spec.StreamSpecs[i].Name == "cursors" {
			spec.StreamSpecs[i].Handler = func(ctx context.Context, _ p2p.Peer, _ p2p.Stream) error {
				select {
				case <-stuck:
				case <-ctx.Done():
				}
				return nil
			}
		}
	}

	recorder := streamtest.New(streamtest.WithProtocols(spec))
	client, _ := newPullSync(t, recorder, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err := client.GetCursors(ctx, swarm.RandAddress(t))
	took := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %v, want %v", err, context.DeadlineExceeded)
	}
	if took > 5*time.Second {
		t.Fatalf("returned after %v, want about the 200ms deadline", took)
	}
}
