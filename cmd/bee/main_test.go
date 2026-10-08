// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/node"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
)

// TestExitCode is wasp #490.
//
// The status is the whole mechanism: packaging/bee.service sets
// RestartPreventExitStatus=78, so whether a node stops or loops every five
// seconds is decided here. Before this, a failed build was logged and the
// process exited 0, systemd reported "Deactivated successfully", and
// Restart=always looped while systemctl is-active still said active.
//
// Tested on the mapping rather than by running the binary: the binary takes
// minutes to build and start and would not make the mapping any more certain.
func TestExitCode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want int
		why  string
	}{
		{
			name: "success",
			err:  nil,
			want: 0,
			why:  "a clean shutdown must not look like a failure",
		},
		{
			name: "config error",
			err:  fmt.Errorf("%w: stake-recovery-on-startup %q", node.ErrConfig, "false"),
			want: exitConfig,
			why:  "the unit refuses to restart on this, so a bad value stops the node",
		},
		{
			name: "config error wrapped further",
			err:  fmt.Errorf("starting node: %w", fmt.Errorf("%w: bad value", node.ErrConfig)),
			want: exitConfig,
			why:  "the sentinel must survive wrapping, since NewBee's callers add context",
		},
		{
			name: "postage stall shutdown",
			err:  fmt.Errorf("node stopped: %w", listener.ErrPostageSyncingStalled),
			want: exitTempFail,
			why:  "postage-stall-shutdown reached is a cause that may pass, so a restart is worth trying (#583)",
		},
		{
			name: "postage contract paused at runtime",
			err:  fmt.Errorf("node stopped: %w", listener.ErrPostagePaused),
			want: exitTempFail,
			why:  "a paused contract can be unpaused (#583)",
		},
		{
			name: "postage contract paused at startup",
			err:  fmt.Errorf("check postage contract: %w", listener.ErrPostagePaused),
			want: exitTempFail,
			why:  "the startup check uses the same sentinel, so both paths give one status (#583)",
		},
		{
			name: "postage events could not be applied",
			err:  fmt.Errorf("node stopped: %w", errors.New("update block number: disk full")),
			want: 1,
			why:  "a fault in the node or its data stays a plain failure (#583)",
		},
		{
			name: "any other failure",
			err:  errors.New("dial tcp: connection refused"),
			want: 1,
			why: "must stay 1 so the unit still restarts it: a node whose chain " +
				"endpoint is briefly down has to come back on its own",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := exitCode(tc.err); got != tc.want {
				t.Fatalf("exitCode(%v) = %d, want %d: %s", tc.err, got, tc.want, tc.why)
			}
		})
	}
}

// TestExitConfigIsNotAmbiguous guards the constant itself. 1 would make every
// failure unrecoverable, and anything at or above 128 collides with the
// signal-derived range systemd and shells use.
func TestExitConfigIsNotAmbiguous(t *testing.T) {
	t.Parallel()

	if exitConfig == 0 || exitConfig == 1 {
		t.Fatalf("exitConfig is %d, which is not distinguishable from success or "+
			"an ordinary failure", exitConfig)
	}
	if exitConfig >= 128 {
		t.Fatalf("exitConfig is %d, which collides with the 128+signal range", exitConfig)
	}
}
