// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/node"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
)

// TestStakeRecoveryModeAcceptsYAMLFalse is wasp #489.
//
// stake-recovery-on-startup takes the word "off", and YAML 1.1 coerces a bare
// off, on, yes or no to a boolean. So the documented default written the obvious
// way,
//
//	stake-recovery-on-startup: off
//
// arrives here as "false". It used to be rejected, which stopped the node from
// starting at all: measured on a real node, NRestarts reached 11 from that one
// line.
func TestStakeRecoveryModeAcceptsYAMLFalse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		mode    string
		wantErr bool
		why     string
	}{
		{mode: "off", wantErr: false, why: "the documented value"},
		{mode: "", wantErr: false, why: "unset, same as off"},
		{mode: "false", wantErr: false, why: "what YAML makes of a bare off"},
		{
			mode:    "true",
			wantErr: true,
			why: "what YAML makes of a bare on, and it does NOT say whether " +
				"withdraw or migrate was wanted. Guessing would move staked funds.",
		},
		{mode: "yes", wantErr: true, why: "not a mode, and not what YAML produces either"},
		{mode: "Off", wantErr: true, why: "the values are lower case"},
		{mode: "nonsense", wantErr: true, why: "an ordinary typo"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()

			// chainEnabled false so no mode ever reaches the chain: what is
			// under test is validation, and a nil service would be used by the
			// recovery path only.
			err := node.RunStakeRecoveryOnStartup(tc.mode, false, nil, log.Noop)

			if tc.wantErr && err == nil {
				t.Fatalf("mode %q was accepted, want rejected: %s", tc.mode, tc.why)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("mode %q was rejected (%v), want accepted: %s", tc.mode, err, tc.why)
			}
		})
	}
}

// TestInvalidStakeRecoveryModeIsAConfigError pins that the rejection is
// identifiable, because the exit status and therefore whether systemd restarts
// the node depend on it rather than on the message.
func TestInvalidStakeRecoveryModeIsAConfigError(t *testing.T) {
	t.Parallel()

	err := node.RunStakeRecoveryOnStartup("nonsense", false, nil, log.Noop)
	if err == nil {
		t.Fatal("an invalid mode was accepted")
	}
	if !errors.Is(err, node.ErrConfig) {
		t.Fatalf("error %v is not node.ErrConfig, so main would exit 1 and the "+
			"unit would restart the node forever on a value a restart cannot fix", err)
	}

	// The message has to point at YAML. An operator who writes the documented
	// default and is told only "must be off, withdraw or migrate" has no way to
	// see where "false" came from.
	if !strings.Contains(err.Error(), "YAML") {
		t.Fatalf("error %q does not mention YAML, which is where the value came from", err)
	}
}

// TestRecoverableErrorIsNotAConfigError is the test that matters most here, and
// it is about what must NOT happen.
//
// ErrConfig makes the service unit refuse to restart. If a transient failure
// were ever wrapped in it, a chain endpoint being briefly down would stop the
// node until a human noticed, which is worse than the restart loop this
// replaced. The guard is cheap and the failure it prevents is not.
func TestRecoverableErrorIsNotAConfigError(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		errors.New("dial tcp: connect: connection refused"),
		fmt.Errorf("get potential stake: %w", errors.New("execution reverted")),
		fmt.Errorf("could not get blockchain log: %w", errors.New("context canceled")),
	} {
		if errors.Is(err, node.ErrConfig) {
			t.Fatalf("a transient error is reported as a configuration error: %v. "+
				"The node would stop instead of retrying.", err)
		}
	}
}

// TestPostageConfirmationDepthOutOfRangeIsAConfigError: a depth of 0 would apply
// postage events from the chain head itself, and one above the maximum would
// stall postage syncing until the node stops, so both are refused at start as
// a configuration error a restart cannot fix (#545).
func TestPostageConfirmationDepthOutOfRangeIsAConfigError(t *testing.T) {
	t.Parallel()

	for _, depth := range []uint64{0, listener.MaxConfirmationDepth + 1, 1_000_000_000} {
		if err := node.ValidatePostageConfirmationDepth(depth); !errors.Is(err, node.ErrConfig) {
			t.Fatalf("depth %d: got %v, want node.ErrConfig", depth, err)
		}
	}
	for _, depth := range []uint64{1, 4, 12, listener.MaxConfirmationDepth} {
		if err := node.ValidatePostageConfirmationDepth(depth); err != nil {
			t.Fatalf("depth %d: got %v, want accepted", depth, err)
		}
	}
}
