// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/spf13/cobra"
)

// streamLimitsFor builds the start command's real flag set, parses args,
// loads the config as the start command does, binds the flags, and
// returns what start.go passes to the node (#638).
func streamLimitsFor(t *testing.T, cfgBody string, args ...string) libp2p.StreamLimitOptions {
	t.Helper()

	c := &command{homeDir: t.TempDir()}
	if cfgBody != "" {
		p := filepath.Join(t.TempDir(), "bee.yaml")
		if err := os.WriteFile(p, []byte(cfgBody), 0o600); err != nil {
			t.Fatal(err)
		}
		c.cfgFile = p
	}
	cmd := &cobra.Command{Use: "start"}
	c.setAllFlags(cmd)
	if err := cmd.Flags().Parse(args); err != nil {
		t.Fatal(err)
	}
	if err := c.initConfig(); err != nil {
		t.Fatal(err)
	}
	if err := c.config.BindPFlags(cmd.Flags()); err != nil {
		t.Fatal(err)
	}
	return inboundStreamLimits(c.config)
}

func TestInboundStreamLimitSettings(t *testing.T) {
	t.Run("unset: off, every value the built-in default", func(t *testing.T) {
		if got := streamLimitsFor(t, ""); got != (libp2p.StreamLimitOptions{}) {
			t.Fatalf("got %+v, want all zero", got)
		}
	})
	t.Run("switch and values from the config file", func(t *testing.T) {
		got := streamLimitsFor(t, "p2p-inbound-stream-limits: true\np2p-inbound-streams-per-peer: 512\np2p-inbound-stream-reserve-pullsync: -1\n")
		want := libp2p.StreamLimitOptions{Enabled: true, PerPeer: 512, ReservePullSync: -1}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
	t.Run("environment", func(t *testing.T) {
		t.Setenv("BEE_P2P_INBOUND_STREAM_LIMITS", "true")
		t.Setenv("BEE_P2P_INBOUND_STREAMS_UNNEGOTIATED_PER_PEER", "96")
		got := streamLimitsFor(t, "")
		if !got.Enabled || got.UnnegotiatedPerPeer != 96 {
			t.Fatalf("got %+v, want on with 96 un-negotiated per peer", got)
		}
	})
	t.Run("flags", func(t *testing.T) {
		got := streamLimitsFor(t, "", "--p2p-inbound-stream-limits", "--p2p-inbound-streams-transient=2048", "--p2p-inbound-stream-reserve-other=-1")
		want := libp2p.StreamLimitOptions{Enabled: true, Transient: 2048, ReserveOther: -1}
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	})
}
