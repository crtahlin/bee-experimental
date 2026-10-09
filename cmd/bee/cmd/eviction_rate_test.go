// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
)

// evictionRateFor builds the start command's real flag set, parses args,
// loads the config the way the start command does, binds the flags as
// preRun does, and returns what start.go passes to the node (#651).
func evictionRateFor(t *testing.T, cfgBody string, args ...string) int {
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
	return reserveEvictionRate(c.config)
}

func TestReserveEvictionRateDefault(t *testing.T) {
	const env = "BEE_RESERVE_EVICTION_RATE"

	t.Run("unset gives 500", func(t *testing.T) {
		if got := evictionRateFor(t, ""); got != 500 {
			t.Fatalf("got %d, want 500", got)
		}
	})
	t.Run("config file 0 means no limit", func(t *testing.T) {
		if got := evictionRateFor(t, "reserve-eviction-rate: 0\n"); got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})
	t.Run("environment 0 means no limit", func(t *testing.T) {
		t.Setenv(env, "0")
		if got := evictionRateFor(t, ""); got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})
	t.Run("flag 0 means no limit", func(t *testing.T) {
		if got := evictionRateFor(t, "", "--reserve-eviction-rate=0"); got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})
	t.Run("empty environment counts as unset", func(t *testing.T) {
		t.Setenv(env, "")
		if got := evictionRateFor(t, ""); got != 500 {
			t.Fatalf("got %d, want 500", got)
		}
	})
	t.Run("config file value is kept", func(t *testing.T) {
		if got := evictionRateFor(t, "reserve-eviction-rate: 1200\n"); got != 1200 {
			t.Fatalf("got %d, want 1200", got)
		}
	})
}
