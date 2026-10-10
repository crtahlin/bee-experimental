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

// stopWaitForRevealFor reads stop-wait-for-reveal as the start command
// does (#725).
func stopWaitForRevealFor(t *testing.T, cfgBody string, args ...string) bool {
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
	return c.config.GetBool(optionNameStopWaitForReveal)
}

func TestStopWaitForRevealSetting(t *testing.T) {
	t.Run("on by default", func(t *testing.T) {
		if !stopWaitForRevealFor(t, "") {
			t.Fatal("got false, want true")
		}
	})
	t.Run("config file off", func(t *testing.T) {
		if stopWaitForRevealFor(t, "stop-wait-for-reveal: false\n") {
			t.Fatal("got true, want false")
		}
	})
	t.Run("environment off", func(t *testing.T) {
		t.Setenv("BEE_STOP_WAIT_FOR_REVEAL", "false")
		if stopWaitForRevealFor(t, "") {
			t.Fatal("got true, want false")
		}
	})
	t.Run("flag off", func(t *testing.T) {
		if stopWaitForRevealFor(t, "", "--stop-wait-for-reveal=false") {
			t.Fatal("got true, want false")
		}
	})
}
