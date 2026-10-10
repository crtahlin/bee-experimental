// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package reserve_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/sharky"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kademlia "github.com/ethersphere/bee/v2/pkg/topology/mock"
)

type tempFS struct{ dir string }

func (d tempFS) Open(path string) (fs.File, error) {
	return os.OpenFile(filepath.Join(d.dir, path), os.O_RDWR|os.O_CREATE, 0o600)
}

// The reserve count, the slowest step of opening a large reserve, ends
// with the context's error, so a stop during startup does not wait for it
// (wasp #635).
func TestNewContextCancelledCount(t *testing.T) {
	sh, err := sharky.New(tempFS{t.TempDir()}, 32, swarm.SocMaxChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	store, _, err := leveldbstore.New("", nil)
	if err != nil {
		t.Fatal(err)
	}
	st := transaction.NewStorage(sh, store)
	t.Cleanup(func() { _ = st.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = reserve.NewContext(ctx, swarm.RandAddress(t), st, 10, kademlia.NewTopologyDriver(), log.Noop)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NewContext with a cancelled context: %v, want context.Canceled", err)
	}

	if _, err := reserve.NewContext(context.Background(), swarm.RandAddress(t), st, 10, kademlia.NewTopologyDriver(), log.Noop); err != nil {
		t.Fatalf("NewContext: %v", err)
	}
}
