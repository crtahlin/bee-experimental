// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cache_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/sharky"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/cache"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type tempFS struct{ dir string }

func (d tempFS) Open(path string) (fs.File, error) {
	return os.OpenFile(filepath.Join(d.dir, path), os.O_RDWR|os.O_CREATE, 0o600)
}

// The cache count ends with the context's error (wasp #635).
func TestNewCancelledCount(t *testing.T) {
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
	if _, err := cache.New(ctx, st.IndexStore(), 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("cache.New with a cancelled context: %v, want context.Canceled", err)
	}
}
