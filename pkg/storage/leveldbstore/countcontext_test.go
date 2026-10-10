// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldbstore_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
)

type countItem struct{ id string }

func (i *countItem) ID() string               { return i.id }
func (i *countItem) Namespace() string        { return "count" }
func (i *countItem) Marshal() ([]byte, error) { return []byte{1}, nil }
func (i *countItem) Unmarshal([]byte) error   { return nil }
func (i *countItem) Clone() storage.Item      { c := *i; return &c }
func (i *countItem) String() string           { return i.id }

func fillCount(tb testing.TB, s storage.Writer, n int) {
	tb.Helper()
	for i := range n {
		if err := s.Put(&countItem{id: strconv.Itoa(i)}); err != nil {
			tb.Fatal(err)
		}
	}
}

// CountContext counts like Count, and ends with the context's error once
// the context ends (wasp #635).
func TestCountContext(t *testing.T) {
	s, err := func() (*leveldbstore.Store, error) { s, _, err := leveldbstore.New(t.TempDir(), nil); return s, err }()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	fillCount(t, s, 10000)

	want, err := s.Count(&countItem{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.CountContext(context.Background(), &countItem{})
	if err != nil || got != want || got != 10000 {
		t.Fatalf("CountContext = %d, %v; Count = %d; want 10000", got, err, want)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.CountContext(ctx, &countItem{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("CountContext with a cancelled context: %v, want context.Canceled", err)
	}
}

func BenchmarkCount(b *testing.B) {
	s, err := func() (*leveldbstore.Store, error) { s, _, err := leveldbstore.New(b.TempDir(), nil); return s, err }()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	fillCount(b, s, 100000)
	b.Run("Count", func(b *testing.B) {
		for b.Loop() {
			if _, err := s.Count(&countItem{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("CountContext", func(b *testing.B) {
		ctx := context.Background()
		for b.Loop() {
			if _, err := s.CountContext(ctx, &countItem{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
