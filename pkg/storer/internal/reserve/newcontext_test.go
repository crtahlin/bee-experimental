// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package reserve_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storer/internal"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kademlia "github.com/ethersphere/bee/v2/pkg/topology/mock"
)

var errContextCount = errors.New("context-aware count used")

// countSpyStorage wraps a storage so its transactions' index store reports
// which count NewContext used.
type countSpyStorage struct {
	transaction.Storage
	t *testing.T
}

func (s countSpyStorage) Run(ctx context.Context, f func(transaction.Store) error) error {
	return s.Storage.Run(ctx, func(st transaction.Store) error {
		return f(countSpyStore{st, s.t})
	})
}

type countSpyStore struct {
	transaction.Store
	t *testing.T
}

func (s countSpyStore) IndexStore() storage.IndexStore {
	return countSpyIndex{s.Store.IndexStore(), s.t}
}

type countSpyIndex struct {
	storage.IndexStore
	t *testing.T
}

func (i countSpyIndex) Count(storage.Key) (int, error) {
	i.t.Error("NewContext used Count, which a stop during startup cannot end")
	return 0, nil
}

func (i countSpyIndex) CountContext(context.Context, storage.Key) (int, error) {
	return 0, errContextCount
}

// NewContext counts the reserve with the context-aware count, so a stop
// during startup does not wait for a full count of a large reserve (wasp
// #635).
func TestNewContextUsesContextAwareCount(t *testing.T) {
	st := countSpyStorage{internal.NewInmemStorage(), t}
	_, err := reserve.NewContext(context.Background(), swarm.RandAddress(t), st, 10, kademlia.NewTopologyDriver(), log.Noop)
	if !errors.Is(err, errContextCount) {
		t.Fatalf("NewContext: %v, want the context-aware count's result", err)
	}
}
