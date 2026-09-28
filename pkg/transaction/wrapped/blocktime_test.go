// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrapped

import (
	"context"
	"math/big"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	"github.com/stretchr/testify/assert"
)

// TestAverageBlockTimeExposed: the backend reports 0 until it has measured
// the chain from two headers, then the measured block time, and
// transaction.ObservedBlockTime falls back to the configured value until then
// (#540).
func TestAverageBlockTimeExposed(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const (
			realBlockTime     = 2 * time.Second
			configBlockTime   = 5 * time.Second
			blockSyncInterval = uint64(3)
		)

		chain := newChainSimulator(100, realBlockTime)
		go chain.advanceEvery(realBlockTime, 1)
		synctest.Wait()

		backend := newTestWrappedBackendWithConfig(t, configBlockTime, blockSyncInterval,
			backendmock.WithHeaderbyNumberFunc(func(context.Context, *big.Int) (*types.Header, error) {
				return chain.header(), nil
			}),
		)
		observed := transaction.ObservedBlockTime(backend, configBlockTime)

		assert.Equal(t, time.Duration(0), backend.AverageBlockTime())
		assert.Equal(t, configBlockTime, observed())

		// The first header anchors the estimate but measures nothing.
		_, err := backend.BlockNumber(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(0), backend.AverageBlockTime())
		assert.Equal(t, configBlockTime, observed())

		// Past the sync interval at the configured block time, the second
		// header measures the chain.
		time.Sleep(time.Duration(blockSyncInterval) * configBlockTime)
		synctest.Wait()
		_, err = backend.BlockNumber(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, realBlockTime, backend.AverageBlockTime())
		assert.Equal(t, realBlockTime, observed())
	})
}
