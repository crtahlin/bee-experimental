// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package accounting

import (
	"math/big"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

func (a *Accounting) SetTimeNow(f func() time.Time) {
	a.timeNow = f
}

func (a *Accounting) SetTime(k int64) {
	a.SetTimeNow(func() time.Time {
		return time.Unix(k, 0)
	})
}

func (a *Accounting) IsPaymentOngoing(peer swarm.Address) bool {
	return a.getAccountingPeer(peer).paymentOngoing
}

// RefreshDueForTest exposes the refresh allowance for a peer that announced
// threshold and was last refreshed at refreshTimestampMillis, as of
// nowMillis. See accrual.go and issue #359.
func (a *Accounting) RefreshDueForTest(threshold *big.Int, refreshTimestampMillis, nowMillis int64) *big.Int {
	return a.refreshDue(&accountingPeer{
		paymentThreshold:             new(big.Int).Set(threshold),
		refreshTimestampMilliseconds: refreshTimestampMillis,
	}, time.UnixMilli(nowMillis))
}

// SetShadowReservedBalanceForTest sets the amount a peer is expected to be
// debited soon, which settle subtracts before deciding a payment. It is
// otherwise written only by settle itself when it queues one, so a test that
// needs it non-zero going in has no other way to arrange it. See wasp #316.
func (a *Accounting) SetShadowReservedBalanceForTest(peer swarm.Address, amount *big.Int) {
	accountingPeer := a.getAccountingPeer(peer)
	accountingPeer.lock.Lock()
	defer accountingPeer.lock.Unlock()
	accountingPeer.shadowReservedBalance.Set(amount)
}

// SetRefreshTimestampForTest sets when a peer was last refreshed, which is
// otherwise only written by a completed refreshment.
func (a *Accounting) SetRefreshTimestampForTest(peer swarm.Address, millis int64) {
	accountingPeer := a.getAccountingPeer(peer)
	accountingPeer.lock.Lock()
	defer accountingPeer.lock.Unlock()
	accountingPeer.refreshTimestampMilliseconds = millis
}

// SafeAccrualCapForTest exposes the cap the continuous mode accrues up to.
func (a *Accounting) SafeAccrualCapForTest(threshold *big.Int) *big.Int {
	return a.safeAccrualCap(&accountingPeer{
		paymentThreshold: new(big.Int).Set(threshold),
	})
}

// SettleRefreshDue exposes the allowance settle subtracts, so a test can pin it
// without reimplementing the expression. See wasp #316.
func (a *Accounting) SettleRefreshDue(peer swarm.Address, now time.Time) *big.Int {
	return a.settleRefreshDue(a.getAccountingPeer(peer), now)
}

// HoldPeerLockForTest takes a peer's accounting lock and returns the function
// that releases it, so a test can make a TryLock fail deterministically. A
// cancelled context alone does not, because TryLock picks at random between a
// free lock and a done context. See wasp #596.
func (a *Accounting) HoldPeerLockForTest(peer swarm.Address) (release func()) {
	accountingPeer := a.getAccountingPeer(peer)
	accountingPeer.lock.Lock()
	return accountingPeer.lock.Unlock
}
