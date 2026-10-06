// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package accounting

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// Checking a debit before serving it, for #596.
//
// When a light peer's debt passes its disconnect limit, debit.Apply returns a
// BlockPeerError and the node blocklists and disconnects the peer. On a
// retrieval that happens after the chunk was already sent. CanDebit lets the
// retrieval handler ask the same question before it does any work, and answer
// with an error delivery instead, so the connection stays.
//
// Both callers use the same two functions, projectDebit for the balance and
// debitOverdraws for the comparison, so for requests that do not overlap the
// check and the debit cannot disagree.

// debitProjection is what a debit of a given price would write, worked out
// without writing it.
type debitProjection struct {
	// nextSurplus is the surplus balance left after the debit. It is nil when
	// the peer holds no surplus, and then the surplus is not written.
	nextSurplus *big.Int
	// coveredBySurplus is true when the surplus pays the whole price. The
	// balance then does not change and is not written.
	coveredBySurplus bool
	// cost is the part of the price the surplus does not pay, which is added
	// to the balance.
	cost *big.Int
	// nextBalance is the balance after the debit.
	nextBalance *big.Int
}

// projectDebit computes the balance a debit of price would leave, the way
// increaseBalance always has: any surplus balance pays first, and only the
// remainder is added to the balance. It reads the store and writes nothing.
//
// When reading the balance fails after the surplus was read, the returned
// projection still carries nextSurplus, because increaseBalance writes the
// surplus before it reads the balance and must keep doing so.
//
// Must be called under the peer's lock.
func (a *Accounting) projectDebit(peer swarm.Address, price *big.Int) (projected debitProjection, err error) {
	cost := new(big.Int).Set(price)

	surplusBalance, err := a.SurplusBalance(peer)
	if err != nil {
		return projected, fmt.Errorf("failed to get surplus balance: %w", err)
	}

	if surplusBalance.Cmp(big.NewInt(0)) > 0 {
		// get new surplus balance after deduct
		newSurplusBalance := new(big.Int).Sub(surplusBalance, cost)

		// if nothing left for debiting, the surplus pays all of it
		if newSurplusBalance.Cmp(big.NewInt(0)) >= 0 {
			projected.nextSurplus = newSurplusBalance
			projected.coveredBySurplus = true
			projected.cost = big.NewInt(0)
			projected.nextBalance, err = a.Balance(peer)
			if err != nil {
				projected.nextBalance = nil
			}
			return projected, err
		}

		// if surplus balance didn't cover full transaction, continue with the
		// leftover part as cost
		debitIncrease := new(big.Int).Sub(price, surplusBalance)

		// a sanity check
		if debitIncrease.Cmp(big.NewInt(0)) <= 0 {
			return projected, errors.New("sanity check failed for partial debit after surplus balance drawn")
		}
		cost.Set(debitIncrease)

		// the surplus is used up
		projected.nextSurplus = big.NewInt(0)
	}

	projected.cost = cost

	currentBalance, err := a.Balance(peer)
	if err != nil {
		if !errors.Is(err, ErrPeerNoBalance) {
			return projected, fmt.Errorf("failed to load balance: %w", err)
		}
	}

	// Get nextBalance by increasing current balance with the cost
	projected.nextBalance = new(big.Int).Add(currentBalance, cost)

	return projected, nil
}

// debitOverdraws reports whether a debit that leaves the peer at nextBalance
// puts it at or past its disconnect limit, plus the refresh due since its last
// refreshment.
//
// This is the check debit.Apply has always made, moved here unchanged so that
// CanDebit can make it too. The shadow reserved balance is not part of it.
// The elapsed time is capped with min(elapsed, 1), as it was inline; whether
// that is intended is open in #603.
//
// Must be called under the peer's lock.
func (a *Accounting) debitOverdraws(accountingPeer *accountingPeer, nextBalance *big.Int) bool {
	timeElapsedInSeconds := min(a.timeNow().Unix()-accountingPeer.refreshReceivedTimestamp, 1)

	// get appropriate refresh rate
	refreshRate := new(big.Int).Set(a.refreshRate)
	if !accountingPeer.fullNode {
		refreshRate = new(big.Int).Set(a.lightRefreshRate)
	}

	refreshDue := new(big.Int).Mul(big.NewInt(timeElapsedInSeconds), refreshRate)
	disconnectLimit := new(big.Int).Add(accountingPeer.disconnectLimit, refreshDue)

	return nextBalance.Cmp(disconnectLimit) >= 0
}

// CanDebit reports whether debiting peer by price now would leave it below
// the point where debit.Apply disconnects it. It changes nothing.
//
// The answer is advisory. CanDebit reserves nothing, so two requests from the
// same peer, or a push-sync debit between this check and the debit, can both
// pass it and the later one can still overdraw in Apply, which then
// disconnects as before. How often that still happens is measured in #599.
//
// It returns true, so the caller takes the path it took before this check
// existed, when:
//   - the peer is not yet connected in accounting, because Connect runs in its
//     own goroutine and may not have run when the first request arrives;
//   - the per-peer lock cannot be taken before ctx ends;
//   - the store cannot be read.
//
// In each of these cases PrepareDebit or Apply then fails or decides exactly
// as it did before.
//
// It is a method on the concrete type rather than on Interface, so the next
// upstream sync sees an added method and not a changed interface. The
// retrieval service finds it with a type assertion.
func (a *Accounting) CanDebit(ctx context.Context, peer swarm.Address, price uint64) bool {
	loggerV2 := a.logger.V(2).Register()

	accountingPeer := a.getAccountingPeer(peer)

	if err := accountingPeer.lock.TryLock(ctx); err != nil {
		loggerV2.Debug("can debit; failed to acquire lock", "error", err)
		return true
	}
	defer accountingPeer.lock.Unlock()

	if !accountingPeer.connected {
		return true
	}

	projected, err := a.projectDebit(peer, new(big.Int).SetUint64(price))
	if err != nil {
		loggerV2.Debug("can debit; failed to project the debit", "peer_address", peer, "error", err)
		return true
	}

	return !a.debitOverdraws(accountingPeer, projected.nextBalance)
}
