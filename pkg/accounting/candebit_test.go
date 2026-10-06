// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package accounting_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/accounting"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	p2pmock "github.com/ethersphere/bee/v2/pkg/p2p/mock"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// With the shared test constants a light peer has a disconnect limit of 1,100
// (10,000 divided by the light factor of 10, plus 10 per cent) and a refresh
// due of 100 (the refresh rate of 1,000 divided by 10). Apply disconnects at a
// balance of 1,200 or more. A full peer has 11,000 plus 1,000, so 12,000.
const (
	lightOverdrawAt = int64(1200)
	fullOverdrawAt  = int64(12000)
)

// newCanDebitAccounting returns an accounting with a fixed clock, and a peer
// connected as a full or a light node. The clock is fixed so the refresh due
// is the same in CanDebit and in Apply.
func newCanDebitAccounting(t *testing.T, fullNode bool, opts ...p2pmock.Option) (*accounting.Accounting, swarm.Address) {
	t.Helper()

	store := mock.NewStateStore()
	t.Cleanup(func() { _ = store.Close() })

	acc, err := accounting.NewAccounting(testPaymentThreshold, testPaymentTolerance, testPaymentEarly, log.Noop, store, &pricingMock{}, big.NewInt(testRefreshRate), testLightFactor, p2pmock.New(opts...))
	if err != nil {
		t.Fatal(err)
	}
	acc.SetTime(1000)

	peer := swarm.RandAddress(t)
	acc.Connect(peer, fullNode)
	return acc, peer
}

// debit applies one debit the way the retrieval handler does.
func debit(t *testing.T, acc *accounting.Accounting, peer swarm.Address, price uint64) error {
	t.Helper()

	action, err := acc.PrepareDebit(context.Background(), peer, price)
	if err != nil {
		t.Fatal(err)
	}
	defer action.Cleanup()
	return action.Apply()
}

// TestCanDebitAgreesWithApply runs serial debits up to and past the point
// where Apply disconnects, and asks CanDebit before each one. The two use the
// same functions, so for requests that do not overlap they must agree at every
// step, including the exact boundary and with a surplus balance paying part of
// the price.
func TestCanDebitAgreesWithApply(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		fullNode   bool
		price      uint64
		surplus    int64
		overdrawAt int64
	}{
		// 100 divides 1,200, so one debit lands exactly on the limit.
		{name: "light, exact boundary", price: 100, overdrawAt: lightOverdrawAt},
		{name: "light, uneven price", price: 7, overdrawAt: lightOverdrawAt},
		// A surplus of 150 pays the first debit in full and part of the
		// second, which is the branch where the cost is not the price.
		{name: "light, with surplus", price: 100, surplus: 150, overdrawAt: lightOverdrawAt},
		// A price of 1,250 alone is past the limit, but a surplus of 100
		// pays part of it and leaves 1,150, so the first debit is allowed.
		// A check that ignored the surplus would refuse it.
		{name: "light, surplus decides", price: 1250, surplus: 100, overdrawAt: lightOverdrawAt},
		{name: "full, exact boundary", fullNode: true, price: 1000, overdrawAt: fullOverdrawAt},
		{name: "full, with surplus", fullNode: true, price: 1000, surplus: 1500, overdrawAt: fullOverdrawAt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			acc, peer := newCanDebitAccounting(t, tc.fullNode)
			if tc.surplus > 0 {
				if err := acc.NotifyPaymentReceived(peer, big.NewInt(tc.surplus)); err != nil {
					t.Fatal(err)
				}
			}

			allowed := 0
			for i := 0; ; i++ {
				if i > 1000 {
					t.Fatal("never reached the disconnect limit")
				}
				balance, err := acc.Balance(peer)
				if err != nil {
					t.Fatal(err)
				}

				can := acc.CanDebit(context.Background(), peer, tc.price)
				err = debit(t, acc, peer, tc.price)
				if can != (err == nil) {
					t.Fatalf("debit %d from balance %d: CanDebit said %v, Apply returned %v", i, balance, can, err)
				}
				if err == nil {
					allowed++
					continue
				}

				var bpe *p2p.BlockPeerError
				if !errors.As(err, &bpe) {
					t.Fatalf("Apply returned %v, want a BlockPeerError", err)
				}
				// The refusal must come where the arithmetic says: the
				// balance after the debit, net of surplus, at or past the
				// limit.
				after := new(big.Int).Add(balance, new(big.Int).SetUint64(tc.price))
				if after.Cmp(big.NewInt(tc.overdrawAt)) < 0 {
					t.Fatalf("refused at balance %d plus %d, below %d", balance, tc.price, tc.overdrawAt)
				}
				break
			}
			if allowed == 0 {
				t.Fatal("no debit was allowed, so the boundary was not tested")
			}
		})
	}
}

// TestCanDebitIgnoresShadowReserve. A request already prepared but not yet
// applied raises the shadow reserved balance. Apply does not count it, so
// CanDebit must not either, or it would refuse requests Apply accepts.
func TestCanDebitIgnoresShadowReserve(t *testing.T) {
	t.Parallel()

	acc, peer := newCanDebitAccounting(t, false)

	// balance 1,000
	for range 10 {
		if err := debit(t, acc, peer, 100); err != nil {
			t.Fatal(err)
		}
	}

	// A request in flight: 150 in the shadow reserve.
	inFlight, err := acc.PrepareDebit(context.Background(), peer, 150)
	if err != nil {
		t.Fatal(err)
	}
	defer inFlight.Cleanup()

	// 1,000 + 100 is below 1,200. Counting the shadow reserve, 1,250 is not.
	if !acc.CanDebit(context.Background(), peer, 100) {
		t.Fatal("CanDebit refused a debit Apply accepts; it counted the shadow reserve")
	}
	if err := debit(t, acc, peer, 100); err != nil {
		t.Fatalf("Apply refused the debit CanDebit allowed: %v", err)
	}
}

// TestCanDebitChangesNothing. The check is advisory and reserves nothing:
// balance, surplus, shadow reserve and ghost balance stay as they were,
// whichever way it answers.
func TestCanDebitChangesNothing(t *testing.T) {
	t.Parallel()

	acc, peer := newCanDebitAccounting(t, false)
	if err := acc.NotifyPaymentReceived(peer, big.NewInt(50)); err != nil {
		t.Fatal(err)
	}

	before, err := acc.PeerAccounting()
	if err != nil {
		t.Fatal(err)
	}

	if !acc.CanDebit(context.Background(), peer, 100) {
		t.Fatal("a small debit was refused")
	}
	if acc.CanDebit(context.Background(), peer, uint64(lightOverdrawAt)+50) {
		t.Fatal("a debit past the limit was allowed")
	}

	after, err := acc.PeerAccounting()
	if err != nil {
		t.Fatal(err)
	}
	b, a := before[peer.String()], after[peer.String()]
	for _, f := range []struct {
		name          string
		before, after *big.Int
	}{
		{"balance", b.Balance, a.Balance},
		{"surplus balance", b.SurplusBalance, a.SurplusBalance},
		{"shadow reserved balance", b.ShadowReservedBalance, a.ShadowReservedBalance},
		{"ghost balance", b.GhostBalance, a.GhostBalance},
	} {
		if f.before.Cmp(f.after) != 0 {
			t.Errorf("%s changed from %d to %d", f.name, f.before, f.after)
		}
	}
}

// TestCanDebitNotConnected. Connect runs in its own goroutine, so a request
// can arrive before it. The check then allows the request, and PrepareDebit
// refuses it as it always has.
func TestCanDebitNotConnected(t *testing.T) {
	t.Parallel()

	acc, _ := newCanDebitAccounting(t, false)
	stranger := swarm.RandAddress(t)

	if !acc.CanDebit(context.Background(), stranger, uint64(fullOverdrawAt)*10) {
		t.Fatal("CanDebit refused a peer not yet connected in accounting")
	}
	if _, err := acc.PrepareDebit(context.Background(), stranger, 1); err == nil {
		t.Fatal("PrepareDebit accepted a peer not yet connected; the old path changed")
	}
}

// TestCanDebitLockNotTaken. When the per-peer lock cannot be taken before the
// context ends, the check allows the request, so PrepareDebit decides as it
// did before.
func TestCanDebitLockNotTaken(t *testing.T) {
	t.Parallel()

	acc, peer := newCanDebitAccounting(t, false)

	release := acc.HoldPeerLockForTest(peer)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if !acc.CanDebit(ctx, peer, uint64(lightOverdrawAt)*10) {
		t.Fatal("CanDebit refused without holding the lock")
	}
}
