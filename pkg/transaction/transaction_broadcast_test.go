// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transaction_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	storemock "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	"github.com/ethersphere/bee/v2/pkg/transaction/monitormock"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
)

// broadcastFixture is a transaction service with a real signer, so hashes
// are real, and a backend whose behaviour each test sets.
type broadcastFixture struct {
	mu          sync.Mutex
	sent        []*types.Transaction
	sendErr     error
	knownTxs    map[common.Hash]bool
	receipts    map[common.Hash]*types.Receipt
	onchain     uint64 // NonceAt and PendingNonceAt
	lookupErr   error
	beforeSend  func(tx *types.Transaction)
	feeCap, tip *big.Int

	store   storage.StateStorer
	service transaction.Service
}

func newBroadcastFixture(t *testing.T) *broadcastFixture {
	t.Helper()

	key, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		t.Fatal(err)
	}
	signer := crypto.NewDefaultSigner(key)
	sender, err := signer.EthereumAddress()
	if err != nil {
		t.Fatal(err)
	}

	f := &broadcastFixture{
		knownTxs: make(map[common.Hash]bool),
		receipts: make(map[common.Hash]*types.Receipt),
		onchain:  5,
		feeCap:   big.NewInt(10_000_000_000),
		tip:      big.NewInt(1_000_000_000),
		store:    storemock.NewStateStore(),
	}

	backend := backendmock.New(
		backendmock.WithSendTransactionFunc(func(ctx context.Context, tx *types.Transaction) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.beforeSend != nil {
				f.beforeSend(tx)
			}
			f.sent = append(f.sent, tx)
			return f.sendErr
		}),
		backendmock.WithEstimateGasFunc(func(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
			return 50_000, nil
		}),
		backendmock.WithPendingNonceAtFunc(func(ctx context.Context, account common.Address) (uint64, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.onchain, nil
		}),
		backendmock.WithNonceAtFunc(func(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.onchain, nil
		}),
		backendmock.WithSuggestedFeeAndTipFunc(func(ctx context.Context, gasPrice *big.Int, boostPercent int) (*big.Int, *big.Int, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return new(big.Int).Set(f.feeCap), new(big.Int).Set(f.tip), nil
		}),
		backendmock.WithTransactionByHashFunc(func(ctx context.Context, txHash common.Hash) (*types.Transaction, bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lookupErr != nil {
				return nil, false, f.lookupErr
			}
			if f.knownTxs[txHash] {
				return nil, true, nil
			}
			return nil, false, ethereum.NotFound
		}),
		backendmock.WithTransactionReceiptFunc(func(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lookupErr != nil {
				return nil, f.lookupErr
			}
			if r, ok := f.receipts[txHash]; ok {
				return r, nil
			}
			return nil, ethereum.NotFound
		}),
	)

	monitor := monitormock.New(
		monitormock.WithWatchTransactionFunc(func(txHash common.Hash, nonce uint64) (<-chan types.Receipt, <-chan error, error) {
			return nil, nil, nil
		}),
	)

	f.service, err = transaction.NewService(log.Noop, sender, backend, signer, f.store, big.NewInt(5), monitor, 0)
	if err != nil {
		t.Fatal(err)
	}
	testutil.CleanupCloser(t, f.service)
	return f
}

func (f *broadcastFixture) sentTxs() []*types.Transaction {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*types.Transaction(nil), f.sent...)
}

func broadcastRequest() *transaction.TxRequest {
	to := common.HexToAddress("0xabcd")
	return &transaction.TxRequest{To: &to, Data: []byte{1, 2, 3}, Value: big.NewInt(0), Description: "test"}
}

func isPending(t *testing.T, s transaction.Service, txHash common.Hash) bool {
	t.Helper()
	pending, err := s.PendingTransactions()
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range pending {
		if h == txHash {
			return true
		}
	}
	return false
}

// The signed transaction is stored, and the hook called with its hash,
// before the send is made.
func TestSendStoresBeforeBroadcast(t *testing.T) {
	t.Parallel()

	f := newBroadcastFixture(t)

	var hooked common.Hash
	var storedAtSend bool
	f.beforeSend = func(tx *types.Transaction) {
		if hooked != tx.Hash() {
			t.Errorf("hook not called with the hash before the send: hook %s, sent %s", hooked, tx.Hash())
		}
		stored, err := f.service.StoredTransaction(tx.Hash())
		storedAtSend = err == nil && len(stored.Raw) > 0
	}

	txHash, err := f.service.SendWithHook(context.Background(), broadcastRequest(), 0, func(h common.Hash) error {
		hooked = h
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !storedAtSend {
		t.Fatal("signed transaction not stored before the send")
	}
	if txHash != hooked {
		t.Fatalf("returned hash %s, hooked %s", txHash, hooked)
	}
}

// Errors raised before SendTransaction wrap ErrNotBroadcast, keep their
// text, and nothing is sent.
func TestSendNotBroadcastBeforeSend(t *testing.T) {
	t.Parallel()

	f := newBroadcastFixture(t)
	hookErr := errors.New("key write failed")

	_, err := f.service.SendWithHook(context.Background(), broadcastRequest(), 0, func(common.Hash) error {
		return hookErr
	})
	if !errors.Is(err, transaction.ErrNotBroadcast) {
		t.Fatalf("got %v, want ErrNotBroadcast", err)
	}
	if !errors.Is(err, hookErr) {
		t.Fatalf("got %v, want the hook error wrapped", err)
	}
	if err.Error() != hookErr.Error() {
		t.Fatalf("error text changed: %q", err.Error())
	}
	if n := len(f.sentTxs()); n != 0 {
		t.Fatalf("sent %d transactions, want 0", n)
	}
}

// Any SendTransaction error is ambiguous: not ErrNotBroadcast, the hash is
// returned, the transaction stays stored and pending, and the next
// transaction does not reuse its nonce although the backend does not know
// it.
func TestSendErrorIsAmbiguous(t *testing.T) {
	t.Parallel()

	f := newBroadcastFixture(t)
	f.sendErr = errors.New("connection reset")

	txHash, err := f.service.Send(context.Background(), broadcastRequest(), 0)
	if err == nil {
		t.Fatal("want the send error")
	}
	if errors.Is(err, transaction.ErrNotBroadcast) {
		t.Fatal("a SendTransaction error reported as not broadcast")
	}
	if txHash == (common.Hash{}) {
		t.Fatal("no hash returned for an ambiguous send")
	}
	stored, err := f.service.StoredTransaction(txHash)
	if err != nil {
		t.Fatal(err)
	}
	if !isPending(t, f.service, txHash) {
		t.Fatal("ambiguous send not registered as pending")
	}

	f.mu.Lock()
	f.sendErr = nil
	f.mu.Unlock()

	next, err := f.service.Send(context.Background(), broadcastRequest(), 0)
	if err != nil {
		t.Fatal(err)
	}
	nextStored, err := f.service.StoredTransaction(next)
	if err != nil {
		t.Fatal(err)
	}
	if nextStored.Nonce != stored.Nonce+1 {
		t.Fatalf("next nonce %d, want %d (ambiguous nonce reused)", nextStored.Nonce, stored.Nonce+1)
	}
}

// RebroadcastTransaction sends the exact stored transaction and returns
// send errors.
func TestRebroadcastTransaction(t *testing.T) {
	t.Parallel()

	f := newBroadcastFixture(t)

	txHash, err := f.service.Send(context.Background(), broadcastRequest(), 0)
	if err != nil {
		t.Fatal(err)
	}

	// fees moved since; a re-signed transaction would get a new hash
	f.mu.Lock()
	f.feeCap = big.NewInt(20_000_000_000)
	f.mu.Unlock()

	if err := f.service.RebroadcastTransaction(context.Background(), txHash); err != nil {
		t.Fatal(err)
	}
	sent := f.sentTxs()
	if got := sent[len(sent)-1].Hash(); got != txHash {
		t.Fatalf("rebroadcast hash %s, want %s", got, txHash)
	}

	sendErr := errors.New("endpoint down")
	f.mu.Lock()
	f.sendErr = sendErr
	f.mu.Unlock()
	if err := f.service.RebroadcastTransaction(context.Background(), txHash); !errors.Is(err, sendErr) {
		t.Fatalf("got %v, want the send error", err)
	}
}

// ReplaceTransaction keeps the nonce, raises both fees by at least 10 %,
// and calls the hook with the new hash before the send.
func TestReplaceTransaction(t *testing.T) {
	t.Parallel()

	f := newBroadcastFixture(t)

	txHash, err := f.service.Send(context.Background(), broadcastRequest(), 0)
	if err != nil {
		t.Fatal(err)
	}
	old, err := f.service.StoredTransaction(txHash)
	if err != nil {
		t.Fatal(err)
	}

	var hooked common.Hash
	f.mu.Lock()
	f.beforeSend = func(tx *types.Transaction) {
		if tx.Hash() != hooked {
			t.Errorf("replacement sent before its hash was hooked")
		}
	}
	f.mu.Unlock()

	newHash, err := f.service.ReplaceTransaction(context.Background(), txHash, func(h common.Hash) error {
		hooked = h
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if newHash == txHash || newHash != hooked {
		t.Fatalf("replacement hash %s, hooked %s, old %s", newHash, hooked, txHash)
	}
	repl, err := f.service.StoredTransaction(newHash)
	if err != nil {
		t.Fatal(err)
	}
	if repl.Nonce != old.Nonce {
		t.Fatalf("replacement nonce %d, want %d", repl.Nonce, old.Nonce)
	}
	plusTen := func(v *big.Int) *big.Int {
		return new(big.Int).Div(new(big.Int).Mul(v, big.NewInt(110)), big.NewInt(100))
	}
	if repl.GasFeeCap.Cmp(plusTen(old.GasFeeCap)) < 0 || repl.GasTipCap.Cmp(plusTen(old.GasTipCap)) < 0 {
		t.Fatalf("fees not raised by 10 %%: cap %s -> %s, tip %s -> %s", old.GasFeeCap, repl.GasFeeCap, old.GasTipCap, repl.GasTipCap)
	}
}

func TestTransactionStatus(t *testing.T) {
	t.Parallel()

	f := newBroadcastFixture(t)

	txHash, err := f.service.Send(context.Background(), broadcastRequest(), 0)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.service.StoredTransaction(txHash)
	if err != nil {
		t.Fatal(err)
	}

	check := func(want transaction.TxState) {
		t.Helper()
		got, err := f.service.TransactionStatus(context.Background(), txHash)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %s, want %s", got, want)
		}
	}

	check(transaction.TxNotFound)

	f.mu.Lock()
	f.onchain = stored.Nonce + 1
	f.mu.Unlock()
	check(transaction.TxCancelled)

	f.mu.Lock()
	f.knownTxs[txHash] = true
	f.mu.Unlock()
	check(transaction.TxPending)

	f.mu.Lock()
	f.receipts[txHash] = &types.Receipt{Status: types.ReceiptStatusFailed}
	f.mu.Unlock()
	check(transaction.TxReverted)

	f.mu.Lock()
	f.receipts[txHash] = &types.Receipt{Status: types.ReceiptStatusSuccessful}
	f.mu.Unlock()
	check(transaction.TxMined)

	lookupErr := errors.New("rpc down")
	f.mu.Lock()
	f.lookupErr = lookupErr
	f.mu.Unlock()
	if _, err := f.service.TransactionStatus(context.Background(), txHash); !errors.Is(err, lookupErr) {
		t.Fatalf("got %v, want the lookup error", err)
	}
}
