// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/safe"
	"github.com/ethersphere/bee/v2/pkg/sctx"
	"github.com/ethersphere/bee/v2/pkg/storage"
)

// loggerName is the tree path name of the logger for this package.
const loggerName = "transaction"

const (
	storedTransactionPrefix  = "transaction_stored_"
	pendingTransactionPrefix = "transaction_pending_"
)

var (
	// ErrTransactionReverted denotes that the sent transaction has been
	// reverted.
	ErrTransactionReverted = errors.New("transaction reverted")
	ErrUnknownTransaction  = errors.New("unknown transaction")
	ErrAlreadyImported     = errors.New("already imported")
	// ErrNotBroadcast denotes an error raised before the transaction was
	// handed to the backend, so it cannot have reached the network.
	ErrNotBroadcast = errors.New("transaction not broadcast")
	// ErrNoRawTransaction denotes a stored transaction without its signed
	// bytes (stored by an older version), which cannot be rebroadcast.
	ErrNoRawTransaction = errors.New("stored transaction has no signed bytes")
)

// ReplacementBumpPercent is how much a same-nonce replacement raises the
// stored fee cap and tip at least; nodes accept a replacement only with
// both raised by 10 % or more.
const ReplacementBumpPercent = 12

// AmbiguousPendingTimeout is how long a transaction whose send returned an
// error, and which the backend does not know, still counts for the next
// nonce. It may have reached the network through the endpoint; reusing its
// nonce at once could replace it.
const AmbiguousPendingTimeout = 10 * time.Minute

// BeforeBroadcastFunc is called with the signed transaction's hash after
// the signed transaction is stored and before it is sent. An error stops
// the send.
type BeforeBroadcastFunc func(txHash common.Hash) error

// TxState is the state of a sent transaction as seen on chain.
type TxState int

const (
	// TxPending: the backend knows the transaction and has no receipt.
	TxPending TxState = iota
	// TxMined: mined with a successful receipt.
	TxMined
	// TxReverted: mined with a failed receipt.
	TxReverted
	// TxNotFound: the backend does not know the transaction and its nonce
	// is not used yet.
	TxNotFound
	// TxCancelled: the backend does not know the transaction and its nonce
	// is used by another transaction.
	TxCancelled
)

func (s TxState) String() string {
	switch s {
	case TxPending:
		return "pending"
	case TxMined:
		return "mined"
	case TxReverted:
		return "reverted"
	case TxNotFound:
		return "not found"
	case TxCancelled:
		return "cancelled"
	}
	return "unknown"
}

// notBroadcastError marks err as raised before the send; its text is the
// text of err.
type notBroadcastError struct{ err error }

func (e *notBroadcastError) Error() string   { return e.err.Error() }
func (e *notBroadcastError) Unwrap() []error { return []error{ErrNotBroadcast, e.err} }

func notBroadcast(err error) error {
	if err == nil || errors.Is(err, ErrNotBroadcast) {
		return err
	}
	return &notBroadcastError{err: err}
}

const (
	DefaultGasLimit        = 1_000_000 // Used for contract operations when setGasLimit flag is enabled
	DefaultTipBoostPercent = 25
	MaxGasLimit            = 10_000_000 // Maximum allowed gas limit to prevent excessive values
	MinGasLimit            = 21_000     // Minimum gas for any transaction
	GasBufferPercent       = 33         // Add 33% buffer to estimated gas
	FallbackGasLimit       = 500_000    // Fallback when estimation fails and no minimum is set
)

// TxRequest describes a request for a transaction that can be executed.
type TxRequest struct {
	To                   *common.Address // recipient of the transaction
	Data                 []byte          // transaction data
	GasPrice             *big.Int        // gas price or nil if suggested gas price should be used
	GasLimit             uint64          // gas limit or 0 if it should be estimated
	MinEstimatedGasLimit uint64          // minimum gas limit to use if the gas limit was estimated; it will not apply when this value is 0 or when GasLimit is not 0
	GasFeeCap            *big.Int        // adds a cap to maximum fee user is willing to pay
	Value                *big.Int        // amount of wei to send
	Description          string          // optional description
}

type StoredTransaction struct {
	To          *common.Address // recipient of the transaction
	Data        []byte          // transaction data
	GasPrice    *big.Int        // used gas price
	GasLimit    uint64          // used gas limit
	GasTipBoost int             // adds a tip for the miner for prioritizing transaction
	GasTipCap   *big.Int        // adds a cap to the tip
	GasFeeCap   *big.Int        // adds a cap to maximum fee user is willing to pay
	Value       *big.Int        // amount of wei to send
	Nonce       uint64          // used nonce
	Created     int64           // creation timestamp
	Description string          // description
	Raw         []byte          // the signed transaction, as sent
}

// pendingTransaction is the value stored under the pending key.
type pendingTransaction struct {
	// Ambiguous is set when the send returned an error: the transaction
	// may or may not have reached the network.
	Ambiguous bool `json:"ambiguous,omitempty"`
}

// Service is the service to send transactions. It takes care of gas price, gas
// limit and nonce management.
type Service interface {
	io.Closer
	// Send creates a transaction based on the request (with gasprice increased by provided percentage) and sends it.
	Send(ctx context.Context, request *TxRequest, tipCapBoostPercent int) (txHash common.Hash, err error)
	// SendWithHook is Send with a hook: the signed transaction is stored
	// first, then beforeBroadcast (if not nil) is called with its hash,
	// then the transaction is sent. Errors raised before the send wrap
	// ErrNotBroadcast. If the send itself fails, the hash is returned with
	// the error, the transaction stays stored and counts as pending for
	// the next nonce, since it may have reached the network.
	SendWithHook(ctx context.Context, request *TxRequest, tipCapBoostPercent int, beforeBroadcast BeforeBroadcastFunc) (txHash common.Hash, err error)
	// RebroadcastTransaction sends the stored signed transaction again,
	// unchanged (same nonce, same fees, same hash). Send errors are
	// returned.
	RebroadcastTransaction(ctx context.Context, txHash common.Hash) error
	// ReplaceTransaction signs the stored transaction again with the same
	// nonce and raised fees, stores it, calls beforeBroadcast (if not nil)
	// with the new hash and sends it. Errors are as for SendWithHook.
	ReplaceTransaction(ctx context.Context, txHash common.Hash, beforeBroadcast BeforeBroadcastFunc) (common.Hash, error)
	// TransactionStatus looks the transaction up on chain. It does not
	// check whether the backend is synced; a caller acting on TxNotFound
	// checks that with IsSynced.
	TransactionStatus(ctx context.Context, txHash common.Hash) (TxState, error)
	// Call simulate a transaction based on the request.
	Call(ctx context.Context, request *TxRequest) (result []byte, err error)
	// WaitForReceipt waits until either the transaction with the given hash has been mined or the context is cancelled.
	// This is only valid for transaction sent by this service.
	WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error)
	// WatchSentTransaction start watching the given transaction.
	// This wraps the monitors watch function by loading the correct nonce from the store.
	// This is only valid for transaction sent by this service.
	WatchSentTransaction(txHash common.Hash) (<-chan types.Receipt, <-chan error, error)
	// StoredTransaction retrieves the stored information for the transaction
	StoredTransaction(txHash common.Hash) (*StoredTransaction, error)
	// PendingTransactions retrieves the list of all pending transaction hashes
	PendingTransactions() ([]common.Hash, error)
	// ResendTransaction resends a previously sent transaction
	// This operation can be useful if for some reason the transaction vanished from the eth networks pending pool
	ResendTransaction(ctx context.Context, txHash common.Hash) error
	// CancelTransaction cancels a previously sent transaction by double-spending its nonce with zero-transfer one
	CancelTransaction(ctx context.Context, originalTxHash common.Hash) (common.Hash, error)
	// TransactionFee retrieves the transaction fee
	TransactionFee(ctx context.Context, txHash common.Hash) (*big.Int, error)
	// UnwrapABIError tries to unwrap the ABI error if the given error is not nil.
	// The original error is wrapped together with the ABI error if it exists.
	UnwrapABIError(ctx context.Context, req *TxRequest, err error, abiErrors map[string]abi.Error) error
}

type transactionService struct {
	wg     sync.WaitGroup
	lock   sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	logger           log.Logger
	backend          Backend
	signer           crypto.Signer
	sender           common.Address
	store            storage.StateStorer
	chainID          *big.Int
	monitor          Monitor
	fallbackGasLimit uint64
}

// NewService creates a new transaction service.
func NewService(logger log.Logger, overlayEthAddress common.Address, backend Backend, signer crypto.Signer, store storage.StateStorer, chainID *big.Int, monitor Monitor, fallbackGasLimit uint64) (Service, error) {
	senderAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}

	if fallbackGasLimit == 0 {
		fallbackGasLimit = FallbackGasLimit
	}

	ctx, cancel := context.WithCancel(context.Background())

	t := &transactionService{
		ctx:              ctx,
		cancel:           cancel,
		logger:           logger.WithName(loggerName).WithValues("sender_address", overlayEthAddress).Register(),
		backend:          backend,
		signer:           signer,
		sender:           senderAddress,
		store:            store,
		chainID:          chainID,
		monitor:          monitor,
		fallbackGasLimit: fallbackGasLimit,
	}

	if err = t.waitForAllPendingTx(); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *transactionService) waitForAllPendingTx() error {
	pendingTxs, err := t.PendingTransactions()
	if err != nil {
		return err
	}

	pending := t.filterPendingTransactions(t.ctx, pendingTxs)

	for txHash := range pending {
		t.waitForPendingTx(txHash)
	}

	return nil
}

// Send creates and signs a transaction based on the request and sends it.
func (t *transactionService) Send(ctx context.Context, request *TxRequest, boostPercent int) (txHash common.Hash, err error) {
	return t.SendWithHook(ctx, request, boostPercent, nil)
}

// SendWithHook creates and signs a transaction based on the request, stores
// it, calls beforeBroadcast and sends it.
func (t *transactionService) SendWithHook(ctx context.Context, request *TxRequest, boostPercent int, beforeBroadcast BeforeBroadcastFunc) (txHash common.Hash, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	nonce, err := t.nextNonce(ctx)
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}

	tx, err := t.prepareTransaction(ctx, request, nonce, boostPercent)
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}

	signedTx, err := t.signer.SignTx(tx, t.chainID)
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}

	return t.storeAndBroadcast(ctx, signedTx, boostPercent, request.Description, beforeBroadcast)
}

// storeAndBroadcast stores the signed transaction, calls beforeBroadcast and
// sends the transaction. It must be called with t.lock held.
func (t *transactionService) storeAndBroadcast(ctx context.Context, signedTx *types.Transaction, boostPercent int, description string, beforeBroadcast BeforeBroadcastFunc) (common.Hash, error) {
	txHash := signedTx.Hash()

	raw, err := signedTx.MarshalBinary()
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}

	err = t.store.Put(storedTransactionKey(txHash), StoredTransaction{
		To:          signedTx.To(),
		Data:        signedTx.Data(),
		GasPrice:    signedTx.GasPrice(),
		GasLimit:    signedTx.Gas(),
		GasTipBoost: boostPercent,
		GasTipCap:   signedTx.GasTipCap(),
		GasFeeCap:   signedTx.GasFeeCap(),
		Value:       signedTx.Value(),
		Nonce:       signedTx.Nonce(),
		Created:     time.Now().Unix(),
		Description: description,
		Raw:         raw,
	})
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}

	if beforeBroadcast != nil {
		if err := beforeBroadcast(txHash); err != nil {
			return common.Hash{}, notBroadcast(err)
		}
	}

	t.logger.V(1).Register().Debug("sending transaction", "tx", txHash, "nonce", signedTx.Nonce())

	sendErr := t.backend.SendTransaction(ctx, signedTx)
	if sendErr != nil {
		// The transaction may have reached the network through the
		// endpoint: keep it stored and count its nonce as used.
		if err := t.store.Put(pendingTransactionKey(txHash), pendingTransaction{Ambiguous: true}); err != nil {
			t.logger.Error(err, "registering possibly sent transaction as pending failed", "tx", txHash)
		}
		t.waitForPendingTx(txHash)
		return txHash, sendErr
	}

	err = t.store.Put(pendingTransactionKey(txHash), pendingTransaction{})
	if err != nil {
		return txHash, err
	}

	t.waitForPendingTx(txHash)

	return txHash, nil
}

func (t *transactionService) waitForPendingTx(txHash common.Hash) {
	t.wg.Go(func() {
		safe.Run(t.logger, "transaction-wait-pending", func() {
			switch _, err := t.WaitForReceipt(t.ctx, txHash); err {
			case nil:
				t.logger.Info("pending transaction confirmed", "tx", txHash)
				err = t.store.Delete(pendingTransactionKey(txHash))
				if err != nil {
					t.logger.Error(err, "unregistering finished pending transaction failed", "tx", txHash)
				}
			default:
				if errors.Is(err, ErrTransactionCancelled) {
					t.logger.Warning("pending transaction cancelled", "tx", txHash)
				} else {
					t.logger.Error(err, "waiting for pending transaction failed", "tx", txHash)
				}
			}
		})
	})
}

func (t *transactionService) Call(ctx context.Context, request *TxRequest) ([]byte, error) {
	msg := ethereum.CallMsg{
		From:     t.sender,
		To:       request.To,
		Data:     request.Data,
		GasPrice: request.GasPrice,
		Gas:      request.GasLimit,
		Value:    request.Value,
	}
	data, err := t.backend.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (t *transactionService) StoredTransaction(txHash common.Hash) (*StoredTransaction, error) {
	var tx StoredTransaction
	err := t.store.Get(storedTransactionKey(txHash), &tx)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrUnknownTransaction
		}
		return nil, err
	}
	return &tx, nil
}

// prepareTransaction creates a signable transaction based on a request.
func (t *transactionService) prepareTransaction(ctx context.Context, request *TxRequest, nonce uint64, boostPercent int) (tx *types.Transaction, err error) {
	var gasLimit uint64
	if request.GasLimit == 0 {
		// Estimate gas using pending state for consistency with PendingNonceAt
		gasLimit, err = t.backend.EstimateGas(ctx, ethereum.CallMsg{
			From:  t.sender,
			To:    request.To,
			Data:  request.Data,
			Value: request.Value,
		})

		if err != nil {
			t.logger.Warning("gas estimation failed, using fallback",
				"error", err,
				"description", request.Description,
			)

			if request.MinEstimatedGasLimit > 0 {
				gasLimit = request.MinEstimatedGasLimit
			} else if len(request.Data) > 0 {
				// Contract call - use configured fallback
				gasLimit = t.fallbackGasLimit
			} else {
				// Simple transfer - use minimum
				gasLimit = MinGasLimit
			}
		} else {
			// Estimation succeeded - add buffer for state changes
			gasLimit += gasLimit * GasBufferPercent / 100

			// Apply minimum if specified
			if gasLimit < request.MinEstimatedGasLimit {
				gasLimit = request.MinEstimatedGasLimit
			}

			// Cap at maximum
			if gasLimit > MaxGasLimit {
				gasLimit = MaxGasLimit
			}
		}

		// Ensure absolute minimum
		if gasLimit < MinGasLimit {
			gasLimit = MinGasLimit
		}
	} else {
		// Use provided gas limit with bounds validation
		gasLimit = min(max(request.GasLimit, MinGasLimit), MaxGasLimit)
	}

	if gasLimit == 0 {
		return nil, errors.New("gas limit cannot be zero")
	}

	/*
		Transactions are EIP 1559 dynamic transactions where there are three fee related fields:
			1. base fee is the price that will be burned as part of the transaction.
			2. max fee is the max price we are willing to spend as gas price.
			3. max priority fee is max price want to give to the miner to prioritize the transaction.
		as an example:
		if base fee is 15, max fee is 20, and max priority is 3, gas price will be 15 + 3 = 18
		if base is 15, max fee is 20, and max priority fee is 10,
		gas price will be 15 + 10 = 25, but since 25 > 20, gas price is 20.
		notice that gas price does not exceed 20 as defined by max fee.
	*/

	gasFeeCap, gasTipCap, err := t.backend.SuggestedFeeAndTip(ctx, request.GasPrice, boostPercent)
	if err != nil {
		return nil, err
	}

	t.logger.Debug("prepared transaction",
		"to", request.To,
		"value", request.Value,
		"gas_limit", gasLimit,
		"gas_fee_cap", gasFeeCap,
		"gas_tip_cap", gasTipCap,
		"nonce", nonce,
		"description", request.Description,
	)

	return types.NewTx(&types.DynamicFeeTx{
		Nonce:     nonce,
		ChainID:   t.chainID,
		To:        request.To,
		Value:     request.Value,
		Gas:       gasLimit,
		GasFeeCap: gasFeeCap,
		GasTipCap: gasTipCap,
		Data:      request.Data,
	}), nil
}

func storedTransactionKey(txHash common.Hash) string {
	return fmt.Sprintf("%s%x", storedTransactionPrefix, txHash)
}

func pendingTransactionKey(txHash common.Hash) string {
	return fmt.Sprintf("%s%x", pendingTransactionPrefix, txHash)
}

func (t *transactionService) nextNonce(ctx context.Context) (uint64, error) {
	onchainNonce, err := t.backend.PendingNonceAt(ctx, t.sender)
	if err != nil {
		return 0, err
	}

	pendingTxs, err := t.PendingTransactions()
	if err != nil {
		return 0, err
	}

	pending := t.filterPendingTransactions(t.ctx, pendingTxs)

	// PendingNonceAt returns the nonce we should use, but we will
	// compare this to our pending tx list, therefore the -1.
	maxNonce := onchainNonce - 1
	for txHash, trx := range pending {
		if trx == nil {
			t.logger.Warning("pending transaction data unavailable, relying on onchain nonce", "tx", txHash)
			continue
		}
		maxNonce = max(maxNonce, trx.Nonce())
	}

	return maxNonce + 1, nil
}

// WaitForReceipt waits until either the transaction with the given hash has
// been mined or the context is cancelled.
func (t *transactionService) WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error) {
	receiptC, errC, err := t.WatchSentTransaction(txHash)
	if err != nil {
		return nil, err
	}
	select {
	case receipt := <-receiptC:
		return &receipt, nil
	case err := <-errC:
		return nil, err
	// don't wait longer than the context that was passed in
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *transactionService) WatchSentTransaction(txHash common.Hash) (<-chan types.Receipt, <-chan error, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// loading the tx here guarantees it was in fact sent from this transaction service
	// also it allows us to avoid having to load the transaction during the watch loop
	storedTransaction, err := t.StoredTransaction(txHash)
	if err != nil {
		return nil, nil, err
	}

	return t.monitor.WatchTransaction(txHash, storedTransaction.Nonce)
}

func (t *transactionService) PendingTransactions() ([]common.Hash, error) {
	txHashes := make([]common.Hash, 0)
	err := t.store.Iterate(pendingTransactionPrefix, func(key, value []byte) (stop bool, err error) {
		txHash := common.HexToHash(strings.TrimPrefix(string(key), pendingTransactionPrefix))
		txHashes = append(txHashes, txHash)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return txHashes, nil
}

// filterPendingTransactions will filter supplied transaction hashes removing those that are not pending anymore.
// Removed transactions will be also removed from store.
// Returns the pending transactions keyed by hash.
func (t *transactionService) filterPendingTransactions(ctx context.Context, txHashes []common.Hash) map[common.Hash]*types.Transaction {
	result := make(map[common.Hash]*types.Transaction, len(txHashes))

	for _, txHash := range txHashes {
		trx, isPending, err := t.backend.TransactionByHash(ctx, txHash)
		// When error occurres consider transaction as pending (so this transaction won't be filtered out),
		// unless it was not found
		if err != nil {
			if errors.Is(err, ethereum.NotFound) {
				if ambiguousTx := t.recentAmbiguousTransaction(txHash); ambiguousTx != nil {
					// sent with an error and not known to the backend:
					// keep counting its nonce for a while
					result[txHash] = ambiguousTx
					continue
				}
				t.logger.Error(err, "pending transactions not found", "tx", txHash)

				isPending = false
			} else {
				isPending = true
			}
		}

		if isPending {
			result[txHash] = trx
		} else {
			err := t.store.Delete(pendingTransactionKey(txHash))
			if err != nil {
				t.logger.Error(err, "error while unregistering transaction as pending", "tx", txHash)
			}
		}
	}

	return result
}

// recentAmbiguousTransaction returns the stored signed transaction if the
// pending key marks it as ambiguous and it was created less than
// AmbiguousPendingTimeout ago; otherwise nil.
func (t *transactionService) recentAmbiguousTransaction(txHash common.Hash) *types.Transaction {
	var pending pendingTransaction
	if err := t.store.Get(pendingTransactionKey(txHash), &pending); err != nil || !pending.Ambiguous {
		return nil
	}
	stored, err := t.StoredTransaction(txHash)
	if err != nil || len(stored.Raw) == 0 {
		return nil
	}
	if time.Since(time.Unix(stored.Created, 0)) >= AmbiguousPendingTimeout {
		return nil
	}
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(stored.Raw); err != nil {
		return nil
	}
	return tx
}

// RebroadcastTransaction sends the stored signed transaction again.
func (t *transactionService) RebroadcastTransaction(ctx context.Context, txHash common.Hash) error {
	stored, err := t.StoredTransaction(txHash)
	if err != nil {
		return err
	}
	if len(stored.Raw) == 0 {
		return ErrNoRawTransaction
	}
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(stored.Raw); err != nil {
		return err
	}
	if tx.Hash() != txHash {
		return fmt.Errorf("stored transaction %s decodes to hash %s", txHash, tx.Hash())
	}
	return t.backend.SendTransaction(ctx, tx)
}

// ReplaceTransaction sends a same-nonce replacement with raised fees.
func (t *transactionService) ReplaceTransaction(ctx context.Context, txHash common.Hash, beforeBroadcast BeforeBroadcastFunc) (common.Hash, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	stored, err := t.StoredTransaction(txHash)
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}

	gasFeeCap, gasTipCap, err := t.backend.SuggestedFeeAndTip(ctx, sctx.GetGasPrice(ctx), stored.GasTipBoost)
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}
	gasFeeCap = bigMax(gasFeeCap, bumped(stored.GasFeeCap))
	gasTipCap = bigMax(gasTipCap, bumped(stored.GasTipCap))
	gasFeeCap = bigMax(gasFeeCap, gasTipCap)

	signedTx, err := t.signer.SignTx(types.NewTx(&types.DynamicFeeTx{
		Nonce:     stored.Nonce,
		ChainID:   t.chainID,
		To:        stored.To,
		Value:     stored.Value,
		Gas:       stored.GasLimit,
		GasTipCap: gasTipCap,
		GasFeeCap: gasFeeCap,
		Data:      stored.Data,
	}), t.chainID)
	if err != nil {
		return common.Hash{}, notBroadcast(err)
	}

	return t.storeAndBroadcast(ctx, signedTx, stored.GasTipBoost, fmt.Sprintf("%s (replacement)", stored.Description), beforeBroadcast)
}

// TransactionStatus looks the transaction up on chain.
func (t *transactionService) TransactionStatus(ctx context.Context, txHash common.Hash) (TxState, error) {
	receipt, err := t.backend.TransactionReceipt(ctx, txHash)
	switch {
	case err == nil && receipt != nil:
		if receipt.Status == types.ReceiptStatusSuccessful {
			return TxMined, nil
		}
		return TxReverted, nil
	case err != nil && !errors.Is(err, ethereum.NotFound):
		return 0, err
	}

	_, _, err = t.backend.TransactionByHash(ctx, txHash)
	if err == nil {
		// known to the backend; a mined one without its receipt yet is
		// pending too
		return TxPending, nil
	}
	if !errors.Is(err, ethereum.NotFound) {
		return 0, err
	}

	stored, err := t.StoredTransaction(txHash)
	if err != nil {
		if errors.Is(err, ErrUnknownTransaction) {
			return TxNotFound, nil
		}
		return 0, err
	}
	nonce, err := t.backend.NonceAt(ctx, t.sender, nil)
	if err != nil {
		return 0, err
	}
	if nonce > stored.Nonce {
		return TxCancelled, nil
	}
	return TxNotFound, nil
}

// bumped raises v by ReplacementBumpPercent, rounded up, so that a small
// value is raised too and the replacement is not rejected as underpriced.
func bumped(v *big.Int) *big.Int {
	if v == nil {
		return new(big.Int)
	}
	n := new(big.Int).Mul(v, big.NewInt(100+ReplacementBumpPercent))
	n.Add(n, big.NewInt(99))
	return n.Div(n, big.NewInt(100))
}

func bigMax(a, b *big.Int) *big.Int {
	if a == nil || a.Cmp(b) < 0 {
		return new(big.Int).Set(b)
	}
	return a
}

func (t *transactionService) ResendTransaction(ctx context.Context, txHash common.Hash) error {
	storedTransaction, err := t.StoredTransaction(txHash)
	if err != nil {
		return err
	}

	gasFeeCap, gasTipCap, err := t.backend.SuggestedFeeAndTip(ctx, sctx.GetGasPrice(ctx), storedTransaction.GasTipBoost)
	if err != nil {
		return err
	}

	tx := types.NewTx(&types.DynamicFeeTx{
		Nonce:     storedTransaction.Nonce,
		ChainID:   t.chainID,
		To:        storedTransaction.To,
		Value:     storedTransaction.Value,
		Gas:       storedTransaction.GasLimit,
		GasTipCap: gasTipCap,
		GasFeeCap: gasFeeCap,
		Data:      storedTransaction.Data,
	})

	signedTx, err := t.signer.SignTx(tx, t.chainID)
	if err != nil {
		return err
	}

	if signedTx.Hash() != txHash {
		return errors.New("transaction hash changed")
	}

	err = t.backend.SendTransaction(t.ctx, signedTx)
	if err != nil {
		if strings.Contains(err.Error(), "already imported") {
			return ErrAlreadyImported
		}
	}
	return nil
}

func (t *transactionService) CancelTransaction(ctx context.Context, originalTxHash common.Hash) (common.Hash, error) {
	storedTransaction, err := t.StoredTransaction(originalTxHash)
	if err != nil {
		return common.Hash{}, err
	}

	gasFeeCap, gasTipCap, err := t.backend.SuggestedFeeAndTip(ctx, sctx.GetGasPrice(ctx), 0)
	if err != nil {
		return common.Hash{}, err
	}

	if gasFeeCap.Cmp(storedTransaction.GasFeeCap) <= 0 {
		gasFeeCap = storedTransaction.GasFeeCap
	}

	if gasTipCap.Cmp(storedTransaction.GasTipCap) <= 0 {
		gasTipCap = storedTransaction.GasTipCap
	}

	gasTipCap = new(big.Int).Div(new(big.Int).Mul(big.NewInt(int64(10)+100), gasTipCap), big.NewInt(100))

	gasFeeCap.Add(gasFeeCap, gasTipCap)

	signedTx, err := t.signer.SignTx(types.NewTx(&types.DynamicFeeTx{
		Nonce:     storedTransaction.Nonce,
		ChainID:   t.chainID,
		To:        &t.sender,
		Value:     big.NewInt(0),
		Gas:       21000,
		GasTipCap: gasTipCap,
		GasFeeCap: gasFeeCap,
		Data:      []byte{},
	}), t.chainID)
	if err != nil {
		return common.Hash{}, err
	}

	err = t.backend.SendTransaction(t.ctx, signedTx)
	if err != nil {
		return common.Hash{}, err
	}

	txHash := signedTx.Hash()
	raw, err := signedTx.MarshalBinary()
	if err != nil {
		return common.Hash{}, err
	}
	err = t.store.Put(storedTransactionKey(txHash), StoredTransaction{
		To:          signedTx.To(),
		Data:        signedTx.Data(),
		GasPrice:    signedTx.GasPrice(),
		GasLimit:    signedTx.Gas(),
		GasFeeCap:   signedTx.GasFeeCap(),
		GasTipBoost: storedTransaction.GasTipBoost,
		GasTipCap:   signedTx.GasTipCap(),
		Value:       signedTx.Value(),
		Nonce:       signedTx.Nonce(),
		Created:     time.Now().Unix(),
		Description: fmt.Sprintf("%s (cancellation)", storedTransaction.Description),
		Raw:         raw,
	})
	if err != nil {
		return common.Hash{}, err
	}

	err = t.store.Put(pendingTransactionKey(txHash), struct{}{})
	if err != nil {
		return common.Hash{}, err
	}

	t.waitForPendingTx(txHash)

	return txHash, err
}

func (t *transactionService) Close() error {
	t.cancel()
	t.wg.Wait()
	return nil
}

func (t *transactionService) TransactionFee(ctx context.Context, txHash common.Hash) (*big.Int, error) {
	trx, _, err := t.backend.TransactionByHash(ctx, txHash)
	if err != nil {
		return nil, err
	}
	return trx.Cost(), nil
}

func (t *transactionService) UnwrapABIError(ctx context.Context, req *TxRequest, err error, abiErrors map[string]abi.Error) error {
	if err == nil {
		return nil
	}

	_, cErr := t.Call(ctx, req)
	if cErr == nil {
		return err
	}
	err = fmt.Errorf("%w: %s", err, cErr) //nolint:errorlint

	var derr rpc.DataError
	if !errors.As(cErr, &derr) {
		return err
	}

	res, ok := derr.ErrorData().(string)
	if !ok {
		return err
	}
	buf := common.FromHex(res)

	if reason, uErr := abi.UnpackRevert(buf); uErr == nil {
		return fmt.Errorf("%w: %s", err, reason)
	}

	// Revert data must be at least a 4-byte error selector; some RPC providers
	// return empty ("0x") or malformed data for a reverted transaction, which
	// would otherwise panic on the buf[:4] slice below.
	if len(buf) < 4 {
		return err
	}

	for _, abiError := range abiErrors {
		if !bytes.Equal(buf[:4], abiError.ID[:4]) {
			continue
		}

		data, uErr := abiError.Unpack(buf)
		if uErr != nil {
			continue
		}

		values, ok := data.([]any)
		if !ok {
			values = make([]any, len(abiError.Inputs))
			for i := range values {
				values[i] = "?"
			}
		}

		params := make([]string, len(abiError.Inputs))
		for i, input := range abiError.Inputs {
			if input.Name == "" {
				input.Name = fmt.Sprintf("arg%d", i)
			}
			params[i] = fmt.Sprintf("%s=%v", input.Name, values[i])

		}

		return fmt.Errorf("%w: %s(%s)", err, abiError.Name, strings.Join(params, ","))
	}

	return err
}
