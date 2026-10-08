// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethersphere/bee/v2/pkg/config"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/postage/postagecontract"
	"github.com/ethersphere/bee/v2/pkg/settlement"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/chequebook"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/priceoracle"
	"github.com/ethersphere/bee/v2/pkg/settlement/swap/swapprotocol"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendnoop"
	"github.com/ethersphere/bee/v2/pkg/transaction/failover"
	"github.com/ethersphere/bee/v2/pkg/transaction/wrapped"
)

const (
	maxDelay                = 1 * time.Minute
	cancellationDepth       = 12
	additionalConfirmations = 2
)

// BlockchainRPCConfig holds the configuration parameters for the blockchain RPC client transport.
type BlockchainRPCConfig struct {
	// Endpoints in priority order. One is the ordinary case; more than one
	// enables failover. See issue #109.
	Endpoints   []string
	DialTimeout time.Duration
	TLSTimeout  time.Duration
	IdleTimeout time.Duration
	Keepalive   time.Duration
}

// InitChain will initialize the Ethereum backend at the given endpoint and
// set up the Transaction Service to interact with it using the provided signer.
func InitChain(
	ctx context.Context,
	logger log.Logger,
	stateStore storage.StateStorer,
	chainID int64,
	signer crypto.Signer,
	pollingInterval time.Duration,
	chainEnabled bool,
	minimumGasTipCap uint64,
	fallbackGasLimit uint64,
	rpcCfg BlockchainRPCConfig,
	blockSyncInterval uint64,
) (transaction.Backend, common.Address, int64, transaction.Monitor, transaction.Service, error) {
	backend := backendnoop.New(chainID)

	if chainEnabled {
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   rpcCfg.DialTimeout,
				KeepAlive: rpcCfg.Keepalive,
			}).DialContext,
			TLSHandshakeTimeout:   rpcCfg.TLSTimeout,
			IdleConnTimeout:       rpcCfg.IdleTimeout,
			MaxIdleConns:          100,
			ForceAttemptHTTP2:     true,
			ExpectContinueTimeout: 1 * time.Second,
		}

		// Dial every configured endpoint. One unreachable endpoint is no longer
		// fatal when others answer, which is the point of #109: a provider
		// outage used to take the node down ten minutes later and keep it down.
		// Since #583 a stall marks the batch store stale instead of stopping
		// the node, so an outage of every endpoint degrades it.
		//
		// An endpoint that does not answer now keeps its place in the list with
		// no connection, rather than being dropped. Dropping it meant a
		// preferred endpoint that happened to be down at boot was never
		// reconsidered, and the operator had to notice and restart to get back
		// to the endpoint they chose. See issue #119.
		var (
			// Every configured endpoint ends up in this list now, connected or
			// not, so its length is known in advance.
			endpoints = make([]failover.Endpoint, 0, len(rpcCfg.Endpoints))
			dialErrs  []error
			connected int
		)
		for _, url := range rpcCfg.Endpoints {
			dial := dialEndpoint(url, transport, chainID, minimumGasTipCap, pollingInterval, blockSyncInterval)

			backend, err := dial(ctx)
			if err != nil {
				logger.Warning("could not reach a blockchain rpc endpoint; "+
					"keeping it in the list and retrying in the background",
					"endpoint", url, "error", err)
				dialErrs = append(dialErrs, err)
			} else {
				logger.Info("connected to blockchain backend", "endpoint", url)
				connected++
			}

			endpoints = append(endpoints, failover.Endpoint{
				Name:    url,
				Backend: backend,
				Dial:    dial,
			})
		}

		if connected == 0 {
			logger.Info("could not connect to any blockchain backend; "+
				"in a swap-enabled network a working blockchain node "+
				"(for xDAI network in production, SepoliaETH in testnet) is required; "+
				"check your node or specify another using --blockchain-rpc-endpoint.",
				"endpoints", rpcCfg.Endpoints)
			return nil, common.Address{}, 0, nil, nil, fmt.Errorf("no usable blockchain rpc endpoint: %w", errors.Join(dialErrs...))
		}

		// Every endpoint must agree on the network. A mismatch is a
		// configuration error, not a health problem, and failing over to it
		// would silently put the node on a different chain.
		endpoints, err := verifySameChain(ctx, logger, endpoints, chainID)
		if err != nil {
			for _, ep := range endpoints {
				if ep.Backend != nil {
					ep.Backend.Close()
				}
			}
			return nil, common.Address{}, 0, nil, nil, err
		}

		// One configured endpoint keeps the plain backend: the failover wrapper
		// would add indirection and nothing else, since Recover has nowhere
		// better to move to. The nil check matters because verifySameChain can
		// drop a connection it could not question, and a nil backend here would
		// be dereferenced two lines later rather than reported.
		if len(endpoints) == 1 && endpoints[0].Backend != nil {
			backend = endpoints[0].Backend
		} else {
			fb, err := failover.New(logger, failover.DefaultMaxBlockLag, endpoints...)
			if err != nil {
				return nil, common.Address{}, 0, nil, nil, fmt.Errorf("failover backend: %w", err)
			}
			go fb.Recover(ctx, 0)
			logger.Info("blockchain rpc failover enabled", "endpoints", len(endpoints), "active", fb.Active())
			backend = fb
		}
	}

	backendChainID, err := backend.ChainID(ctx)
	if err != nil {
		return nil, common.Address{}, 0, nil, nil, fmt.Errorf("getting chain id: %w", err)
	}

	if chainID != -1 && chainID != backendChainID.Int64() {
		return nil, common.Address{}, 0, nil, nil, fmt.Errorf("connected to wrong network: expected chain id %d, got %d", chainID, backendChainID.Int64())
	}

	overlayEthAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, common.Address{}, 0, nil, nil, fmt.Errorf("blockchain address: %w", err)
	}

	transactionMonitor := transaction.NewMonitor(logger, backend, overlayEthAddress, pollingInterval, cancellationDepth)

	transactionService, err := transaction.NewService(logger, overlayEthAddress, backend, signer, stateStore, backendChainID, transactionMonitor, fallbackGasLimit)
	if err != nil {
		return nil, common.Address{}, 0, nil, nil, fmt.Errorf("transaction service: %w", err)
	}

	return backend, overlayEthAddress, backendChainID.Int64(), transactionMonitor, transactionService, nil
}

// verifySameChain rejects a configuration whose endpoints disagree about which
// network they are on.
//
// A mismatch is a configuration error, not a health problem — almost certainly a
// mistyped URL — and it must be fatal rather than a reason to fail over. Failing
// over between chains would corrupt everything downstream of the backend.
// It returns the endpoints that answered. An endpoint that cannot be asked is
// dropped rather than fatal, matching how a failed dial is treated: tolerating a
// dead endpoint at dial time but not one moment later would be an odd place to
// draw the line. Only a disagreement is fatal.
func verifySameChain(ctx context.Context, logger log.Logger, endpoints []failover.Endpoint, want int64) ([]failover.Endpoint, error) {
	var (
		usable []failover.Endpoint
		first  int64
		seen   bool
	)
	for _, ep := range endpoints {
		if ep.Backend == nil {
			// Not reachable now, so there is nothing to ask. It keeps its place
			// and its Dial, which verifies the chain itself before the endpoint
			// can ever be adopted.
			usable = append(usable, ep)
			continue
		}

		got, err := ep.Backend.ChainID(ctx)
		if err != nil {
			logger.Warning("blockchain rpc endpoint did not report a chain id, "+
				"dropping the connection but keeping the endpoint",
				"endpoint", ep.Name, "error", err)
			ep.Backend.Close()
			ep.Backend = nil
			usable = append(usable, ep)
			continue
		}
		id := got.Int64()

		if want != -1 && id != want {
			return nil, fmt.Errorf("endpoint %s is on chain %d, but this node is configured for %d",
				ep.Name, id, want)
		}
		if seen && id != first {
			return nil, fmt.Errorf("endpoints disagree on the network: one reports chain %d, %s reports %d",
				first, ep.Name, id)
		}
		first, seen = id, true
		usable = append(usable, ep)
	}
	if !seen {
		return nil, errors.New("no blockchain rpc endpoint reported a chain id")
	}
	return usable, nil
}

// dialEndpoint returns a function that opens a connection to url and refuses it
// unless it is on the expected chain.
//
// The chain check lives here, in the dial, rather than only in the startup
// sweep. An endpoint that was down at boot is adopted later by the failover
// backend, long after the startup check has run, and nothing would look at it
// again. A mistyped URL that happened to be unreachable at boot would then be
// taken up an hour later on whatever chain it is really on.
//
// It is not fatal here, unlike at startup. Refusing to start because a
// configuration is wrong is right; killing a running node because a standby
// came back on the wrong chain is not, and the node is working on another
// endpoint at that moment. The mismatch is logged and the endpoint is left
// unconnected.
func dialEndpoint(
	url string,
	transport *http.Transport,
	wantChainID int64,
	minimumGasTipCap uint64,
	pollingInterval time.Duration,
	blockSyncInterval uint64,
) func(context.Context) (transaction.Backend, error) {
	return func(ctx context.Context) (transaction.Backend, error) {
		rpcClient, err := rpc.DialOptions(ctx, url, rpc.WithHTTPClient(&http.Client{Transport: transport}))
		if err != nil {
			return nil, fmt.Errorf("%s: dial: %w", url, err)
		}

		var versionString string
		if err := rpcClient.CallContext(ctx, &versionString, "web3_clientVersion"); err != nil {
			rpcClient.Close()
			return nil, fmt.Errorf("%s: client version: %w", url, err)
		}

		backend := wrapped.NewBackend(ethclient.NewClient(rpcClient), minimumGasTipCap, pollingInterval, blockSyncInterval)

		if wantChainID != -1 {
			got, err := backend.ChainID(ctx)
			if err != nil {
				backend.Close()
				return nil, fmt.Errorf("%s: chain id: %w", url, err)
			}
			if got.Int64() != wantChainID {
				backend.Close()
				return nil, fmt.Errorf("%s: is on chain %d, but this node is configured for %d",
					url, got.Int64(), wantChainID)
			}
		}

		return backend, nil
	}
}

// InitChequebookFactory will initialize the chequebook factory with the given
// chain backend.
func InitChequebookFactory(logger log.Logger, backend transaction.Backend, chainID int64, transactionService transaction.Service, factoryAddress string) (chequebook.Factory, error) {
	var currentFactory common.Address

	chainCfg, found := config.GetByChainID(chainID)

	foundFactory := chainCfg.CurrentFactoryAddress
	if factoryAddress == "" {
		if !found {
			return nil, fmt.Errorf("no known factory address for this network (chain id: %d)", chainID)
		}
		currentFactory = foundFactory
		logger.Info("using default factory address", "chain_id", chainID, "factory_address", currentFactory)
	} else if !common.IsHexAddress(factoryAddress) {
		return nil, errors.New("malformed factory address")
	} else {
		currentFactory = common.HexToAddress(factoryAddress)
		logger.Info("using custom factory address", "factory_address", currentFactory)
	}

	return chequebook.NewFactory(backend, transactionService, currentFactory), nil
}

// InitChequebookService will initialize the chequebook service with the given
// chequebook factory and chain backend.
func InitChequebookService(
	ctx context.Context,
	logger log.Logger,
	stateStore storage.StateStorer,
	signer crypto.Signer,
	chainID int64,
	backend transaction.Backend,
	overlayEthAddress common.Address,
	transactionService transaction.Service,
	chequebookFactory chequebook.Factory,
	initialDeposit string,
	erc20Service erc20.Service,
) (chequebook.Service, error) {
	chequeSigner := chequebook.NewChequeSigner(signer, chainID)

	deposit, ok := new(big.Int).SetString(initialDeposit, 10)
	if !ok {
		return nil, fmt.Errorf("initial swap deposit \"%s\" cannot be parsed", initialDeposit)
	}

	chequebookService, err := chequebook.Init(
		ctx,
		chequebookFactory,
		stateStore,
		logger,
		deposit,
		transactionService,
		backend,
		chainID,
		overlayEthAddress,
		chequeSigner,
		erc20Service,
	)
	if err != nil {
		return nil, fmt.Errorf("chequebook init: %w", err)
	}

	return chequebookService, nil
}

func initChequeStoreCashout(
	stateStore storage.StateStorer,
	swapBackend transaction.Backend,
	chequebookFactory chequebook.Factory,
	chainID int64,
	overlayEthAddress common.Address,
	transactionService transaction.Service,
) (chequebook.ChequeStore, chequebook.CashoutService) {
	chequeStore := chequebook.NewChequeStore(
		stateStore,
		chequebookFactory,
		chainID,
		overlayEthAddress,
		transactionService,
		chequebook.RecoverCheque,
	)

	cashout := chequebook.NewCashoutService(
		stateStore,
		swapBackend,
		transactionService,
		chequeStore,
	)

	return chequeStore, cashout
}

// InitSwap will initialize and register the swap service.
func InitSwap(
	p2ps *libp2p.Service,
	logger log.Logger,
	stateStore storage.StateStorer,
	networkID uint64,
	overlayEthAddress common.Address,
	chequebookService chequebook.Service,
	chequeStore chequebook.ChequeStore,
	cashoutService chequebook.CashoutService,
	accounting settlement.Accounting,
	priceOracleAddress string,
	chainID int64,
	transactionService transaction.Service,
) (*swap.Service, priceoracle.Service, error) {
	var currentPriceOracleAddress common.Address
	if priceOracleAddress == "" {
		chainCfg, found := config.GetByChainID(chainID)
		currentPriceOracleAddress = chainCfg.SwapPriceOracleAddress
		if !found {
			return nil, nil, errors.New("no known price oracle address for this network")
		}
	} else {
		currentPriceOracleAddress = common.HexToAddress(priceOracleAddress)
	}

	priceOracle := priceoracle.New(logger, currentPriceOracleAddress, transactionService, 300)
	priceOracle.Start()
	swapProtocol := swapprotocol.New(p2ps, logger, overlayEthAddress, priceOracle)
	swapAddressBook := swap.NewAddressbook(stateStore)

	cashoutAddress := overlayEthAddress
	if chequebookService != nil {
		cashoutAddress = chequebookService.Address()
	}

	swapService := swap.New(
		swapProtocol,
		logger,
		stateStore,
		chequebookService,
		chequeStore,
		swapAddressBook,
		networkID,
		cashoutService,
		accounting,
		cashoutAddress,
	)

	swapProtocol.SetSwap(swapService)

	err := p2ps.AddProtocol(swapProtocol.Protocol())
	if err != nil {
		return nil, nil, err
	}

	return swapService, priceOracle, nil
}

func GetTxHash(stateStore storage.StateStorer, logger log.Logger, trxString string) ([]byte, error) {
	if trxString != "" {
		txHashTrimmed := strings.TrimPrefix(trxString, "0x")
		if len(txHashTrimmed) != 64 {
			return nil, errors.New("invalid length")
		}
		txHash, err := hex.DecodeString(txHashTrimmed)
		if err != nil {
			return nil, err
		}
		logger.Info("using the provided transaction hash", "tx_hash", txHashTrimmed)
		return txHash, nil
	}

	var txHash common.Hash
	key := chequebook.ChequebookDeploymentKey
	if err := stateStore.Get(key, &txHash); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, errors.New("chequebook deployment transaction hash not found, please specify the transaction hash manually")
		}
		return nil, err
	}

	logger.Info("using the chequebook transaction hash", "tx_hash", txHash)
	return txHash.Bytes(), nil
}

func GetTxNextBlock(ctx context.Context, logger log.Logger, backend transaction.Backend, monitor transaction.Monitor, duration time.Duration, trx []byte, blockHash string) ([]byte, error) {
	if blockHash != "" {
		blockHashTrimmed := strings.TrimPrefix(blockHash, "0x")
		if len(blockHashTrimmed) != 64 {
			return nil, errors.New("invalid length")
		}
		blockHash, err := hex.DecodeString(blockHashTrimmed)
		if err != nil {
			return nil, err
		}
		logger.Info("using the provided block hash", "block_hash", hex.EncodeToString(blockHash))
		return blockHash, nil
	}

	block, err := transaction.WaitBlockAfterTransaction(ctx, backend, duration, common.BytesToHash(trx), additionalConfirmations)
	if err != nil {
		return nil, err
	}

	hash := block.Hash()
	hashBytes := hash.Bytes()

	logger.Info("using the next block hash from the blockchain", "block_hash", hex.EncodeToString(hashBytes))

	return hashBytes, nil
}

// noOpChequebookService is a noOp implementation for chequebook.Service interface.
type noOpChequebookService struct{}

func (m *noOpChequebookService) Deposit(context.Context, *big.Int) (hash common.Hash, err error) {
	return hash, postagecontract.ErrChainDisabled
}

func (m *noOpChequebookService) Withdraw(context.Context, *big.Int) (hash common.Hash, err error) {
	return hash, postagecontract.ErrChainDisabled
}

func (m *noOpChequebookService) WaitForDeposit(context.Context, common.Hash) error {
	return postagecontract.ErrChainDisabled
}

func (m *noOpChequebookService) Balance(context.Context) (*big.Int, error) {
	return nil, postagecontract.ErrChainDisabled
}

func (m *noOpChequebookService) AvailableBalance(context.Context) (*big.Int, error) {
	return nil, postagecontract.ErrChainDisabled
}

func (m *noOpChequebookService) Address() common.Address {
	return common.Address{}
}

func (m *noOpChequebookService) Issue(context.Context, common.Address, *big.Int, chequebook.SendChequeFunc) (*big.Int, error) {
	return nil, postagecontract.ErrChainDisabled
}

func (m *noOpChequebookService) LastCheque(common.Address) (*chequebook.SignedCheque, error) {
	return nil, postagecontract.ErrChainDisabled
}

func (m *noOpChequebookService) LastCheques() (map[common.Address]*chequebook.SignedCheque, error) {
	return nil, postagecontract.ErrChainDisabled
}
