// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmd

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethersphere/bee/v2"
	"github.com/ethersphere/bee/v2/pkg/accesscontrol"
	"github.com/ethersphere/bee/v2/pkg/bmt"
	"github.com/ethersphere/bee/v2/pkg/bmtpool"
	chaincfg "github.com/ethersphere/bee/v2/pkg/config"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/keccak"
	"github.com/ethersphere/bee/v2/pkg/keystore"
	filekeystore "github.com/ethersphere/bee/v2/pkg/keystore/file"
	memkeystore "github.com/ethersphere/bee/v2/pkg/keystore/mem"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/node"
	"github.com/ethersphere/bee/v2/pkg/resolver/multiresolver"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/kardianos/service"
	"github.com/spf13/cobra"
)

const (
	serviceName      = "SwarmBeeSvc"
	libp2pPKFilename = "libp2p_v2"
)

//go:embed bee-welcome-message.txt
var beeWelcomeMessage string

func (c *command) initStartCmd() (err error) {
	cmd := &cobra.Command{
		Use:               "start",
		Short:             "Start a Swarm node",
		PersistentPreRunE: c.CheckUnknownParams,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) > 0 {
				return cmd.Help()
			}

			logger := c.logger

			fmt.Print(beeWelcomeMessage)
			// This is not upstream Bee. Say so at boot: an operator reading logs
			// months from now must be able to tell what they are running, and
			// which upstream release it derives from.
			logger.Warning("this is wasp, an unofficial experimental build of Bee; it is not affiliated with or supported by the Swarm Foundation, and carries changes not reviewed by the Bee maintainers")
			logger.Info("wasp version", "version", bee.Version, "upstream_base", bee.UpstreamBase)

			// ctx is global context of bee node; which is canceled after interrupt signal is received.
			ctx, cancel := context.WithCancel(context.Background())
			sysInterruptChannel := make(chan os.Signal, 1)
			signal.Notify(sysInterruptChannel, syscall.SIGINT, syscall.SIGTERM)

			go func() {
				select {
				case <-sysInterruptChannel:
					logger.Info("received interrupt signal")
					cancel()
				case <-ctx.Done():
				}
			}()

			// Building bee node can take up some time (because node.NewBee(...) is compute have function )
			// Because of this we need to do it in background so that program could be terminated when interrupt signal is received
			// while bee node is being constructed.
			respC := buildBeeNodeAsync(ctx, c, cmd, logger)
			var beeNode atomic.Value

			// A failed build used to be logged here and nothing more, so RunE
			// returned nil and the process exited 0. systemd then reported
			// "Deactivated successfully" and Restart=always restarted it every
			// few seconds, so a bad configuration value looped indefinitely
			// while systemctl is-active still said active (issue #490).
			// The error is kept and returned, so the process exits non-zero and
			// main can map a configuration error to its own status.
			var buildErr atomic.Value

			p := &program{
				start: func() {
					// Wait for bee node to fully build and initialized
					select {
					case resp := <-respC:
						if resp.err != nil {
							logger.Error(resp.err, "failed to build bee node")
							buildErr.Store(resp.err)
							return
						}
						beeNode.Store(resp.bee)
					case <-ctx.Done():
						return
					}

					// Bee has fully started at this point, from now on we
					// block main goroutine until it is interrupted or stopped
					select {
					case <-ctx.Done():
					case <-beeNode.Load().(*node.Bee).SyncingStopped():
						logger.Debug("syncing has stopped")
					}

					logger.Info("shutting down...")
				},
				stop: func() {
					// Whenever program is being stopped we need to cancel main context
					// beforehand so that node could be stopped via Shutdown method
					cancel()

					// Shutdown node (if node was fully started)
					val := beeNode.Load()
					if val == nil {
						return
					}

					done := make(chan struct{})
					go func(beeNode *node.Bee) {
						defer close(done)

						if err := beeNode.Shutdown(); err != nil {
							logger.Error(err, "shutdown failed")
						}
					}(val.(*node.Bee))

					// If shutdown function is blocking too long,
					// allow process termination by receiving another signal.
					select {
					case <-sysInterruptChannel:
						logger.Info("node shutdown terminated")
					case <-done:
						logger.Info("node shutdown")
					}

					// Last, after the final log line: the sink buffers, so
					// without this the lines an operator most wants — the ones
					// explaining why the node stopped — are still in the queue
					// when the process exits. Bounded internally, so a log
					// consumer that has gone away cannot hold the exit open.
					if err := log.CloseAsyncSinks(); err != nil {
						fmt.Fprintln(os.Stderr, err)
					}
				},
			}

			if c.isWindowsService {
				s, err := service.New(p, &service.Config{
					Name:        serviceName,
					DisplayName: "Bee",
					Description: "Bee, Swarm client.",
				})
				if err != nil {
					return err
				}

				if err = s.Run(); err != nil {
					return err
				}
			} else {
				// start blocks until some interrupt is received
				p.start()
				p.stop()
			}

			// Returned after stop() so shutdown still runs and the async log
			// sinks are still closed: the exit status changes, the teardown
			// does not.
			if err, ok := buildErr.Load().(error); ok && err != nil {
				return err
			}

			return nil
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := c.preRun(cmd); err != nil {
				return err
			}
			c.bindBlockchainRpcConfig(cmd)
			c.bindTracingConfig(cmd)
			c.warnDeprecatedTracingKeys()
			c.warnTracingTLSWithoutCA()
			return nil
		},
	}

	c.setAllFlags(cmd)
	c.root.AddCommand(cmd)
	return nil
}

type buildBeeNodeResp struct {
	bee *node.Bee
	err error
}

func buildBeeNodeAsync(ctx context.Context, c *command, cmd *cobra.Command, logger log.Logger) <-chan buildBeeNodeResp {
	respC := make(chan buildBeeNodeResp, 1)

	go func() {
		bee, err := buildBeeNode(ctx, c, cmd, logger)
		respC <- buildBeeNodeResp{bee, err}
	}()

	return respC
}

// simdSelectedBy distinguishes an operator's explicit choice from the default,
// so a log line is enough to tell which happened.
func simdSelectedBy(explicit bool) string {
	if explicit {
		return "configuration"
	}
	return "cpu-detection"
}

func buildBeeNode(ctx context.Context, c *command, cmd *cobra.Command, logger log.Logger) (*node.Bee, error) {
	var err error

	// If the resolver is specified, resolve all connection strings
	// and fail on any errors.
	var resolverCfgs []multiresolver.ConnectionConfig
	resolverEndpoints := c.config.GetStringSlice(optionNameResolverEndpoints)
	if len(resolverEndpoints) > 0 {
		resolverCfgs, err = multiresolver.ParseConnectionStrings(resolverEndpoints)
		if err != nil {
			return nil, fmt.Errorf("parse resolver endpoints: %w", err)
		}
	}

	signerConfig, err := c.configureSigner(cmd, logger)
	if err != nil {
		return nil, fmt.Errorf("configure signer: %w", err)
	}

	bootNode := c.config.GetBool(optionNameBootnodeMode)
	fullNode := c.config.GetBool(optionNameFullNode)

	if bootNode && !fullNode {
		return nil, errors.New("boot node must be started as a full node")
	}

	mainnet := c.config.GetBool(optionNameMainNet)
	userHasSetNetworkID := c.config.IsSet(optionNameNetworkID)

	// if the user has provided a value - we use it and overwrite the default
	// if mainnet is true then we only accept networkID value 1, error otherwise
	// if the user has not provided a network ID but mainnet is true - just overwrite with mainnet network ID (1)
	// in all the other cases we default to test network ID (10)
	networkID := chaincfg.Testnet.NetworkID

	if userHasSetNetworkID {
		networkID = c.config.GetUint64(optionNameNetworkID)
		if mainnet && networkID != chaincfg.Mainnet.NetworkID {
			return nil, errors.New("provided network ID does not match mainnet")
		}
	} else if mainnet {
		networkID = chaincfg.Mainnet.NetworkID
	}

	bootnodes := c.config.GetStringSlice(optionNameBootnodes)
	blockTime := c.config.GetUint64(optionNameBlockTime)

	networkConfig := getConfigByNetworkID(networkID, blockTime)

	if c.config.IsSet(optionNameBootnodes) {
		networkConfig.bootNodes = bootnodes
	}

	if c.config.IsSet(optionNameBlockTime) && blockTime != 0 {
		networkConfig.blockTime = time.Duration(blockTime) * time.Second
	}

	staticNodesOpt := c.config.GetStringSlice(optionNameStaticNodes)
	staticNodes := make([]swarm.Address, 0, len(staticNodesOpt))
	for _, p := range staticNodesOpt {
		addr, err := swarm.ParseHexAddress(p)
		if err != nil {
			return nil, fmt.Errorf("invalid swarm address %q configured for static node", p)
		}

		staticNodes = append(staticNodes, addr)
	}
	if len(staticNodes) > 0 && !bootNode {
		return nil, errors.New("static nodes can only be configured on bootnodes")
	}

	var neighborhoodSuggester string
	if networkID == chaincfg.Mainnet.NetworkID {
		neighborhoodSuggester = c.config.GetString(optionNameNeighborhoodSuggester)
	}

	// SIMD hashing is on by default wherever the CPU can run it, and off
	// everywhere else. It measured 25% off reserve-sample wall clock on bench-1
	// (92.0s to 69.0s over 243 runs), which is worth having without an operator
	// needing to know the flag exists. See issue #54.
	//
	// It cannot simply default to true: bee refuses to start when the flag is
	// set on hardware without AVX2, so a blanket default would stop every ARM
	// and older x86 node from booting.
	//
	// An explicit setting is still honoured strictly, including that refusal.
	// Someone who asked for SIMD and cannot have it should be told, not quietly
	// given the slow path — the whole point of asking was to be sure.
	simdSupported := runtime.GOOS == "linux" && runtime.GOARCH == "amd64" && keccak.HasSIMD()
	// IsSet is false for an option nobody mentioned and true for one set on the
	// command line, in the config file, or through BEE_USE_SIMD_HASHING —
	// including when it is set to the same value as the flag default. It does
	// not count the default itself, because viper looks it up with flagDefault
	// disabled. TestSimdOptionDetection pins all four cases, since the whole
	// default rests on that distinction.
	simdChosen := c.config.IsSet(optionUseSIMD)
	useSIMD := simdSupported
	if simdChosen {
		useSIMD = c.config.GetBool(optionUseSIMD)
	}
	if useSIMD {
		if simdChosen && (runtime.GOOS != "linux" || runtime.GOARCH != "amd64") {
			return nil, fmt.Errorf("SIMD hashing requires linux/amd64 (this build is %s/%s)", runtime.GOOS, runtime.GOARCH)
		}
		if simdChosen && !keccak.HasSIMD() {
			return nil, errors.New("SIMD hashing requires a CPU with AVX2 or AVX-512; this CPU has neither")
		}
		bmt.SetSIMDOptIn(true)
		// Rebuild the global bmtpool instance so the new SIMDOptIn value
		// is reflected in the pool created for hot-path BMT hashing.
		bmtpool.Rebuild()
		// SIMD hashing runs foreign machine code (the XKCP blob) on a scratch
		// stack, not on the goroutine stack — see pkg/keccak and issue #92.
		// Running it on a goroutine stack corrupted memory and killed the node
		// within ~12 minutes under load; with the scratch stack it soaked 7.6
		// hours clean at 113 peers.
		//
		// Still Info-with-detail rather than silent: this is an opt-in path
		// with hand-written assembly and a pre-compiled binary blob in it, and
		// an operator debugging an odd crash should be able to see from the
		// logs whether it was active.
		logger.Info("SIMD hashing enabled",
			"batch_width", keccak.BatchWidth(), "avx512", keccak.HasAVX512(),
			"selected_by", simdSelectedBy(simdChosen))
	} else if !simdChosen && !simdSupported {
		// Say so once at startup. An operator comparing two machines and
		// wondering why one samples faster should not have to guess.
		logger.Debug("SIMD hashing not available on this platform",
			"os", runtime.GOOS, "arch", runtime.GOARCH, "avx2_or_avx512", keccak.HasSIMD())
	}

	var bzzTokenAddress common.Address
	if a := c.config.GetString(optionNameBzzTokenAddress); a != "" {
		if !common.IsHexAddress(a) {
			return nil, errors.New("malformed bzz token address")
		}
		bzzTokenAddress = common.HexToAddress(a)
	}

	b, err := node.NewBee(ctx, c.config.GetString(optionNameP2PAddr), signerConfig.publicKey, signerConfig.signer, networkID, logger, signerConfig.libp2pPrivateKey, signerConfig.pssPrivateKey, signerConfig.session, &node.Options{
		Addr:                            c.config.GetString(optionNameP2PAddr),
		AllowPrivateCIDRs:               c.config.GetBool(optionNameAllowPrivateCIDRs),
		APIAddr:                         c.config.GetString(optionNameAPIAddr),
		EnableWSS:                       c.config.GetBool(optionNameP2PWSSEnable),
		WSSAddr:                         c.config.GetString(optionP2PWSSAddr),
		AutoTLSStorageDir:               filepath.Join(c.config.GetString(optionNameDataDir), "autotls"),
		BlockchainRpcEndpoints:          rpcEndpoints(c.config),
		BlockchainRpcDialTimeout:        c.config.GetDuration(configKeyBlockchainRpcDialTimeout),
		BlockchainRpcTLSTimeout:         c.config.GetDuration(configKeyBlockchainRpcTLSTimeout),
		BlockchainRpcIdleTimeout:        c.config.GetDuration(configKeyBlockchainRpcIdleTimeout),
		BlockchainRpcKeepalive:          c.config.GetDuration(configKeyBlockchainRpcKeepalive),
		BzzTokenAddress:                 bzzTokenAddress,
		BlockProfile:                    c.config.GetBool(optionNamePProfBlock),
		BlockTime:                       networkConfig.blockTime,
		BlockTimeSet:                    c.config.IsSet(optionNameBlockTime) && blockTime != 0,
		PostageConfirmationDepth:        c.config.GetUint64(optionNamePostageConfirmationDepth),
		BlockSyncInterval:               c.config.GetUint64(optionNameBlockSyncInterval),
		BootnodeMode:                    bootNode,
		Bootnodes:                       networkConfig.bootNodes,
		CacheCapacity:                   c.config.GetUint64(optionNameCacheCapacity),
		AutoTLSCAEndpoint:               c.config.GetString(optionAutoTLSCAEndpoint),
		ChainID:                         networkConfig.chainID,
		ChequebookEnable:                c.config.GetBool(optionNameChequebookEnable),
		ChequebookVerification:          c.config.GetBool(optionNameChequebookVerification),
		ChequebookMinBalance:            c.config.GetString(optionNameChequebookMinBalance),
		CORSAllowedOrigins:              c.config.GetStringSlice(optionCORSAllowedOrigins),
		DataDir:                         c.config.GetString(optionNameDataDir),
		DBBlockCacheCapacity:            c.config.GetUint64(optionNameDBBlockCacheCapacity),
		DBDisableSeeksCompaction:        c.config.GetBool(optionNameDBDisableSeeksCompaction),
		StorageEngine:                   c.config.GetString(optionNameStorageEngine),
		ReserveProofMode:                c.config.GetString(optionNameReserveProofMode),
		ReserveCapacity:                 c.config.GetUint64(optionNameReserveCapacity),
		DBOpenFilesLimit:                c.config.GetUint64(optionNameDBOpenFilesLimit),
		DBWriteBufferSize:               c.config.GetUint64(optionNameDBWriteBufferSize),
		DBCompactionL0Trigger:           c.config.GetInt(optionNameDBCompactionL0Trigger),
		DBWriteSlowdownTrigger:          c.config.GetInt(optionNameDBWriteSlowdownTrigger),
		DBWritePauseTrigger:             c.config.GetInt(optionNameDBWritePauseTrigger),
		EnableStorageIncentives:         c.config.GetBool(optionNameStorageIncentivesEnable),
		EnableWS:                        c.config.GetBool(optionNameP2PWSEnable),
		AutoTLSDomain:                   c.config.GetString(optionAutoTLSDomain),
		AutoTLSRegistrationEndpoint:     c.config.GetString(optionAutoTLSRegistrationEndpoint),
		FullNodeMode:                    fullNode,
		LightNodeLimit:                  c.config.GetInt(optionNameLightNodeLimit),
		UltraLightNodeLimit:             c.config.GetInt(optionNameUltraLightNodeLimit),
		P2PMaxConnectionsPerIP:          c.config.GetInt(optionNameP2PMaxConnectionsPerIP),
		P2PConnectionRatePerIP:          c.config.GetFloat64(optionNameP2PConnectionRatePerIP),
		P2PConnectionBurstPerIP:         c.config.GetInt(optionNameP2PConnectionBurstPerIP),
		Logger:                          logger,
		MinimumGasTipCap:                c.config.GetUint64(optionNameMinimumGasTipCap),
		GasLimitFallback:                c.config.GetUint64(optionNameGasLimitFallback),
		MinimumStorageRadius:            c.config.GetUint(optionMinimumStorageRadius),
		MutexProfile:                    c.config.GetBool(optionNamePProfMutex),
		NATAddr:                         c.config.GetString(optionNameNATAddr),
		NATWSSAddr:                      c.config.GetString(optionNATWSSAddr),
		NeighborhoodSuggester:           neighborhoodSuggester,
		PaymentEarly:                    c.config.GetInt64(optionNamePaymentEarly),
		PaymentThreshold:                c.config.GetString(optionNamePaymentThreshold),
		PaymentTolerance:                c.config.GetInt64(optionNamePaymentTolerance),
		RefreshAllowanceAccrual:         c.config.GetString(optionNameRefreshAllowanceAccrual),
		PostageContractAddress:          c.config.GetString(optionNamePostageContractAddress),
		PostageContractStartBlock:       c.config.GetUint64(optionNamePostageContractStartBlock),
		PriceOracleAddress:              c.config.GetString(optionNamePriceOracleAddress),
		RedistributionContractAddress:   c.config.GetString(optionNameRedistributionAddress),
		ReserveCapacityDoubling:         c.config.GetInt(optionReserveCapacityDoubling),
		MaxReserveCapacityDoubling:      c.config.GetInt(optionMaxReserveCapacityDoubling),
		RedistributionSyncRateThreshold: c.config.GetInt(optionRedistributionSyncRateThreshold),
		ResolverConnectionCfgs:          resolverCfgs,
		Resync:                          c.config.GetBool(optionNameResync),
		RetrievalCaching:                c.config.GetBool(optionNameRetrievalCaching),
		LocalIngestEnable:               c.config.GetBool(optionNameLocalIngestEnable),
		LocalIngestLimit:                c.config.GetUint64(optionNameLocalIngestLimit),
		ProvidersEnable:                 c.config.GetBool(optionNameProvidersEnable),
		ProvidersPaymentThreshold:       c.config.GetString(optionNameProvidersPaymentThreshold),
		ProvidersCreditBudget:           c.config.GetString(optionNameProvidersCreditBudget),
		SkipPostageSnapshot:             c.config.GetBool(optionSkipPostageSnapshot),
		StakingContractAddress:          c.config.GetString(optionNameStakingAddress),
		StakeRecoveryOnStartup:          c.config.GetString(optionNameStakeRecoveryOnStartup),
		StatestoreCacheCapacity:         c.config.GetUint64(optionNameStateStoreCacheCapacity),
		StaticNodes:                     staticNodes,
		SwapEnable:                      c.config.GetBool(optionNameSwapEnable),
		SwapFactoryAddress:              c.config.GetString(optionNameSwapFactoryAddress),
		SwapInitialDeposit:              c.config.GetString(optionNameSwapInitialDeposit),
		TargetNeighborhood:              c.config.GetString(optionNameTargetNeighborhood),
		TracingEnabled:                  c.config.GetBool(configKeyTracingEnabled),
		TracingEndpoint:                 c.config.GetString(configKeyTracingEndpoint),
		TracingInsecure:                 c.config.GetBool(configKeyTracingInsecure),
		TracingCAFile:                   c.config.GetString(configKeyTracingCAFile),
		TracingProtocol:                 c.config.GetString(configKeyTracingProtocol),
		TracingSamplingRatio:            c.config.GetFloat64(configKeyTracingSamplingRatio),
		TracingServiceName:              c.config.GetString(configKeyTracingServiceName),
		TrxDebugMode:                    c.config.GetBool(optionNameTransactionDebugMode),
		WarmupTime:                      c.config.GetDuration(optionWarmUpTime),
		WelcomeMessage:                  c.config.GetString(optionWelcomeMessage),
		SamplerReadConcurrency:          c.config.GetInt(optionNameSamplerReadConcurrency),
		SamplerSortWindow:               c.config.GetInt(optionNameSamplerSortWindow),
		ReserveHasConcurrency:           c.config.GetInt(optionNameReserveHasConcurrency),
		KademliaSaturationPeers:         c.config.GetInt(optionNameKademliaSaturationPeers),
		KademliaOverSaturationPeers:     c.config.GetInt(optionNameKademliaOverSaturationPeers),
		PullSyncMaxChunksPerSecond:      c.config.GetInt(optionNamePullSyncMaxChunksPerSecond),
		PullerMaxChunksPerSecond:        c.config.GetInt(optionNamePullerMaxChunksPerSecond),
		PullerRecalcPeersDur:            c.config.GetDuration(optionNamePullSyncRecalcInterval),
		ReserveWakeUpDuration:           c.config.GetDuration(optionNameReserveWakeUpDuration),
		ReserveBatchSweepInterval:       c.config.GetDuration(optionNameReserveBatchSweepInterval),
		WhitelistedWithdrawalAddress:    c.config.GetStringSlice(optionNameWhitelistedWithdrawalAddress),
	})

	return b, err
}

type program struct {
	start func()
	stop  func()
}

func (p *program) Start(s service.Service) error {
	// Start should not block. Do the actual work async.
	go p.start()
	return nil
}

func (p *program) Stop(s service.Service) error {
	p.stop()
	return nil
}

type signerConfig struct {
	signer           crypto.Signer
	publicKey        *ecdsa.PublicKey
	libp2pPrivateKey *ecdsa.PrivateKey
	pssPrivateKey    *ecdsa.PrivateKey
	session          accesscontrol.Session
}

func (c *command) configureSigner(cmd *cobra.Command, logger log.Logger) (config *signerConfig, err error) {
	var keystore keystore.Service
	if c.config.GetString(optionNameDataDir) == "" {
		keystore = memkeystore.New()
		logger.Warning("data directory not provided, keys are not persisted")
	} else {
		keystore = filekeystore.New(filepath.Join(c.config.GetString(optionNameDataDir), "keys"))
	}

	var signer crypto.Signer
	var password string
	var publicKey *ecdsa.PublicKey
	var session accesscontrol.Session
	if p := c.config.GetString(optionNamePassword); p != "" {
		password = p
	} else if pf := c.config.GetString(optionNamePasswordFile); pf != "" {
		b, err := os.ReadFile(pf)
		if err != nil {
			return nil, err
		}
		password = string(bytes.Trim(b, "\n"))
	} else {
		// if libp2p key exists we can assume all required keys exist
		// so prompt for a password to unlock them
		// otherwise prompt for new password with confirmation to create them
		exists, err := keystore.Exists(libp2pPKFilename)
		if err != nil {
			return nil, err
		}
		if exists {
			password, err = terminalPromptPassword(cmd, c.passwordReader, "Password")
			if err != nil {
				return nil, err
			}
		} else {
			password, err = terminalPromptCreatePassword(cmd, c.passwordReader)
			if err != nil {
				return nil, err
			}
		}
	}

	swarmPrivateKey, _, err := keystore.Key("swarm", password, crypto.EDGSecp256_K1)
	if err != nil {
		return nil, fmt.Errorf("swarm key: %w", err)
	}
	signer = crypto.NewDefaultSigner(swarmPrivateKey)
	publicKey = &swarmPrivateKey.PublicKey
	session = accesscontrol.NewDefaultSession(swarmPrivateKey)

	logger.Info("swarm public key", "public_key", hex.EncodeToString(crypto.EncodeSecp256k1PublicKey(publicKey)))

	libp2pPrivateKey, created, err := keystore.Key(libp2pPKFilename, password, crypto.EDGSecp256_R1)
	if err != nil {
		return nil, fmt.Errorf("libp2p v2 key: %w", err)
	}
	if created {
		logger.Debug("new libp2p v2 key created")
	} else {
		logger.Debug("using existing libp2p key")
	}

	pssPrivateKey, created, err := keystore.Key("pss", password, crypto.EDGSecp256_K1)
	if err != nil {
		return nil, fmt.Errorf("pss key: %w", err)
	}
	if created {
		logger.Debug("new pss key created")
	} else {
		logger.Debug("using existing pss key")
	}

	logger.Info("pss public key", "public_key", hex.EncodeToString(crypto.EncodeSecp256k1PublicKey(&pssPrivateKey.PublicKey)))

	// postinst and post scripts inside packaging/{deb,rpm} depend and parse on this log output
	overlayEthAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}
	logger.Info("using ethereum address", "address", overlayEthAddress)

	return &signerConfig{
		signer:           signer,
		publicKey:        publicKey,
		libp2pPrivateKey: libp2pPrivateKey,
		pssPrivateKey:    pssPrivateKey,
		session:          session,
	}, nil
}

type networkConfig struct {
	bootNodes []string
	blockTime time.Duration
	chainID   int64
}

func getConfigByNetworkID(networkID uint64, defaultBlockTimeInSeconds uint64) *networkConfig {
	config := networkConfig{
		blockTime: time.Duration(defaultBlockTimeInSeconds) * time.Second,
	}
	switch networkID {
	case chaincfg.Mainnet.NetworkID:
		config.bootNodes = []string{"/dnsaddr/mainnet.ethswarm.org"}
		config.blockTime = 5 * time.Second
		config.chainID = chaincfg.Mainnet.ChainID
	case 5: // Staging.
		config.chainID = chaincfg.Testnet.ChainID
	case chaincfg.Testnet.NetworkID:
		config.bootNodes = []string{"/dnsaddr/testnet.ethswarm.org"}
		config.blockTime = 12 * time.Second
		config.chainID = chaincfg.Testnet.ChainID
	default: // Will use the value provided by the chain.
		config.chainID = -1
	}

	return &config
}
