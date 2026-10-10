// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	chaincfg "github.com/ethersphere/bee/v2/pkg/config"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/node"
	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/postage/listener"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	p2pforge "github.com/ipshipyard/p2p-forge/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	optionNameDataDir                      = "data-dir"
	optionNameCacheCapacity                = "cache-capacity"
	optionNameStorageEngine                = "storage-engine"
	optionNameReserveProofMode             = "reserve-proof-mode"
	optionNameReserveCapacity              = "reserve-capacity"
	optionNameDBOpenFilesLimit             = "db-open-files-limit"
	optionNameDBBlockCacheCapacity         = "db-block-cache-capacity"
	optionNameDBWriteBufferSize            = "db-write-buffer-size"
	optionNameDBDisableSeeksCompaction     = "db-disable-seeks-compaction"
	optionNameDBCompactionL0Trigger        = "db-compaction-l0-trigger"
	optionNameDBWriteSlowdownTrigger       = "db-write-slowdown-trigger"
	optionNameDBWritePauseTrigger          = "db-write-pause-trigger"
	optionNamePassword                     = "password"
	optionNamePasswordFile                 = "password-file"
	optionNameAPIAddr                      = "api-addr"
	optionNameP2PAddr                      = "p2p-addr"
	optionNameNATAddr                      = "nat-addr"
	optionNameP2PWSEnable                  = "p2p-ws-enable"
	optionNameBootnodes                    = "bootnode"
	optionNameNetworkID                    = "network-id"
	optionWelcomeMessage                   = "welcome-message"
	optionNameSamplerReadConcurrency       = "sampler-read-concurrency"
	optionNameSamplerSortWindow            = "sampler-sort-window"
	optionNameReserveHasConcurrency        = "reserve-has-concurrency"
	optionNameReserveEvictionRate          = "reserve-eviction-rate"
	optionNameKademliaSaturationPeers      = "kademlia-saturation-peers"
	optionNameKademliaOverSaturationPeers  = "kademlia-over-saturation-peers"
	optionNameLogSinkBuffer                = "log-sink-buffer"
	optionNamePullSyncMaxChunksPerSecond   = "pullsync-max-chunks-per-second"
	optionNamePullerMaxChunksPerSecond     = "puller-max-chunks-per-second"
	optionNamePullSyncRecalcInterval       = "pullsync-recalc-interval"
	optionNameReserveWakeUpDuration        = "reserve-wakeup-duration"
	optionNameReserveBatchSweepInterval    = "reserve-batch-sweep-interval"
	optionCORSAllowedOrigins               = "cors-allowed-origins"
	optionNameTracingEnabled               = "tracing-enable"
	optionNameTracingEndpoint              = "tracing-endpoint"
	optionNameTracingInsecure              = "tracing-insecure"
	optionNameTracingCAFile                = "tracing-ca-file"
	optionNameTracingProtocol              = "tracing-protocol"
	optionNameTracingSamplingRatio         = "tracing-sampling-ratio"
	optionNameTracingServiceName           = "tracing-service-name"
	optionNameVerbosity                    = "verbosity"
	optionNamePaymentThreshold             = "payment-threshold"
	optionNamePaymentTolerance             = "payment-tolerance-percent"
	optionNameRefreshAllowanceAccrual      = "refresh-allowance-accrual"
	optionNamePaymentEarly                 = "payment-early-percent"
	optionNameResolverEndpoints            = "resolver-options"
	optionNameBootnodeMode                 = "bootnode-mode"
	optionNameBzzTokenAddress              = "bzz-token-address"
	optionNameSwapFactoryAddress           = "swap-factory-address"
	optionNameSwapInitialDeposit           = "swap-initial-deposit"
	optionNameSwapEnable                   = "swap-enable"
	optionNameChequebookEnable             = "chequebook-enable"
	optionNameChequebookVerification       = "chequebook-verification"
	optionNameChequebookMinBalance         = "chequebook-min-balance"
	optionNameFullNode                     = "full-node"
	optionNameLightNodeLimit               = "light-node-limit"
	optionNameUltraLightNodeLimit          = "ultra-light-node-limit"
	optionNameP2PMaxConnectionsPerIP       = "p2p-max-connections-per-ip"
	optionNameP2PConnectionRatePerIP       = "p2p-connection-rate-per-ip"
	optionNameP2PConnectionBurstPerIP      = "p2p-connection-burst-per-ip"
	optionNameP2PInboundConnectionRate     = "p2p-inbound-connection-rate"
	optionNameP2PInboundConnectionBurst    = "p2p-inbound-connection-burst"
	optionNameP2PInboundStreamLimits       = "p2p-inbound-stream-limits"
	optionNameP2PInboundStreamsPerPeer     = "p2p-inbound-streams-per-peer"
	optionNameP2PInboundStreamReservePull  = "p2p-inbound-stream-reserve-pullsync"
	optionNameP2PInboundStreamReserveOther = "p2p-inbound-stream-reserve-other"
	optionNameP2PInboundStreamsTransient   = "p2p-inbound-streams-transient"
	optionNameP2PInboundStreamsUnnegPeer   = "p2p-inbound-streams-unnegotiated-per-peer"
	optionNamePostageContractAddress       = "postage-stamp-address"
	optionNamePostageContractStartBlock    = "postage-stamp-start-block"
	optionNamePriceOracleAddress           = "price-oracle-address"
	optionNameRedistributionAddress        = "redistribution-address"
	optionNameStakingAddress               = "staking-address"
	optionNameStakeRecoveryOnStartup       = "stake-recovery-on-startup"
	optionNameBlockTime                    = "block-time"
	optionNameBlockSyncInterval            = "block-sync-interval"
	optionNamePostageConfirmationDepth     = "postage-confirmation-depth"
	optionNamePostageStallShutdown         = "postage-stall-shutdown"
	optionWarmUpTime                       = "warmup-time"
	optionNameMainNet                      = "mainnet"
	optionNameRetrievalCaching             = "cache-retrieval"
	optionNameLocalIngestEnable            = "local-ingest-enable"
	optionNameLocalIngestLimit             = "local-ingest-limit"
	optionNameProvidersEnable              = "providers-enable"
	optionNameProvidersPaymentThreshold    = "providers-payment-threshold"
	optionNameProvidersCreditBudget        = "providers-credit-budget"
	optionNameResync                       = "resync"
	optionNamePProfBlock                   = "pprof-profile"
	optionNamePProfMutex                   = "pprof-mutex"
	optionNameStaticNodes                  = "static-nodes"
	optionNameAllowPrivateCIDRs            = "allow-private-cidrs"
	optionNameSleepAfter                   = "sleep-after"
	optionNameStorageIncentivesEnable      = "storage-incentives-enable"
	optionNameStopWaitForReveal            = "stop-wait-for-reveal"
	optionNameStateStoreCacheCapacity      = "statestore-cache-capacity"
	optionNameTargetNeighborhood           = "target-neighborhood"
	optionNameNeighborhoodSuggester        = "neighborhood-suggester"
	optionNameWhitelistedWithdrawalAddress = "withdrawal-addresses-whitelist"
	optionNameTransactionDebugMode         = "transaction-debug-mode"
	optionMinimumStorageRadius             = "minimum-storage-radius"
	optionReserveCapacityDoubling          = "reserve-capacity-doubling"
	optionMaxReserveCapacityDoubling       = "max-reserve-capacity-doubling"
	optionRedistributionSyncRateThreshold  = "redistribution-sync-rate-threshold"
	optionSkipPostageSnapshot              = "skip-postage-snapshot"
	optionNameMinimumGasTipCap             = "minimum-gas-tip-cap"
	optionNameGasLimitFallback             = "gas-limit-fallback"
	optionNameP2PWSSEnable                 = "p2p-wss-enable"
	optionP2PWSSAddr                       = "p2p-wss-addr"
	optionNATWSSAddr                       = "nat-wss-addr"
	optionAutoTLSDomain                    = "autotls-domain"
	optionAutoTLSRegistrationEndpoint      = "autotls-registration-endpoint"
	optionAutoTLSCAEndpoint                = "autotls-ca-endpoint"
	optionUseSIMD                          = "use-simd-hashing"

	// blockchain-rpc
	optionNameBlockchainRpcEndpoint    = "blockchain-rpc-endpoint"
	optionNameBlockchainRpcDialTimeout = "blockchain-rpc-dial-timeout"
	optionNameBlockchainRpcTLSTimeout  = "blockchain-rpc-tls-timeout"
	optionNameBlockchainRpcIdleTimeout = "blockchain-rpc-idle-timeout"
	optionNameBlockchainRpcKeepalive   = "blockchain-rpc-keepalive"
	configKeyBlockchainRpcEndpoint     = "blockchain-rpc.endpoint"
	configKeyBlockchainRpcDialTimeout  = "blockchain-rpc.dial-timeout"
	configKeyBlockchainRpcTLSTimeout   = "blockchain-rpc.tls-timeout"
	configKeyBlockchainRpcIdleTimeout  = "blockchain-rpc.idle-timeout"
	configKeyBlockchainRpcKeepalive    = "blockchain-rpc.keepalive"

	// tracing
	configKeyTracingEnabled       = "tracing.enable"
	configKeyTracingEndpoint      = "tracing.endpoint"
	configKeyTracingInsecure      = "tracing.insecure"
	configKeyTracingCAFile        = "tracing.ca-file"
	configKeyTracingProtocol      = "tracing.protocol"
	configKeyTracingSamplingRatio = "tracing.sampling-ratio"
	configKeyTracingServiceName   = "tracing.service-name"
)

var blockchainRpcConfigPairs = []struct{ flat, dotted string }{
	{optionNameBlockchainRpcEndpoint, configKeyBlockchainRpcEndpoint},
	{optionNameBlockchainRpcDialTimeout, configKeyBlockchainRpcDialTimeout},
	{optionNameBlockchainRpcTLSTimeout, configKeyBlockchainRpcTLSTimeout},
	{optionNameBlockchainRpcIdleTimeout, configKeyBlockchainRpcIdleTimeout},
	{optionNameBlockchainRpcKeepalive, configKeyBlockchainRpcKeepalive},
}

var tracingConfigPairs = []struct{ flat, dotted string }{
	{optionNameTracingEnabled, configKeyTracingEnabled},
	{optionNameTracingEndpoint, configKeyTracingEndpoint},
	{optionNameTracingInsecure, configKeyTracingInsecure},
	{optionNameTracingCAFile, configKeyTracingCAFile},
	{optionNameTracingProtocol, configKeyTracingProtocol},
	{optionNameTracingSamplingRatio, configKeyTracingSamplingRatio},
	{optionNameTracingServiceName, configKeyTracingServiceName},
}

// Deprecated tracing options, removed in the OpenTelemetry migration. They are
// kept only as hidden no-op flags so that existing configs do not break node
// startup; their values are ignored (see deprecatedTracingKeys).
//
// Note: tracing-endpoint is intentionally NOT here — it is reused as the active
// OTLP endpoint flag (see optionNameTracingEndpoint). Its meaning changed from a
// Jaeger agent address to an OTLP collector endpoint.
const (
	optionNameTracingHost = "tracing-host"
	optionNameTracingPort = "tracing-port"
)

// deprecatedTracingKeys are the pre-OpenTelemetry tracing options. They are
// registered as hidden no-op flags so stale configs still start the node, but
// their values are ignored; operators should migrate to the tracing-* keys.
var deprecatedTracingKeys = []string{
	optionNameTracingHost,
	optionNameTracingPort,
}

var knownNestedKeys = func() map[string]bool {
	m := make(map[string]bool, len(blockchainRpcConfigPairs)+len(tracingConfigPairs))
	for _, p := range blockchainRpcConfigPairs {
		m[p.dotted] = true
	}
	for _, p := range tracingConfigPairs {
		m[p.dotted] = true
	}
	return m
}()

// nolint:gochecknoinits
func init() {
	cobra.EnableCommandSorting = false
}

type command struct {
	root             *cobra.Command
	config           *viper.Viper
	logger           log.Logger
	passwordReader   passwordReader
	cfgFile          string
	homeDir          string
	isWindowsService bool
}

type option func(*command)

func newCommand(opts ...option) (c *command, err error) {
	c = &command{
		root: &cobra.Command{
			Use:           "bee",
			Short:         "Ethereum Swarm Bee",
			SilenceErrors: true,
			SilenceUsage:  true,
			PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
				return c.initConfig()
			},
		},
	}

	for _, o := range opts {
		o(c)
	}
	if c.passwordReader == nil {
		c.passwordReader = new(stdInPasswordReader)
	}

	// Find home directory.
	if err := c.setHomeDir(); err != nil {
		return nil, err
	}

	c.initGlobalFlags()

	if err := c.initCommandVariables(); err != nil {
		return nil, err
	}

	if err := c.initStartCmd(); err != nil {
		return nil, err
	}

	if err := c.initInitCmd(); err != nil {
		return nil, err
	}

	if err := c.initDeployCmd(); err != nil {
		return nil, err
	}

	c.initVersionCmd()
	c.initDBCmd()
	if err := c.initSplitCmd(); err != nil {
		return nil, err
	}

	if err := c.initConfigurateOptionsCmd(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *command) Execute() (err error) {
	return c.root.Execute()
}

// Execute parses command line arguments and runs appropriate functions.
func Execute() (err error) {
	c, err := newCommand()
	if err != nil {
		return err
	}
	return c.Execute()
}

func (c *command) initGlobalFlags() {
	globalFlags := c.root.PersistentFlags()
	globalFlags.StringVar(&c.cfgFile, "config", "", "config file (default is $HOME/.bee.yaml)")
}

func (c *command) initCommandVariables() error {
	isWindowsService, err := isWindowsService()
	if err != nil {
		return fmt.Errorf("failed to determine if we are running in service: %w", err)
	}

	c.isWindowsService = isWindowsService

	return nil
}

func (c *command) initConfig() (err error) {
	config := viper.New()
	configName := ".bee"
	if c.cfgFile != "" {
		// Use config file from the flag.
		config.SetConfigFile(c.cfgFile)
	} else {
		// Search config in home directory with name ".bee" (without extension).
		config.AddConfigPath(c.homeDir)
		config.SetConfigName(configName)
	}

	// Environment
	config.SetEnvPrefix("bee")
	config.AutomaticEnv() // read in environment variables that match
	config.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	if c.homeDir != "" && c.cfgFile == "" {
		c.cfgFile = filepath.Join(c.homeDir, configName+".yaml")
	}

	// If a config file is found, read it in.
	if err := config.ReadInConfig(); err != nil {
		var e viper.ConfigFileNotFoundError
		if !errors.As(err, &e) {
			return err
		}
	}
	c.config = config
	return nil
}

func (c *command) setHomeDir() (err error) {
	if c.homeDir != "" {
		return
	}
	dir, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	c.homeDir = dir
	return nil
}

// defaultReserveEvictionRate is the reserve-eviction-rate a node uses when
// the setting is unset: 500 chunks per second per node, chosen by the
// operator ahead of the measurement in #650 (spec:
// docs/experiments/eviction-rate-default/spec.md). An explicit 0 still
// means no limit.
const defaultReserveEvictionRate = 500

// reserveEvictionRate returns the configured reserve-eviction-rate: the flag
// default when the key is unset, the set value otherwise, including 0.
func reserveEvictionRate(config *viper.Viper) int {
	return config.GetInt(optionNameReserveEvictionRate)
}

// inboundStreamLimits reads the inbound stream limit settings, as start
// passes them to the node.
func inboundStreamLimits(config *viper.Viper) libp2p.StreamLimitOptions {
	return libp2p.StreamLimitOptions{
		Enabled:             config.GetBool(optionNameP2PInboundStreamLimits),
		PerPeer:             config.GetInt(optionNameP2PInboundStreamsPerPeer),
		ReservePullSync:     config.GetInt(optionNameP2PInboundStreamReservePull),
		ReserveOther:        config.GetInt(optionNameP2PInboundStreamReserveOther),
		Transient:           config.GetInt(optionNameP2PInboundStreamsTransient),
		UnnegotiatedPerPeer: config.GetInt(optionNameP2PInboundStreamsUnnegPeer),
	}
}

func (c *command) setAllFlags(cmd *cobra.Command) {
	cmd.Flags().String(optionNameDataDir, filepath.Join(c.homeDir, ".bee"), "data directory")
	cmd.Flags().Uint64(optionNameCacheCapacity, 1_000_000, fmt.Sprintf("cache capacity in chunks, multiply by %d to get approximate capacity in bytes", swarm.ChunkSize))
	cmd.Flags().String(optionNameStorageEngine, "", "index-store engine for a NEW data directory: \"pebble\" (default) or \"leveldb\". Empty uses the engine the data directory was created with, and pebble for a fresh one. An existing goleveldb store keeps goleveldb. The two on-disk formats are not interchangeable, so a directory is bound to its engine. See issue #185.")
	cmd.Flags().String(optionNameReserveProofMode, "classic", "redistribution reserve-size proof: \"classic\" (default, the whole-reserve scan the live contract accepts) or \"windowed\" (experimental sublinear proof; NOT accepted by the live contract, wins nothing, testnet and research only). See issue #273.")
	cmd.Flags().Uint64(optionNameReserveCapacity, 0, "reserve size in chunks; 0 uses the default of 4194304 (1<<22). Sets the base that reserve-capacity-doubling multiplies. For sizing the reserve to a disk or driving radius behaviour in testing. See issue #283.")
	cmd.Flags().Uint64(optionNameDBOpenFilesLimit, 200, "number of open files allowed by database")
	cmd.Flags().Uint64(optionNameDBBlockCacheCapacity, 32*1024*1024, "usable size of the database block cache in bytes; with the pebble engine the default 33554432 becomes 67108864, and the memtable reservation (3 x db-write-buffer-size) is added on top")
	cmd.Flags().Uint64(optionNameDBWriteBufferSize, 32*1024*1024, "size of the database write buffer in bytes")
	cmd.Flags().Bool(optionNameDBDisableSeeksCompaction, true, "disables db compactions triggered by seeks")
	cmd.Flags().Int(optionNameDBCompactionL0Trigger, 8, "level-0 SST files at which the index store starts compacting; lower compacts sooner and more often (more write amplification), higher lets level 0 grow (more read amplification on the sampling path); 0 means the goleveldb default of 4")
	cmd.Flags().Int(optionNameDBWriteSlowdownTrigger, 8, "level-0 SST files at which the index store slows writes by 1ms each to let compaction catch up; 0 means the goleveldb default of 8")
	cmd.Flags().Int(optionNameDBWritePauseTrigger, 12, "level-0 SST files at which the index store blocks writes until compaction catches up; raising it buys burst headroom before a stall but allows more level-0 files and so more read amplification, lowering it stalls sooner; 0 means the goleveldb default of 12")
	cmd.Flags().String(optionNamePassword, "", "password for decrypting keys")
	cmd.Flags().String(optionNamePasswordFile, "", "path to a file that contains password for decrypting keys")
	cmd.Flags().String(optionNameAPIAddr, "127.0.0.1:1633", "HTTP API listen address")
	cmd.Flags().String(optionNameP2PAddr, ":1634", "P2P listen address")
	cmd.Flags().String(optionNameNATAddr, "", "NAT exposed address, as host:port, or as :port to advertise the public IP peers observe on this port and follow it when it changes (only where inbound and outbound traffic share one public IP)")
	cmd.Flags().Bool(optionNameP2PWSEnable, false, "enable P2P WebSocket transport")
	cmd.Flags().StringSlice(optionNameBootnodes, []string{"/dnsaddr/mainnet.ethswarm.org"}, "initial nodes to connect to")
	cmd.Flags().Uint64(optionNameNetworkID, chaincfg.Mainnet.NetworkID, "ID of the Swarm network")
	cmd.Flags().StringSlice(optionCORSAllowedOrigins, []string{}, "origins with CORS headers enabled")
	cmd.Flags().Bool(optionNameTracingEnabled, false, "enable tracing")
	cmd.Flags().String(optionNameTracingEndpoint, "127.0.0.1:4318", "OTLP collector endpoint to send tracing data (host:port); default port is 4318 for http, 4317 for grpc")
	cmd.Flags().Bool(optionNameTracingInsecure, false, "disable TLS for the OTLP exporter (useful for a local collector); when false, set --tracing-ca-file to verify the collector certificate against a private CA")
	cmd.Flags().String(optionNameTracingCAFile, "", "path to a PEM-encoded CA bundle used to verify the OTLP collector certificate; ignored when --tracing-insecure=true")
	cmd.Flags().String(optionNameTracingProtocol, "http", "OTLP exporter transport: http or grpc")
	cmd.Flags().Float64(optionNameTracingSamplingRatio, 1.0, "head-based sampling ratio in [0,1]; 0 samples nothing, 1 samples everything")
	cmd.Flags().String(optionNameTracingServiceName, "bee", "service name identifier for tracing")
	// Deprecated, no-op tracing flags kept for backward compatibility so that
	// existing configs do not break node startup. Their values are ignored.
	for _, name := range deprecatedTracingKeys {
		cmd.Flags().String(name, "", "deprecated and ignored; use the tracing-* options")
		_ = cmd.Flags().MarkDeprecated(name, "no longer used after the OpenTelemetry migration; use the tracing-* options")
	}
	cmd.Flags().String(optionNameVerbosity, "info", "log verbosity level 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=trace")
	cmd.Flags().String(optionWelcomeMessage, "", "send a welcome message string during handshakes")
	cmd.Flags().Int(optionNamePullSyncMaxChunksPerSecond, 0, "rate this node serves to each peer pulling from it, in chunks per second; 0 uses the default of 250. Raising it lets peers pull from you faster at the cost of your own upload bandwidth; lowering it makes you a slower source for the network.")
	cmd.Flags().Duration(optionNamePullSyncRecalcInterval, 0, "how often the puller re-decides which peers to sync from; 0 uses the default of 5m")
	cmd.Flags().Duration(optionNameReserveWakeUpDuration, 0, "how long the reserve worker waits between runs; 0 uses the default of 15m")
	cmd.Flags().Duration(optionNameReserveBatchSweepInterval, 0, "how often the reserve reconciles chunks against their batches and evicts orphans; 0 runs it on every wake-up (the default). A longer interval keeps the frequent wake-up scan to the cheap within-radius count; it delays repairing the divergence case the sweep exists for, bounded by the interval")
	cmd.Flags().Int(optionNamePullerMaxChunksPerSecond, 0, "total inbound sync rate across all peers, in chunks per second; 0 uses the default of 1000, roughly 4 MB/s. Raising it fills a large reserve faster but draws more serving bandwidth from the peers you sync from, a cost paid by them; keep the default unless you run a large-reserve node that needs it.")
	cmd.Flags().Int(optionNameSamplerReadConcurrency, 0, "chunk loads the reserve sampler keeps in flight; 0 uses the default, which matches the CPU count and preserves previous behaviour")
	cmd.Flags().Int(optionNameSamplerSortWindow, 0, "chunks the reserve sampler buffers and sorts into disk order before reading; 0 reads in bin order, which is the previous behaviour")
	cmd.Flags().Int(optionNameReserveHasConcurrency, 0, "reserve lookups pullsync may have in flight at once; 0 leaves them unbounded, which is the previous behaviour")
	cmd.Flags().Int(optionNameReserveEvictionRate, defaultReserveEvictionRate, "most chunks per second reserve eviction deletes, raised to the rate chunks arrive at; 0 (set explicitly) means no limit, the behaviour before this default; an empty value counts as unset. Raising it finishes an eviction sooner with more CPU and disk load meanwhile, which on a host with many nodes can make them all slow to serve and miss lottery samples; lowering it keeps the node responsive but keeps evicted and expired chunks on disk longer, makes the node sit out more lottery rounds while it evicts, and makes peers fetch and reject chunks of expired batches for longer")
	cmd.Flags().Int(optionNameKademliaSaturationPeers, 0, "connected peers per bin below which the bin is not considered saturated; 0 uses the default of 8. Raising it consumes other nodes' connection budget, not only your own")
	cmd.Flags().Int(optionNameKademliaOverSaturationPeers, 0, "connected peers per bin above which the bin is over-saturated and further peers are pruned; 0 uses the default of 18. Raising it consumes other nodes' connection budget, not only your own")
	cmd.Flags().Int(optionNameLogSinkBuffer, log.DefaultSinkBuffer, "log lines that may wait to be written before further lines are dropped; 0 writes synchronously, which lets a stalled log reader block the node")
	cmd.Flags().String(optionNamePaymentThreshold, "13500000", "threshold in BZZ where you expect to get paid from your peers")
	cmd.Flags().Int64(optionNamePaymentTolerance, 25, "excess debt above payment threshold in percentages where you disconnect from your peer")
	cmd.Flags().String(optionNameRefreshAllowanceAccrual, "step", "how the refresh allowance raises a peer's overdraft limit inside the first second after a refreshment: step (default, grants nothing until a full second) or continuous (accrues per millisecond, capped below what the peer tolerates)")
	cmd.Flags().Int64(optionNamePaymentEarly, 50, "percentage below the peers payment threshold when we initiate settlement")
	cmd.Flags().StringSlice(optionNameResolverEndpoints, []string{}, "ENS compatible API endpoint for a TLD and with contract address, can be repeated, format [tld:][contract-addr@]url")
	cmd.Flags().Bool(optionNameBootnodeMode, false, "cause the node to always accept incoming connections")
	cmd.Flags().StringSlice(optionNameBlockchainRpcEndpoint, nil, "rpc blockchain endpoint; repeat or list several for failover, in priority order")
	cmd.Flags().Duration(optionNameBlockchainRpcDialTimeout, 30*time.Second, "blockchain rpc TCP dial timeout")
	cmd.Flags().Duration(optionNameBlockchainRpcTLSTimeout, 10*time.Second, "blockchain rpc TLS handshake timeout")
	cmd.Flags().Duration(optionNameBlockchainRpcIdleTimeout, 90*time.Second, "blockchain rpc idle connection timeout")
	cmd.Flags().Duration(optionNameBlockchainRpcKeepalive, 30*time.Second, "blockchain rpc TCP keepalive interval")
	cmd.Flags().String(optionNameSwapFactoryAddress, "", "swap factory addresses")
	cmd.Flags().String(optionNameBzzTokenAddress, "", "bzz token contract address")
	cmd.Flags().String(optionNameSwapInitialDeposit, "0", "initial deposit if deploying a new chequebook")
	cmd.Flags().Bool(optionNameSwapEnable, false, "enable swap")
	cmd.Flags().Bool(optionNameChequebookEnable, true, "enable chequebook")
	cmd.Flags().Bool(optionNameChequebookVerification, false, "reject full-node hive/handshake records that carry no chequebook address")
	cmd.Flags().String(optionNameChequebookMinBalance, "110000000000000000", "minimum chequebook token balance required for verification, in token small units (default 11 BZZ)")
	cmd.Flags().Bool(optionNameFullNode, false, "cause the node to start in full mode")
	cmd.Flags().Int(optionNameLightNodeLimit, 100, "light node limit")
	cmd.Flags().Int(optionNameP2PMaxConnectionsPerIP, 200, "open connections accepted from one IPv4 address or one IPv6 /56 subnet; 0 uses the default of 200. Raising it lets one address hold more of this node's connections, handshakes and light slots; lowering it can refuse users who share one address behind a carrier NAT, who must then use other nodes")
	cmd.Flags().Float64(optionNameP2PConnectionRatePerIP, 10, "new connections per second accepted from one IPv4 address or one IPv6 /56 subnet; 0 uses the default of 10. Raising it lets one address open connections faster and spend more of this node's handshakes; lowering it slows the reconnects of peers that share one address behind a carrier NAT, who then wait or try other nodes")
	cmd.Flags().Int(optionNameP2PConnectionBurstPerIP, 40, "new connections accepted at once from one IPv4 address or one IPv6 /56 subnet above the per-second rate; 0 uses the default of 40. Raising it lets one address open more connections at once; lowering it can refuse the reconnects of a restarting peer, or of users behind a shared carrier NAT, who then wait or try other nodes")
	cmd.Flags().Float64(optionNameP2PInboundConnectionRate, 30, "new inbound connections per second the node accepts in total, checked after the per-IP limits and before the security handshake, in each of two buckets: full peers that completed a handshake with this node, and every other address; 0 uses the default of 30, -1 turns the limit off, other negative values are refused; off on a bootnode unless set. Raising it lets more connection setups through under a flood, which costs this node CPU and takes it from its reserve samples; lowering it makes new light clients wait or go to other nodes, can make new full peers drop this node from their address books after 4 refused dials, and can make other nodes, and this node itself, judge it unreachable")
	cmd.Flags().Int(optionNameP2PInboundConnectionBurst, 200, "new inbound connections the node accepts at once above p2p-inbound-connection-rate, per bucket; 0 uses the default of 200, -1 turns the limit off, other negative values are refused. Raising it lets a larger flood through before the rate applies; lowering it can refuse the peers that reconnect at once after a restart, who then wait or go to other nodes")
	cmd.Flags().Bool(optionNameP2PInboundStreamLimits, false, "apply the inbound stream limits per peer and per protocol group set by the p2p-inbound-stream* settings; off by default until they are measured on real nodes. With it on, a peer or protocol group that reaches its limit has new inbound streams refused (stream reset 0x1002), which keeps one peer or protocol from taking the node-wide stream cap; a refused neighbour cannot sync from or retrieve through this node while it is at the limit, and an upstream neighbour may retry at once in a loop that costs both nodes CPU")
	cmd.Flags().Int(optionNameP2PInboundStreamsPerPeer, 0, "inbound streams one peer may hold at once across all protocols, when p2p-inbound-stream-limits is on; 0 uses the default of 256, -1 turns this limit off, other negative values are refused. Raising it lets one peer take more of this node's stream cap; lowering it can refuse a legitimate neighbour, which then cannot sync from or retrieve through this node, and an upstream neighbour may retry in a tight loop that costs both nodes CPU")
	cmd.Flags().Int(optionNameP2PInboundStreamReservePull, 0, "inbound streams kept free for pull-sync, which other protocols and un-negotiated streams cannot use, when p2p-inbound-stream-limits is on; 0 uses the default of 2000, -1 turns this reserve off, other negative values are refused. Raising it leaves less of the node-wide cap for retrieval, push-sync and new connections from other peers; lowering it lets those take more of the room neighbours need to sync from this node")
	cmd.Flags().Int(optionNameP2PInboundStreamReserveOther, 0, "inbound streams kept free for every protocol other than pull-sync, when p2p-inbound-stream-limits is on; 0 uses the default of 1000, -1 turns this reserve off, other negative values are refused. Raising it leaves less room for neighbours to sync from this node; lowering it lets pull-sync take room that retrieval, push-sync and new connections need")
	cmd.Flags().Int(optionNameP2PInboundStreamsTransient, 0, "inbound streams the node holds at once before their protocol is negotiated, when p2p-inbound-stream-limits is on; 0 uses the default of 1024, -1 turns this limit off (refused while a reserve is on), other negative values are refused. Raising it leaves less room in the cap for negotiated streams; lowering it can refuse streams of peers that open many at once, such as a neighbour reconnecting after a restart")
	cmd.Flags().Int(optionNameP2PInboundStreamsUnnegPeer, 0, "inbound streams one peer may hold at once before their protocol is negotiated, when p2p-inbound-stream-limits is on; 0 uses the default of 64, -1 turns this limit off, other negative values are refused. Raising it lets one peer hold more silent streams that count against the node-wide limit; lowering it can refuse a neighbour that opens its pull-sync streams at once on connect (about 52 at radius 6)")
	cmd.Flags().Int(optionNameUltraLightNodeLimit, 0, "light-node slots that ultra-light peers (light peers without a chequebook) may take; 0 means the same as light-node-limit, and a larger value is reduced to it. Raising it lets clients that cannot pay take more of the node's free bandwidth and handshakes; lowering it refuses such clients sooner and sends them to other nodes, while paying light peers keep their slots")
	cmd.Flags().String(optionNamePostageContractAddress, "", "postage stamp contract address")
	cmd.Flags().Uint64(optionNamePostageContractStartBlock, 0, "postage stamp contract start block number")
	cmd.Flags().String(optionNamePriceOracleAddress, "", "price oracle contract address")
	cmd.Flags().String(optionNameRedistributionAddress, "", "redistribution contract address")
	cmd.Flags().String(optionNameStakingAddress, "", "staking contract address")
	cmd.Flags().String(optionNameStakeRecoveryOnStartup, "off", "recover stake left in retired staking contracts when the node starts: \"off\" (default, do nothing), \"withdraw\" (recover it to the node's wallet), or \"migrate\" (recover it into the current staking contract). It moves staked funds, so it is off by default and runs only on explicit opt-in. If the node has no gas or the chain is unreachable it is skipped and retried on the next start, never blocking startup. In a YAML config file QUOTE the value: a bare off, on, yes or no is read as a boolean, so stake-recovery-on-startup: off arrives as \"false\". That is accepted as off, but quoting is what makes the file say what it means.")
	cmd.Flags().Uint64(optionNameBlockTime, 5, "chain block time in seconds; if set, used as is; if unset, the network default is used until the node has measured the chain's block time")
	cmd.Flags().Uint64(optionNameBlockSyncInterval, 10, "block number cache sync interval in blocks")
	cmd.Flags().Uint64(optionNamePostageConfirmationDepth, listener.DefaultConfirmationDepth, "blocks behind the chain head that postage events must be before they are applied, from 1 to 64; raising it delays seeing stamp events and pauses postage syncing once by the difference, lowering it risks applying events from blocks that are rolled back")
	cmd.Flags().Duration(optionNamePostageStallShutdown, 0, "stop the node once its batch store has been stale this long, counted from when it became stale (10 minutes after the last postage page applied); 0 never stops: a stale node stays up, serves content, holds chunks of batches it has not seen yet and sits out storage lottery rounds until it catches up; a duration stops it with exit status 75, which takes a node off the network and adds restart load to a host that may already be overloaded")
	cmd.Flags().Duration(optionWarmUpTime, time.Minute*5, "maximum node warmup duration; proceeds when stable or after this time")
	cmd.Flags().Bool(optionNameMainNet, true, "triggers connect to main net bootnodes.")
	cmd.Flags().Bool(optionNameRetrievalCaching, true, "enable forwarded content caching")
	cmd.Flags().Bool(optionNameLocalIngestEnable, false, "local ingest: accept content at POST /wasp/ingest and hold it in this node's own store without postage. Off by default. The route is mounted either way and answers 403 when this is off. Enabling it costs local storage that nothing reclaims and for which no rent is ever paid, gives content that only this node holds so losing the node loses the content with no replication anywhere, and opens a write endpoint to anyone who can reach the API, which includes a web page in the operator's own browser: a cross-origin POST to 127.0.0.1 reaches the handler and runs, and only the response is withheld from the page. There is no authentication layer to put it behind, so this flag is the only control. See docs/experiments/content-providers/local-ingest.md")
	cmd.Flags().Uint64(optionNameLocalIngestLimit, 65536, "local ingest: the most chunks this node will hold from local ingests. About limit*4104 bytes of store, as an upper bound because a chunk this node already held costs no new disk and is still counted. Raising it hands over more disk that nothing will reclaim; lowering it means ingests are refused with 507. 0 means no limit, which leaves nothing to stop an ingest filling the disk, and the node warns at startup when it is set with local-ingest-enable on")
	cmd.Flags().Bool(optionNameProvidersEnable, false, "content providers: announce pinned content through /wasp/providers, look up providers for larger downloads, honor the Wasp-Providers download header, and answer local-only requests from peers. Off by default. Announcing costs stamp slots and publishes which content this node holds; lookups cost extra retrievals, some of them unpaid work for the nodes that forward them. See docs/experiments/content-providers/spec.md")
	cmd.Flags().String(optionNameProvidersPaymentThreshold, "0", "content providers: the payment threshold in BZZ announced to a peer that is downloading content this node announced, instead of the node-wide one. 0 turns it off, which is the default. Raising it serves a larger burst of this node's bandwidth to each such peer before that peer has to pay or wait, on top of the refresh allowance every peer already gets, and a peer can take the burst again after reconnecting. Lowering it means the provider delivers a smaller share of each download. Needs providers-enable, must be ABOVE payment-threshold and at most 108000000, and needs providers-credit-budget set. It also costs the peer: an announced threshold sets that peer's early-payment point to a percentage of it, so the peer settles later and carries more unsettled debt than it otherwise would. See docs/experiments/content-providers/per-peer-threshold.md")
	cmd.Flags().String(optionNameProvidersCreditBudget, "0", "content providers: the sum of the extra credit granted across all connections currently holding a provider grant, in BZZ. Raising it lets more peers hold a grant at once, so more of this node's unsecured credit is outstanding simultaneously; lowering it means provider downloads beyond the budget are served at the ordinary threshold. Must be at least one grant's worth, that is providers-payment-threshold minus payment-threshold, or no peer could ever be admitted")
	cmd.Flags().Bool(optionNameResync, false, "forces the node to resync postage contract data")
	cmd.Flags().Bool(optionNamePProfBlock, false, "enable pprof block profile")
	cmd.Flags().Bool(optionNamePProfMutex, false, "enable pprof mutex profile")
	cmd.Flags().StringSlice(optionNameStaticNodes, []string{}, "protect nodes from getting kicked out on bootnode")
	cmd.Flags().Bool(optionNameAllowPrivateCIDRs, false, "allow to advertise private CIDRs to the public network")
	cmd.Flags().Bool(optionNameStorageIncentivesEnable, true, "enable storage incentives feature")
	cmd.Flags().Bool(optionNameStopWaitForReveal, true, "on a stop, wait for a pending storage-lottery reveal until it is mined or the round's reveal phase ends (up to about 200 s at 5-second blocks); a second signal ends the wait. Off, a stop right after a commit misses the reveal and the stake is frozen for twice as long as for a wrong reveal")
	cmd.Flags().Uint64(optionNameStateStoreCacheCapacity, 100_000, "lru memory caching capacity in number of statestore entries")
	cmd.Flags().String(optionNameTargetNeighborhood, "", "neighborhood to target in binary format (ex: 111111001) for mining the initial overlay")
	cmd.Flags().String(optionNameNeighborhoodSuggester, "https://api.swarmscan.io/v1/network/neighborhoods/suggestion", "suggester for target neighborhood")
	cmd.Flags().StringSlice(optionNameWhitelistedWithdrawalAddress, []string{}, "withdrawal target addresses")
	cmd.Flags().Bool(optionNameTransactionDebugMode, false, "skips the gas estimate step for contract transactions")
	cmd.Flags().Uint(optionMinimumStorageRadius, 0, "minimum radius storage threshold")
	cmd.Flags().Int(optionReserveCapacityDoubling, 0, "reserve capacity doubling")
	cmd.Flags().Int(optionMaxReserveCapacityDoubling, 1, "maximum allowed value for reserve-capacity-doubling. Raising it lets a node hold a larger reserve if it has the disk and the sync-rate headroom to fill and sample it in time; set too high the reserve cannot be sampled within a redistribution round and the node is frozen. Lowering it below 1 has no effect. See the reserve-capacity-doubling operator guide.")
	cmd.Flags().Int(optionRedistributionSyncRateThreshold, 0, "pull-sync rate in chunks per second below which the node counts itself synced enough to enter the redistribution draw. Default 0 keeps the strict behaviour of requiring a fully settled sync rate. A positive value is scaled by 2^reserve-capacity-doubling; raising it lets a doubled node participate while still syncing, at the risk of committing an incomplete sample and being frozen. See the reserve-capacity-doubling operator guide.")
	cmd.Flags().Bool(optionSkipPostageSnapshot, false, "skip postage snapshot")
	cmd.Flags().Uint64(optionNameMinimumGasTipCap, 0, "minimum gas tip cap in wei for transactions, 0 means use suggested gas tip cap")
	cmd.Flags().Uint64(optionNameGasLimitFallback, 500_000, "gas limit fallback when estimation fails for contract transactions")
	cmd.Flags().Bool(optionNameP2PWSSEnable, false, "Enable Secure WebSocket P2P connections")
	cmd.Flags().String(optionP2PWSSAddr, ":1635", "p2p wss address")
	cmd.Flags().String(optionNATWSSAddr, "", "WSS NAT exposed address")
	cmd.Flags().String(optionAutoTLSDomain, p2pforge.DefaultForgeDomain, "autotls domain")
	cmd.Flags().String(optionAutoTLSRegistrationEndpoint, p2pforge.DefaultForgeEndpoint, "autotls registration endpoint")
	cmd.Flags().String(optionAutoTLSCAEndpoint, p2pforge.DefaultCAEndpoint, "autotls certificate authority endpoint")
	cmd.Flags().Bool(optionUseSIMD, false, "use SIMD BMT hasher; unset means enable it where the CPU supports it (linux/amd64 with AVX2 or AVX-512). Setting it explicitly is strict: true on unsupported hardware refuses to start")
}

// preRun must be called from every command's PreRunE, after which c.logger is
// ready for use in RunE. It binds CLI flags to viper and initialises the logger.
func (c *command) preRun(cmd *cobra.Command) error {
	if err := c.config.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return c.initLogger(cmd)
}

func (c *command) initLogger(cmd *cobra.Command) error {
	v := strings.ToLower(c.config.GetString(optionNameVerbosity))
	logger, err := newLogger(cmd, v)
	if err != nil {
		return fmt.Errorf("new logger: %w", err)
	}
	if c.isWindowsService {
		logger, err = createWindowsEventLogger(serviceName, logger)
		if err != nil {
			return fmt.Errorf("new windows logger: %w", err)
		}
	}
	c.logger = logger
	return nil
}

// rpcEndpoints reads the blockchain RPC endpoints in priority order.
//
// The option became repeatable for #109, and a single value must keep working
// unchanged. viper's GetStringSlice handles a YAML list, a repeated flag and a
// comma-separated flag; for a plain scalar it falls back to splitting on
// whitespace, which leaves a URL intact because URLs contain none. Blanks are
// dropped so a stray comma or an empty list entry cannot become an endpoint
// named "".
func rpcEndpoints(config *viper.Viper) []string {
	raw := config.GetStringSlice(configKeyBlockchainRpcEndpoint)
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		if e = strings.TrimSpace(e); e != "" {
			out = append(out, e)
		}
	}
	return out
}

// bindBlockchainRpcConfig supports both flat (blockchain-rpc-endpoint) and
// nested (blockchain-rpc.endpoint) YAML forms, with nested taking precedence.
func (c *command) bindBlockchainRpcConfig(cmd *cobra.Command) {
	c.bindNestedConfig(cmd, blockchainRpcConfigPairs)
}

// bindTracingConfig supports both flat (tracing-endpoint) and nested
// (tracing.endpoint) YAML forms, with nested taking precedence.
func (c *command) bindTracingConfig(cmd *cobra.Command) {
	c.bindNestedConfig(cmd, tracingConfigPairs)
}

// warnDeprecatedTracingKeys logs a warning for each pre-OpenTelemetry tracing
// key found in the config file. The keys are accepted to keep stale configs
// bootable, but their values are ignored. Keys passed on the command line are
// already handled by cobra's deprecation notice.
func (c *command) warnDeprecatedTracingKeys() {
	for _, key := range deprecatedTracingKeys {
		if c.config.InConfig(key) {
			c.logger.Warning("deprecated tracing config key is ignored; use the tracing-* options", "key", key)
		}
	}
}

// warnTracingTLSWithoutCA warns when tracing runs over TLS without an explicit
// CA bundle, in which case the OTLP exporter falls back to the system root CAs.
func (c *command) warnTracingTLSWithoutCA() {
	if c.config.GetBool(configKeyTracingEnabled) &&
		!c.config.GetBool(configKeyTracingInsecure) &&
		c.config.GetString(configKeyTracingCAFile) == "" {
		c.logger.Warning("tracing: TLS is enabled but no CA bundle is configured; the OTLP exporter will rely on the system root CAs. Provide --tracing-ca-file, set --tracing-insecure=true for a plaintext local collector, or disable tracing.")
	}
}

func (c *command) bindNestedConfig(cmd *cobra.Command, pairs []struct{ flat, dotted string }) {
	for _, p := range pairs {
		nested := c.config.Get(p.dotted)
		conflict := c.config.InConfig(p.flat) && c.config.IsSet(p.dotted)
		_ = c.config.BindPFlag(p.dotted, cmd.Flags().Lookup(p.flat))
		c.config.RegisterAlias(p.flat, p.dotted)
		if conflict {
			c.logger.Warning("config key conflict: nested form takes precedence", "ignored", p.flat, "used", p.dotted)
			c.config.Set(p.dotted, nested)
		}
	}
}

func newLogger(cmd *cobra.Command, verbosity string) (log.Logger, error) {
	var (
		sink   = cmd.OutOrStdout()
		vLevel = log.VerbosityNone
	)

	switch verbosity {
	case "0", "silent":
		sink = io.Discard
	case "1", "error":
		vLevel = log.VerbosityError
	case "2", "warn":
		vLevel = log.VerbosityWarning
	case "3", "info":
		vLevel = log.VerbosityInfo
	case "4", "debug":
		vLevel = log.VerbosityDebug
	case "5", "trace":
		vLevel = log.VerbosityDebug + 1 // For backwards compatibility, just enable v1 debugging as trace.
	default:
		return nil, fmt.Errorf("unknown verbosity level %q", verbosity)
	}

	log.ModifyDefaults(
		log.WithTimestamp(),
		log.WithLogMetrics(),
	)

	opts := []log.Option{
		log.WithSink(sink),
		log.WithVerbosity(vLevel),
	}

	// Only pass WithSinkBuffer when the operator actually set the flag.
	//
	// WithSinkBuffer means "buffer this writer whatever it is", so passing it
	// unconditionally buffers a test's bytes.Buffer as readily as a pipe. That
	// raced against db_test.go reading the buffer it had installed with
	// SetOut. Left alone, the package default buffers only an *os.File, which
	// is what os.Stdout is in production and what a captured test buffer is
	// not. See issue #156.
	if cmd.Flags().Changed(optionNameLogSinkBuffer) {
		buffer, _ := cmd.Flags().GetInt(optionNameLogSinkBuffer)
		opts = append(opts, log.WithSinkBuffer(buffer))
		if buffer <= 0 {
			opts = append(opts, log.WithSynchronousSink())
		}
	}

	return log.NewLogger(node.LoggerName, opts...).Register(), nil
}

func (c *command) CheckUnknownParams(cmd *cobra.Command, args []string) error {
	if err := c.initConfig(); err != nil {
		return err
	}
	var unknownParams []string
	for _, v := range c.config.AllKeys() {
		if cmd.Flags().Lookup(v) != nil {
			continue
		}
		// Only accept the dotted→hyphenated form for explicitly registered
		// nested config keys.
		if knownNestedKeys[v] && cmd.Flags().Lookup(strings.ReplaceAll(v, ".", "-")) != nil {
			continue
		}
		unknownParams = append(unknownParams, v)
	}

	if len(unknownParams) > 0 {
		return fmt.Errorf("unknown parameters:\n\t%v", strings.Join(unknownParams, "\n\t"))
	}

	return nil
}
