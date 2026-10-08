// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package listener

import (
	"context"
	"errors"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/postage/batchservice"
	"github.com/ethersphere/bee/v2/pkg/safe"
	"github.com/ethersphere/bee/v2/pkg/transaction"
	"github.com/ethersphere/bee/v2/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
)

// loggerName is the tree path name of the logger for this package.
const loggerName = "listener"

const (
	blockPage          = 5000      // how many blocks to sync every time we page
	blockPageSnapshot  = 50000     // how many blocks to sync every time from snapshot
	defaultBatchFactor = uint64(5) // minimal number of blocks to sync at once

	// minBlockPage is the smallest page the listener falls back to when log
	// queries fail: 500 s of chain at 5 s blocks, 200 s at 2 s (#583).
	minBlockPage = 100
	// pageGrowAfter is how many pages applied in a row at a reduced size
	// double the page again (#583).
	pageGrowAfter = 3
)

// DefaultConfirmationDepth is how many blocks behind the chain head events must
// be before they are applied, unless postage-confirmation-depth sets another
// value (#545). It was the fixed tailSize before.
const DefaultConfirmationDepth = 4

// MaxConfirmationDepth is the largest postage-confirmation-depth a node accepts.
// While the listener waits for the chain to move that far past what it has
// synced, it makes no progress, and after the postage stall timeout (10
// minutes) the batch store becomes stale (#583). 64 blocks is 320 s at 5 s
// blocks and 128 s at 2 s blocks, so even raising the depth from 4 to 64 on a
// synced node stays under it. An Ethereum reorg rolls Gnosis Chain back about
// 6 to 12 blocks (#545).
const MaxConfirmationDepth = 64

// for testing, set externally
var batchFactorOverridePublic = "5"

// While the batch store is stale the node stays up, and the watcher checks
// the state, keeps the gauges current and repeats the stale Warning (#583).
// Variables only so tests can shorten them; New copies them.
var (
	staleWatchInterval = time.Second
	staleWarnRepeat    = 30 * time.Minute
	// staleMaxBackoff caps the wait after a failed call while stale; the wait
	// doubles from backoffTime only while stale (#583).
	staleMaxBackoff = 60 * time.Second
)

// Deadlines of the listener's own chain calls. Without them a call that never
// answers blocks the loop until the connection itself fails (#583). They are
// variables only so tests can shorten them; New copies them.
var (
	blockNumberTimeout = 30 * time.Second
	filterLogsTimeout  = 60 * time.Second
)

var (
	// ErrPostageSyncingStalled is the stop cause when postage-stall-shutdown
	// is set and the batch store stayed stale that long (#583). A stall alone
	// no longer stops the node.
	ErrPostageSyncingStalled = errors.New("postage syncing stalled")
	ErrPostagePaused         = errors.New("postage contract is paused")
	ErrParseSnapshot         = errors.New("failed to parse snapshot data")
)

type BlockHeightContractFilterer interface {
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)
	BlockNumber(context.Context) (uint64, error)
}

type listener struct {
	logger    log.Logger
	ev        BlockHeightContractFilterer
	blockTime func() time.Duration // read each time, so a block time change is followed (#540)
	// confirmationDepth is how many blocks behind the head events must be
	// before they are applied (#545).
	confirmationDepth uint64

	postageStampContractAddress common.Address
	postageStampContractABI     abi.ABI
	quit                        chan struct{}
	wg                          sync.WaitGroup
	metrics                     metrics
	stallingTimeout             time.Duration
	backoffTime                 time.Duration
	syncingStopped              *syncutil.Signaler
	blockNumberTimeout          time.Duration
	filterLogsTimeout           time.Duration
	watchInterval               time.Duration
	warnRepeat                  time.Duration
	maxBackoff                  time.Duration
	// stallShutdown, when positive, stops the node once the batch store has
	// been stale this long (postage-stall-shutdown, #583). Zero never stops.
	stallShutdown time.Duration

	// Sync health (#583). Written by the loop and the watcher, read by
	// Stale and the node.
	listening    atomic.Bool
	lastProgress atomic.Int64 // unix nanoseconds of the last applied page
	// staleFrom is when the current or last stale period began (unix
	// nanoseconds), caughtUpAt when a caught-up page was last applied. The
	// batch store is stale while staleFrom is the later of the two.
	staleFrom    atomic.Int64
	caughtUpAt   atomic.Int64
	caughtUpOnce atomic.Bool  // a caught-up page was applied since Listen
	staleSince   atomic.Int64 // unix nanoseconds the stale state began, 0 if not stale
	staleEndedAt atomic.Int64 // unix nanoseconds the stale state last ended, 0 if never
	headOkAt     atomic.Int64 // unix nanoseconds of the last successful block number call
	behind       atomic.Int64 // confirmed head minus the next block, at headOkAt
	lastErr      atomic.Value // string: the last backend error

	// Cached postage stamp contract event topics.
	batchCreatedTopic       common.Hash
	batchTopUpTopic         common.Hash
	batchDepthIncreaseTopic common.Hash
	priceUpdateTopic        common.Hash
	pausedTopic             common.Hash
}

func New(
	syncingStopped *syncutil.Signaler,
	logger log.Logger,
	ev BlockHeightContractFilterer,
	postageStampContractAddress common.Address,
	postageStampContractABI abi.ABI,
	blockTime func() time.Duration,
	confirmationDepth uint64,
	stallingTimeout time.Duration,
	backoffTime time.Duration,
	opts ...Option,
) postage.Listener {
	l := &listener{
		syncingStopped:              syncingStopped,
		logger:                      logger.WithName(loggerName).Register(),
		ev:                          ev,
		blockTime:                   blockTime,
		confirmationDepth:           confirmationDepth,
		postageStampContractAddress: postageStampContractAddress,
		postageStampContractABI:     postageStampContractABI,
		quit:                        make(chan struct{}),
		metrics:                     newMetrics(),
		stallingTimeout:             stallingTimeout,
		backoffTime:                 backoffTime,
		blockNumberTimeout:          blockNumberTimeout,
		filterLogsTimeout:           filterLogsTimeout,
		watchInterval:               staleWatchInterval,
		warnRepeat:                  staleWarnRepeat,
		maxBackoff:                  staleMaxBackoff,

		batchCreatedTopic:       postageStampContractABI.Events["BatchCreated"].ID,
		batchTopUpTopic:         postageStampContractABI.Events["BatchTopUp"].ID,
		batchDepthIncreaseTopic: postageStampContractABI.Events["BatchDepthIncrease"].ID,
		priceUpdateTopic:        postageStampContractABI.Events["PriceUpdate"].ID,
		pausedTopic:             postageStampContractABI.Events["Paused"].ID,
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Option configures a listener.
type Option func(*listener)

// WithStallShutdown stops the node once the batch store has been stale for d
// (postage-stall-shutdown). Zero, the default, never stops it (#583).
func WithStallShutdown(d time.Duration) Option {
	return func(l *listener) { l.stallShutdown = d }
}

// Stale reports whether the batch store is stale: no page applied for the
// stall timeout, or not caught up since that happened. It is computed when
// read, so a chain call that hangs cannot keep it from turning true (#583).
func (l *listener) Stale() bool {
	if !l.listening.Load() {
		return false
	}
	last := l.lastProgress.Load()
	if time.Since(time.Unix(0, last)) >= l.stallingTimeout {
		l.markStale(last)
		return true
	}
	return l.staleFrom.Load() > l.caughtUpAt.Load()
}

// markStale records a stale period that began one stall timeout after the
// given last progress. The start comes from the progress that was read, not
// from the time of the write, so a write that lands after a caught-up page
// cannot leave the node stale: the page was applied after that start.
func (l *listener) markStale(lastProgress int64) {
	from := lastProgress + int64(l.stallingTimeout)
	for {
		old := l.staleFrom.Load()
		if old >= from || l.staleFrom.CompareAndSwap(old, from) {
			return
		}
	}
}

// markCaughtUp records an applied page that reached the confirmed head.
func (l *listener) markCaughtUp() {
	l.caughtUpAt.Store(time.Now().UnixNano())
}

// CaughtUp reports whether a page that reached the confirmed head has been
// applied since the listener started, and the batch store is not stale now.
func (l *listener) CaughtUp() bool {
	return l.listening.Load() && l.caughtUpOnce.Load() && !l.Stale()
}

// SinceProgress returns the time since the listener last applied a page, or
// zero before it has started.
func (l *listener) SinceProgress() time.Duration {
	if !l.listening.Load() {
		return 0
	}
	return time.Since(time.Unix(0, l.lastProgress.Load()))
}

// StaleEndedAt returns when the stale state last ended, or the zero time if
// it never has.
func (l *listener) StaleEndedAt() time.Time {
	if n := l.staleEndedAt.Load(); n != 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

func (l *listener) setLastErr(err error) {
	l.lastErr.Store(err.Error())
}

func (l *listener) lastError() string {
	if v, ok := l.lastErr.Load().(string); ok {
		return v
	}
	return ""
}

func (l *listener) filterQuery(from, to *big.Int) ethereum.FilterQuery {
	return ethereum.FilterQuery{
		FromBlock: from,
		ToBlock:   to,
		Addresses: []common.Address{
			l.postageStampContractAddress,
		},
		Topics: [][]common.Hash{
			{
				l.batchCreatedTopic,
				l.batchTopUpTopic,
				l.batchDepthIncreaseTopic,
				l.priceUpdateTopic,
				l.pausedTopic,
			},
		},
	}
}

func (l *listener) processEvent(e types.Log, updater postage.EventUpdater) error {
	defer l.metrics.EventsProcessed.Inc()
	switch e.Topics[0] {
	case l.batchCreatedTopic:
		c := &batchCreatedEvent{}
		err := transaction.ParseEvent(&l.postageStampContractABI, "BatchCreated", c, e)
		if err != nil {
			return err
		}
		l.metrics.CreatedCounter.Inc()
		return updater.Create(
			c.BatchId[:],
			c.Owner.Bytes(),
			c.TotalAmount,
			c.NormalisedBalance,
			c.Depth,
			c.BucketDepth,
			c.ImmutableFlag,
			e.TxHash,
		)
	case l.batchTopUpTopic:
		c := &batchTopUpEvent{}
		err := transaction.ParseEvent(&l.postageStampContractABI, "BatchTopUp", c, e)
		if err != nil {
			return err
		}
		l.metrics.TopupCounter.Inc()
		return updater.TopUp(
			c.BatchId[:],
			c.TopupAmount,
			c.NormalisedBalance,
			e.TxHash,
		)
	case l.batchDepthIncreaseTopic:
		c := &batchDepthIncreaseEvent{}
		err := transaction.ParseEvent(&l.postageStampContractABI, "BatchDepthIncrease", c, e)
		if err != nil {
			return err
		}
		l.metrics.DepthCounter.Inc()
		return updater.UpdateDepth(
			c.BatchId[:],
			c.NewDepth,
			c.NormalisedBalance,
			e.TxHash,
		)
	case l.priceUpdateTopic:
		c := &priceUpdateEvent{}
		err := transaction.ParseEvent(&l.postageStampContractABI, "PriceUpdate", c, e)
		if err != nil {
			return err
		}
		l.metrics.PriceCounter.Inc()
		return updater.UpdatePrice(
			c.Price,
			e.TxHash,
		)
	case l.pausedTopic:
		l.logger.Warning("Postage contract is paused.")
		return ErrPostagePaused
	default:
		l.metrics.EventErrors.Inc()
		return errors.New("unknown event")
	}
}

func (l *listener) Listen(ctx context.Context, from uint64, updater postage.EventUpdater) <-chan error {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-l.quit
		cancel()
	}()

	processEvents := func(events []types.Log, to uint64) error {
		if err := updater.TransactionStart(); err != nil {
			return err
		}

		for _, e := range events {
			startEv := time.Now()
			err := updater.UpdateBlockNumber(e.BlockNumber)
			if err != nil {
				return err
			}
			if err = l.processEvent(e, updater); err != nil {
				// if we have a zero value batch - silence & log then move on
				if !errors.Is(err, batchservice.ErrZeroValueBatch) {
					return err
				}
				l.logger.Debug("failed processing event", "error", err)
			}
			totalTimeMetric(l.metrics.EventProcessDuration, startEv)
		}

		err := updater.UpdateBlockNumber(to)
		if err != nil {
			return err
		}

		if err := updater.TransactionEnd(); err != nil {
			return err
		}

		return nil
	}

	batchFactor, err := strconv.ParseUint(batchFactorOverridePublic, 10, 64)
	if err != nil {
		l.logger.Warning("batch factor conversation failed", "batch_factor", batchFactor, "error", err)
		batchFactor = defaultBatchFactor
	}

	l.logger.Debug("batch factor", "value", batchFactor)

	// Type assertion to detect if backend is SnapshotLogFilterer
	pageSize := uint64(blockPage)
	_, isSnapshot := l.ev.(interface{ GetBatchSnapshot() []byte })
	if isSnapshot {
		pageSize = blockPageSnapshot
		l.logger.Debug("using snapshot page size", "page_size", pageSize)
	} else {
		l.logger.Debug("using standard page size", "page_size", pageSize)
	}

	synced := make(chan error)
	closeOnce := new(sync.Once)
	// sendSynced reports to the one reader of synced, at most once. It gives
	// up when the listener closes, so a send nobody reads cannot block
	// shutdown.
	sendSynced := func(err error) {
		closeOnce.Do(func() {
			select {
			case synced <- err:
			case <-l.quit:
			}
		})
	}
	paged := true

	// The page shrinks when a log query fails and grows back after pages
	// apply at the smaller size; the wait after a failed call grows only
	// while stale (#583).
	page := pageSize
	pagesAtSize := 0
	errWait := l.backoffTime
	l.metrics.PageBlocks.Set(float64(page))
	failedCall := func() {
		if !l.Stale() {
			errWait = l.backoffTime
			return
		}
		if errWait <= 0 {
			errWait = l.backoffTime
		}
		errWait *= 2
		if errWait > l.maxBackoff {
			errWait = l.maxBackoff
		}
	}

	l.lastProgress.Store(time.Now().UnixNano())
	l.staleFrom.Store(0)
	l.caughtUpAt.Store(0)
	l.caughtUpOnce.Store(false)
	l.listening.Store(true)
	lastConfirmedBlock := uint64(0)

	// The snapshot listener's caller waits for the snapshot to be applied
	// and treats the synced signal as success, so a stale snapshot load
	// must not end that wait; only the chain listener's startup wait ends
	// when it becomes stale.
	watchSynced := sendSynced
	if isSnapshot {
		watchSynced = func(error) {}
	}
	l.wg.Add(1)
	go l.watch(ctx, watchSynced)

	l.wg.Add(1)
	listenf := safe.RunFunc(l.logger, "postage-listener-func", func() error {
		defer l.wg.Done()
		for {
			// A stall no longer ends the loop: the watcher marks the batch
			// store stale and the node stays up in a degraded state (#583).

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// if we have a last blocknumber from the backend we can make a good estimate on when we need to requery
			// otherwise we just use the backoff time
			var expectedWaitTime time.Duration
			if lastConfirmedBlock != 0 {
				nextExpectedBatchBlock := (lastConfirmedBlock/batchFactor + 1) * batchFactor
				remainingBlocks := nextExpectedBatchBlock - lastConfirmedBlock
				expectedWaitTime = l.blockTime() * time.Duration(remainingBlocks)
			} else {
				expectedWaitTime = errWait
			}

			if !paged {
				l.logger.Debug("sleeping until next block batch", "duration", expectedWaitTime)
				select {
				case <-time.After(expectedWaitTime):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			paged = false

			start := time.Now()

			l.metrics.BackendCalls.Inc()
			callCtx, cancelCall := context.WithTimeout(ctx, l.blockNumberTimeout)
			to, err := l.ev.BlockNumber(callCtx)
			cancelCall()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				if errors.Is(err, ErrParseSnapshot) {
					return err
				}
				l.metrics.BackendErrors.Inc()
				l.logger.Warning("could not get block number", "error", err)
				l.setLastErr(err)
				failedCall()
				lastConfirmedBlock = 0
				continue
			}
			l.headOkAt.Store(time.Now().UnixNano())

			if to < l.confirmationDepth {
				// in a test blockchain there might be not be enough blocks yet
				continue
			}

			// consider to-confirmationDepth as the "latest" block we need to sync to
			to = to - l.confirmationDepth
			lastConfirmedBlock = to
			if to >= from {
				l.behind.Store(int64(to - from))
			} else {
				l.behind.Store(0)
			}

			// round down to the largest multiple of batchFactor
			to = (to / batchFactor) * batchFactor

			if to < from {
				// if the blockNumber is actually less than what we already, it might mean the backend is not synced or some reorg scenario
				continue
			}

			// do some paging (sub-optimal)
			// A page in the non-paged branch reaches the confirmed head: once
			// it is applied, the listener is caught up (#583).
			caughtUpPage := true
			if to-from >= page {
				paged = true
				caughtUpPage = false
				to = from + page - 1
			}
			l.metrics.BackendCalls.Inc()

			callCtx, cancelCall = context.WithTimeout(ctx, l.filterLogsTimeout)
			events, err := l.ev.FilterLogs(callCtx, l.filterQuery(big.NewInt(int64(from)), big.NewInt(int64(to))))
			cancelCall()
			if err != nil {
				if errors.Is(err, ErrParseSnapshot) {
					return err
				}
				l.metrics.BackendErrors.Inc()
				l.logger.Warning("could not get blockchain log", "error", err)
				l.setLastErr(err)
				failedCall()
				// The block number answered and the log query did not: try
				// a smaller page next, down to minBlockPage. A failed block
				// number query does not change the page (#583).
				if page > minBlockPage {
					page /= 2
					if page < minBlockPage {
						page = minBlockPage
					}
					l.metrics.PageBlocks.Set(float64(page))
				}
				pagesAtSize = 0
				lastConfirmedBlock = 0
				// Wait backoffTime before retrying, as after a failed block
				// number query; a paged pass would otherwise retry at once.
				// See #578.
				paged = false
				continue
			}

			if err := processEvents(events, to); err != nil {
				return err
			}

			from = to + 1
			l.lastProgress.Store(time.Now().UnixNano())
			errWait = l.backoffTime
			if page < pageSize {
				pagesAtSize++
				if pagesAtSize >= pageGrowAfter {
					page *= 2
					if page > pageSize {
						page = pageSize
					}
					pagesAtSize = 0
					l.metrics.PageBlocks.Set(float64(page))
				}
			}
			if caughtUpPage {
				// Caught up: the stale state ends only here, not on any
				// applied page, and the startup wait ends (#583).
				l.markCaughtUp()
				l.caughtUpOnce.Store(true)
				sendSynced(nil)
			}
			totalTimeMetric(l.metrics.PageProcessDuration, start)
			l.metrics.PagesProcessed.Inc()
		}
	})

	go func() {
		err := listenf()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// Context cancelled is returned on shutdown, therefore we do nothing here.
				l.logger.Debug("shutting down event listener")
				return
			}
			// Only faults that waiting cannot fix end the loop: a failure to
			// apply events, a broken snapshot, a paused contract (#583).
			l.logger.Error(err, "postage listener stopped on an error it cannot recover from; shutting down node")
		}
		sendSynced(err)
		if l.syncingStopped != nil {
			l.syncingStopped.SignalWithError(err) // trigger shutdown in start.go
		}
	}()

	return synced
}

// watch follows the sync health while the listener runs (#583). It marks the
// node stale and ends the startup wait when the batch store goes stale, keeps
// the gauges current, repeats the stale Warning, logs the recovery, and stops
// the node when postage-stall-shutdown is set and reached.
func (l *listener) watch(ctx context.Context, sendSynced func(error)) {
	defer l.wg.Done()

	t := time.NewTicker(l.watchInterval)
	defer t.Stop()

	var (
		wasStale bool
		lastWarn time.Time
		stopped  bool
	)
	for {
		select {
		case <-l.quit:
			return
		case <-ctx.Done():
			return
		case <-t.C:
		}

		now := time.Now()
		stale := l.Stale()
		since := l.SinceProgress()
		l.metrics.SecondsSinceProgress.Set(since.Seconds())

		switch {
		case stale && !wasStale:
			l.staleSince.Store(now.UnixNano())
			l.metrics.Stale.Set(1)
			l.warnStale(since)
			lastWarn = now
			// At startup the node waits for the listener; a stale batch
			// store ends that wait, and the node comes up degraded.
			sendSynced(nil)
		case stale && now.Sub(lastWarn) >= l.warnRepeat:
			l.warnStale(since)
			lastWarn = now
		case !stale && wasStale:
			began := time.Unix(0, l.staleSince.Load())
			l.staleSince.Store(0)
			l.staleEndedAt.Store(now.UnixNano())
			l.metrics.Stale.Set(0)
			l.logger.Info("postage sync caught up", "stale_for", now.Sub(began).Round(time.Second))
		}
		wasStale = stale

		l.metrics.BlocksBehind.Set(float64(l.blocksBehind()))

		if stale && !stopped && l.stallShutdown > 0 && now.Sub(time.Unix(0, l.staleSince.Load())) >= l.stallShutdown {
			stopped = true
			l.logger.Error(ErrPostageSyncingStalled, "batch store stale for longer than postage-stall-shutdown; shutting down node", "stale_for", l.stallShutdown)
			if l.syncingStopped != nil {
				l.syncingStopped.SignalWithError(ErrPostageSyncingStalled)
			}
		}
	}
}

// blocksBehind returns the confirmed head minus the listener's next block,
// or -1 while stale when no block number call has succeeded since the stale
// state began.
func (l *listener) blocksBehind() int64 {
	if since := l.staleSince.Load(); since != 0 && l.headOkAt.Load() < since {
		return -1
	}
	return l.behind.Load()
}

func (l *listener) warnStale(since time.Duration) {
	l.logger.Warning("postage sync stalled; the node stays up and serves content but does not play the storage lottery until the batch store catches up; chunks of batches not seen yet are held and served, and validated once caught up",
		"since_progress", since.Round(time.Second),
		"blocks_behind", l.blocksBehind(),
		"last_error", l.lastError(),
	)
}

func (l *listener) Close() error {
	close(l.quit)

	done := make(chan struct{})
	go func() {
		defer close(done)
		l.wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return errors.New("postage listener closed with running goroutines")
	}
	return nil
}

type batchCreatedEvent struct {
	BatchId           [32]byte
	TotalAmount       *big.Int
	NormalisedBalance *big.Int
	Owner             common.Address
	Depth             uint8
	BucketDepth       uint8
	ImmutableFlag     bool
}

type batchTopUpEvent struct {
	BatchId           [32]byte
	TopupAmount       *big.Int
	NormalisedBalance *big.Int
}

type batchDepthIncreaseEvent struct {
	BatchId           [32]byte
	NewDepth          uint8
	NormalisedBalance *big.Int
}

type priceUpdateEvent struct {
	Price *big.Int
}

func totalTimeMetric(metric prometheus.Counter, start time.Time) {
	totalTime := time.Since(start)
	metric.Add(float64(totalTime))
}

var _ postage.SyncHealth = (*listener)(nil)
