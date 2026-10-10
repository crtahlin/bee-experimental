// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/storageutil"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

const (
	reserveOverCapacity = "reserveOverCapacity"
	reserveUnreserved   = "reserveUnreserved"
	batchExpiry         = "batchExpiry"
	batchExpiryDone     = "batchExpiryDone"
)

var (
	errMaxRadius            = errors.New("max radius reached")
	reserveSizeWithinRadius atomic.Uint64
)

type Syncer interface {
	// Number of active historical syncing jobs.
	SyncRate() float64
	Start(context.Context)
}

// RadiusSyncReporter is implemented by a Syncer that can tell since when it
// has been syncing at a storage radius. The reserve worker then lowers the
// radius again only after the syncer has acted on the current one (#696).
type RadiusSyncReporter interface {
	// ActedOnRadius returns the storage radius the puller has acted on and
	// since when. ok is false until it has acted on any radius.
	ActedOnRadius() (radius uint8, since time.Time, ok bool)
}

// Reasons a radius decrease is deferred (#696).
const (
	deferNotStarted  = "puller_not_started"
	deferNotAtRadius = "puller_not_at_radius"
	deferSettling    = "puller_settling"
)

// radiusDecreaseDeferral returns why a decrease from radius must wait for the
// puller, or "" when it may go ahead. Without a reporter it never defers.
func (db *DB) radiusDecreaseDeferral(radius uint8, now time.Time) (reason string, acted uint8, age time.Duration) {
	if db.radiusReporter == nil {
		return "", 0, 0
	}
	acted, since, ok := db.radiusReporter.ActedOnRadius()
	if !ok {
		return deferNotStarted, 0, 0
	}
	age = now.Sub(since)
	if acted != radius {
		return deferNotAtRadius, acted, age
	}
	if age < db.reserveOptions.decreaseSettle {
		return deferSettling, acted, age
	}
	return "", acted, age
}

func threshold(capacity int) int { return capacity * 5 / 10 }

func (db *DB) startReserveWorkers(
	ctx context.Context,
	radius func() (uint8, error),
	ready chan<- struct{},
) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-db.quit
		cancel()
	}()

	db.inFlight.Add(1)
	go db.reserveWorker(ctx, ready)

	// Held chunks are validated only with the reserve worker running and
	// after storer recovery, which New has finished by now (#583).
	db.inFlight.Add(1)
	go db.heldValidator(ctx)

	sub, unsubscribe := db.reserveOptions.startupStabilizer.Subscribe()
	defer unsubscribe()

	select {
	case <-sub:
		db.logger.Debug("node warmup check completed")
	case <-db.quit:
		return
	}

	r, err := radius()
	if err != nil {
		db.logger.Error(err, "reserve worker initial radius")
		return // node shutdown
	}

	err = db.reserve.SetRadius(r)
	if err != nil {
		db.logger.Error(err, "reserve set radius")
	} else {
		db.metrics.StorageRadius.Set(float64(r))
	}

	// syncing can now begin now that the reserver worker is running
	db.syncer.Start(ctx)
}

// countChunksWithinRadius counts reserve chunks at or beyond the storage
// radius.
//
// It exists so that the definition lives in one place. This quantity was
// computed independently in two places — here, feeding /status, and in
// DebugInfo, feeding /debugstorage — with different iteration bases and no
// shared code, so the two could report different numbers for the same thing.
// That is confusing exactly when someone is debugging. See issue #34.
func (db *DB) countChunksWithinRadius() (int, error) {
	defer db.recordScan("count")()

	count := 0
	err := db.reserve.IterateChunksItems(db.StorageRadius(), func(*reserve.ChunkBinItem) (bool, error) {
		select {
		case <-db.quit:
			return true, ErrDBQuit
		default:
		}

		count++
		return false, nil
	})
	return count, err
}

// recordScan times a full pass over the reserve index. The returned function
// records the elapsed time, so a caller times a pass with a single deferred
// call.
//
// kind says what the pass was for, because the passes differ in what they cost
// and in how often they have to happen, and a single number covering all of
// them would answer neither question. See issue #28 and
// docs/experiments/expired-batch-sweep/spec.md.
func (db *DB) recordScan(kind string) func() {
	start := time.Now()
	return func() {
		db.metrics.ReserveScanDuration.WithLabelValues(kind).Observe(time.Since(start).Seconds())
	}
}

func (db *DB) countWithinRadius(ctx context.Context) (int, error) {
	// "combined" rather than "count", because this pass does two jobs: it
	// counts within radius and it reconciles chunks against the batches they
	// claim. Labelling it the same as the count-only pass would make the two
	// look comparable when the split in #28 separates them, and the whole point
	// of measuring first is to be able to compare.
	defer db.recordScan("combined")()

	count := 0
	missing := 0
	radius := db.StorageRadius()

	evictBatches := make(map[string]bool)

	// Batch existence is asked once per BATCH, not once per chunk. Every chunk
	// carries a batch id, but a reserve holds orders of magnitude more chunks
	// than distinct batches — 4.06 million against 426 on the bench node — and
	// batchstore.Exists takes a read lock and does a store lookup each time.
	//
	// Caching within a single scan changes nothing observable. A batch cannot
	// meaningfully appear or disappear part-way through: which chunks saw the
	// old answer and which saw the new one would depend on iteration order, so
	// the previous behaviour was already arbitrary in that case.
	batchExists := make(map[string]bool, 64)
	exists := func(batchID []byte) bool {
		key := string(batchID)
		if e, seen := batchExists[key]; seen {
			return e
		}
		e, err := db.batchstore.Exists(batchID)
		if err != nil {
			// Preserve the previous behaviour: an error is not treated as
			// absence, so a transient failure cannot cause an eviction.
			return true
		}
		batchExists[key] = e
		return e
	}

	err := db.reserve.IterateChunksItems(0, func(ci *reserve.ChunkBinItem) (bool, error) {
		select {
		case <-db.quit:
			// Stop before touching the store again: on shutdown the store is
			// being closed, and iterating into a closed store segfaults under
			// pebble. See issue #399.
			return true, ErrDBQuit
		default:
		}

		if ci.Bin >= radius {
			count++
		}

		if !exists(ci.BatchID) {
			missing++
			evictBatches[string(ci.BatchID)] = true
		}
		return false, nil
	})
	if err != nil {
		return 0, err
	}

	for batch := range evictBatches {
		db.logger.Debug("reserve: invalid batch", "batch_id", hex.EncodeToString([]byte(batch)))
		err = errors.Join(err, db.EvictBatch(ctx, []byte(batch)))
	}

	db.metrics.ReserveSizeWithinRadius.Set(float64(count))
	db.metrics.ReserveMissingBatch.Set(float64(missing))
	reserveSizeWithinRadius.Store(uint64(count))

	return count, err
}

// reserveWakeupScan returns the within-radius reserve count for the radius
// decision the wake-up makes. It runs the combined pass, which also reconciles
// every chunk against the batch it claims and evicts any whose batch has
// vanished, when a reconciliation sweep is due; otherwise it runs the cheap
// within-radius count that starts at the storage radius.
//
// A sweep is due on every wake-up when batchSweepInterval is zero, which is the
// previous behaviour, and once per interval otherwise. Splitting the two jobs
// this way lets the frequent wake-up pay only for the count while the
// reconciliation, a net that finds nothing on a healthy node, runs on its own
// cadence. See issue #28 and docs/experiments/expired-batch-sweep/spec.md.
func (db *DB) reserveWakeupScan(ctx context.Context, lastSweep *time.Time) (int, error) {
	interval := db.reserveOptions.batchSweepInterval
	if interval == 0 || time.Since(*lastSweep) >= interval {
		*lastSweep = time.Now()
		return db.countWithinRadius(ctx)
	}

	count, err := db.countChunksWithinRadius()
	if err != nil {
		return 0, err
	}
	db.metrics.ReserveSizeWithinRadius.Set(float64(count))
	reserveSizeWithinRadius.Store(uint64(count))
	return count, nil
}

func (db *DB) reserveWorker(ctx context.Context, ready chan<- struct{}) {
	defer db.inFlight.Done()

	batchExpiryTrigger, batchExpiryUnsub := db.events.Subscribe(batchExpiry)
	defer batchExpiryUnsub()

	overCapTrigger, overCapUnsub := db.events.Subscribe(reserveOverCapacity)
	defer overCapUnsub()

	thresholdTicker := time.NewTicker(db.reserveOptions.wakeupDuration)
	defer thresholdTicker.Stop()

	_, _ = db.countWithinRadius(ctx)
	lastSweep := time.Now()

	if !db.reserve.IsWithinCapacity() {
		db.events.Trigger(reserveOverCapacity)
	}

	if ready != nil {
		close(ready)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-batchExpiryTrigger:

			err := db.evictionRun(evictionKindExpiry, func() error { return db.evictExpiredBatches(ctx) })
			if err != nil {
				// A shutdown is not a fault, and the worker is stopping
				// anyway, so it returns rather than warning (#407).
				if errors.Is(err, ErrDBQuit) {
					return
				}
				db.logger.Warning("reserve worker evict expired batches", "error", err)
			}

			db.events.Trigger(batchExpiryDone)

			if !db.reserve.IsWithinCapacity() {
				db.events.Trigger(reserveOverCapacity)
			}

		case <-overCapTrigger:

			db.metrics.OverCapTriggerCount.Inc()
			if err := db.evictionRun(evictionKindUnreserve, func() error { return db.unreserve(ctx) }); err != nil {
				if errors.Is(err, ErrDBQuit) {
					return
				}
				db.logger.Warning("reserve worker unreserve", "error", err)
			}

		case <-thresholdTicker.C:

			radius := db.reserve.Radius()
			count, err := db.reserveWakeupScan(ctx, &lastSweep)
			if err != nil {
				if errors.Is(err, ErrDBQuit) {
					return
				}
				db.logger.Warning("reserve worker count within radius", "error", err)
				continue
			}

			// While the batch store is stale, and for one sync-rate window
			// after, the radius does not drop: the reserve size is not
			// reliable while chunks are held, and neither is the sync rate
			// (#583). A radius increase stays possible.
			if db.radiusDecreaseBlocked() {
				db.logger.Debug("reserve radius decrease skipped while the batch store is stale or just recovered", "radius", radius)
				continue
			}

			if count < threshold(db.reserve.Capacity()) && db.syncer.SyncRate() == 0 && radius > db.reserveOptions.minimumRadius {
				// A sync rate of 0 also holds while the puller has not synced at
				// this radius at all; lower it again only once it has (#696).
				if reason, acted, age := db.radiusDecreaseDeferral(radius, time.Now()); reason != "" {
					db.metrics.RadiusDecreaseDeferred.WithLabelValues(reason).Inc()
					db.logger.Debug("reserve radius decrease deferred until the puller has synced at the current radius", "radius", radius, "puller_radius", acted, "since", age, "reason", reason)
					continue
				}
				radius--
				if err := db.reserve.SetRadius(radius); err != nil {
					db.logger.Error(err, "reserve set radius")
				}
				db.metrics.StorageRadius.Set(float64(radius))
				db.logger.Info("reserve radius decrease", "radius", radius)
			}
		}
	}
}

func (db *DB) evictExpiredBatches(ctx context.Context) error {
	batches, err := db.getExpiredBatches()
	if err != nil {
		return err
	}
	if len(batches) > 0 {
		rate, workers := db.evictionRateAndWorkers()
		db.logger.Info("evict expired batches start", "batches", len(batches), "rate", rate, "workers", workers)
	}
	// Expired batches use the same rounds and limiter as unreserve (#623).
	// A batch expiry does not interrupt this eviction, as before.
	hooks := db.evictionHooks(ctx, true, nil)

	for _, batchID := range batches {
		// wasp #407: stop between batches on shutdown. Close waits five
		// seconds for this worker and then closes the store anyway, so a
		// long evict that keeps running is the read-after-close #399 fixed
		// for the scan. A batch boundary is a safe point: the batch just
		// evicted is committed and the next has not started.
		select {
		case <-db.quit:
			return ErrDBQuit
		default:
		}

		evicted, err := db.evictBatch(ctx, batchID, math.MaxInt, swarm.MaxBins, hooks)
		if err != nil {
			return err
		}
		if evicted > 0 {
			db.logger.Debug("evicted expired batch", "batch_id", hex.EncodeToString(batchID), "total_evicted", evicted)
		}
		err = db.storage.Run(ctx, func(st transaction.Store) error {
			return st.IndexStore().Delete(&expiredBatchItem{BatchID: batchID})
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) getExpiredBatches() ([][]byte, error) {
	var batchesToEvict [][]byte
	err := db.storage.IndexStore().Iterate(storage.Query{
		Factory:      func() storage.Item { return new(expiredBatchItem) },
		ItemProperty: storage.QueryItemID,
	}, func(result storage.Result) (bool, error) {
		batchesToEvict = append(batchesToEvict, []byte(result.ID))
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return batchesToEvict, nil
}

func (db *DB) evictBatch(
	ctx context.Context,
	batchID []byte,
	evictCount int,
	upToBin uint8,
	hooks reserve.EvictionHooks,
) (evicted int, err error) {
	dur := captureDuration(time.Now())
	defer func() {
		db.metrics.ReserveSize.Set(float64(db.reserve.Size()))
		db.metrics.MethodCallsDuration.WithLabelValues("reserve", "EvictBatch").Observe(dur())
		if err == nil {
			db.metrics.MethodCalls.WithLabelValues("reserve", "EvictBatch", "success").Inc()
		} else {
			db.metrics.MethodCalls.WithLabelValues("reserve", "EvictBatch", "failure").Inc()
		}
		// The evicted and expired counters are updated after each round,
		// in hooks.After (#623).
		db.logger.Debug(
			"reserve eviction",
			"uptoBin", upToBin,
			"evicted", evicted,
			"batchID", hex.EncodeToString(batchID),
			"new_size", db.reserve.Size(),
		)
	}()

	return db.reserve.EvictBatchBin(ctx, batchID, evictCount, upToBin, hooks)
}

// EvictBatch evicts all chunks belonging to a batch from the reserve.
func (db *DB) EvictBatch(ctx context.Context, batchID []byte) error {
	if db.reserve == nil {
		// if reserve is not configured, do nothing
		return nil
	}

	err := db.storage.Run(ctx, func(tx transaction.Store) error {
		return tx.IndexStore().Put(&expiredBatchItem{BatchID: batchID})
	})
	if err != nil {
		return fmt.Errorf("save expired batch: %w", err)
	}

	db.events.Trigger(batchExpiry)
	return nil
}

func (db *DB) ReserveGet(ctx context.Context, addr swarm.Address, batchID []byte, stampHash []byte) (ch swarm.Chunk, err error) {
	dur := captureDuration(time.Now())
	defer func() {
		db.metrics.MethodCallsDuration.WithLabelValues("reserve", "ReserveGet").Observe(dur())
		if err == nil || errors.Is(err, storage.ErrNotFound) {
			db.metrics.MethodCalls.WithLabelValues("reserve", "ReserveGet", "success").Inc()
		} else {
			db.metrics.MethodCalls.WithLabelValues("reserve", "ReserveGet", "failure").Inc()
			db.logger.Debug("reserve get error", "error", err)
		}
	}()

	return db.reserve.Get(ctx, addr, batchID, stampHash)
}

func (db *DB) ReserveHas(addr swarm.Address, batchID []byte, stampHash []byte) (has bool, err error) {
	// Bounded because pullsync calls this once per offered chunk from one
	// goroutine per syncing peer, so the concurrency the reserve sees is the
	// peer count. Unbounded by default; see issue #20.
	//
	// Guarded rather than run unconditionally, because this is the hottest
	// path in pullsync and a node on the default gets nothing from the clock
	// read. Whatever the bound costs should be paid only by nodes that asked
	// for one.
	if db.reserveHasLimiter != nil {
		waitStart := time.Now()
		release, err := acquireSlot(db.reserveHasLimiter, db.quit)
		if err != nil {
			return false, err
		}
		defer release()
		// Recorded so a bound that is too tight is visible as queueing rather
		// than being mistaken for the contention it was meant to remove.
		db.metrics.ReserveHasWaitDuration.Observe(time.Since(waitStart).Seconds())
	}

	dur := captureDuration(time.Now())
	defer func() {
		db.metrics.MethodCallsDuration.WithLabelValues("reserve", "ReserveHas").Observe(dur())
		if err == nil {
			db.metrics.MethodCalls.WithLabelValues("reserve", "ReserveHas", "success").Inc()
		} else {
			db.metrics.MethodCalls.WithLabelValues("reserve", "ReserveHas", "failure").Inc()
			db.logger.Debug("reserve has error", "error", err)
		}
	}()

	return db.reserve.Has(addr, batchID, stampHash)
}

// ReservePutter returns a Putter for inserting chunks into the reserve.
func (db *DB) ReservePutter() storage.Putter {
	return putterWithMetrics{
		storage.PutterFunc(
			func(ctx context.Context, chunk swarm.Chunk) error {
				err := db.reserve.Put(ctx, chunk)
				if err != nil {
					db.logger.Debug("reserve put error", "error", err)
					return fmt.Errorf("reserve putter.Put: %w", err)
				}
				db.reserveBinEvents.Trigger(string(db.po(chunk.Address())))
				if !db.reserve.IsWithinCapacity() {
					db.events.Trigger(reserveOverCapacity)
				}
				db.metrics.ReserveSize.Set(float64(db.reserve.Size()))
				return nil
			},
		),
		db.metrics,
		"reserve",
	}
}

func (db *DB) unreserve(ctx context.Context) (err error) {
	dur := captureDuration(time.Now())
	defer func() {
		db.metrics.MethodCallsDuration.WithLabelValues("reserve", "unreserve").Observe(dur())
		if err == nil {
			db.metrics.MethodCalls.WithLabelValues("reserve", "unreserve", "success").Inc()
		} else {
			db.metrics.MethodCalls.WithLabelValues("reserve", "unreserve", "failure").Inc()
		}
	}()

	radius := db.reserve.Radius()
	defer db.events.Trigger(reserveUnreserved)

	target := db.reserve.EvictionTarget()
	if target <= 0 {
		return nil
	}

	rate, workers := db.evictionRateAndWorkers()
	db.logger.Info("unreserve start", "target", target, "radius", radius, "rate", rate, "workers", workers)

	batchExpiry, unsub := db.events.Subscribe(batchExpiry)
	defer unsub()

	hooks := db.evictionHooks(ctx, false, batchExpiry)

	totalEvicted := 0

	var batches [][]byte
	err = db.batchstore.Iterate(func(b *postage.Batch) (bool, error) {
		batches = append(batches, b.ID)
		return false, nil
	})
	if err != nil {
		return err
	}

	for radius < swarm.MaxBins {

		for _, b := range batches {

			select {
			case <-db.quit:
				// wasp #407: same reason as evictExpiredBatches. Returning
				// the error rather than nil so the caller can tell a
				// shutdown from an ordinary finish.
				return ErrDBQuit
			case <-batchExpiry:
				db.logger.Debug("stopping unreserve, received batch expiration signal")
				return nil
			default:
			}

			evict := max(target-totalEvicted,
				// evict at least a min count
				int(db.reserveOptions.minEvictCount))

			binEvicted, err := db.evictBatch(ctx, b, evict, radius, hooks)
			// eviction happens in batches, so we need to keep track of the total
			// number of chunks evicted even if there was an error
			totalEvicted += binEvicted

			// A paced wait ends early on shutdown or a batch expiry, with
			// the same outcome as the checks between batches above (#623).
			if errors.Is(err, errEvictionExpiry) {
				db.logger.Debug("stopping unreserve, received batch expiration signal")
				return nil
			}

			// we can only get error here for critical cases, for eg. batch commit
			// error, which is not recoverable
			if err != nil {
				return err
			}

			if totalEvicted >= target {
				db.logger.Info("unreserve finished", "evicted", totalEvicted, "radius", radius)
				return nil
			}
		}

		// No radius step while a reserve sample runs: on a default node
		// the sample walks from the storage radius up, and the rounds
		// after a step delete in the bin it walks (#649).
		release, err := db.evictionBarrier(ctx, batchExpiry)
		if errors.Is(err, errEvictionExpiry) {
			db.logger.Debug("stopping unreserve, received batch expiration signal")
			return nil
		}
		if err != nil {
			return err
		}
		radius++
		db.logger.Info("reserve radius increase", "radius", radius)
		_ = db.reserve.SetRadius(radius)
		db.metrics.StorageRadius.Set(float64(radius))
		release()
	}

	return errMaxRadius
}

// ReserveLastBinIDs returns all of the highest binIDs from all the bins in the reserve and the epoch time of the reserve.
func (db *DB) ReserveLastBinIDs() ([]uint64, uint64, error) {
	if db.reserve == nil {
		return nil, 0, nil
	}

	return db.reserve.LastBinIDs()
}

func (db *DB) ReserveIterateChunks(cb func(swarm.Chunk) (bool, error)) error {
	return db.reserve.IterateChunks(0, cb)
}

// IsSampling reports whether a reserve sample is currently running. The puller
// reads it to pause pulling during sampling. See issue #23.
func (db *DB) IsSampling() bool {
	return db.samplingActive.Load() > 0
}

func (db *DB) StorageRadius() uint8 {
	if db.reserve == nil {
		return 0
	}
	return db.reserve.Radius()
}

// RadiusState returns the storage radius and how many times it has
// increased, from one read, or (0, 0) without a reserve (#658).
func (db *DB) RadiusState() (uint8, uint64) {
	if db.reserve == nil {
		return 0, 0
	}
	return db.reserve.RadiusState()
}

func (db *DB) CommittedDepth() uint8 {
	if db.reserve == nil {
		return 0
	}

	return uint8(db.reserveOptions.capacityDoubling) + db.reserve.Radius()
}

func (db *DB) CapacityDoubling() uint8 {
	if db.reserve == nil {
		return 0
	}
	return uint8(db.reserveOptions.capacityDoubling)
}

func (db *DB) ReserveSize() int {
	if db.reserve == nil {
		return 0
	}
	return db.reserve.Size()
}

func (db *DB) ReserveSizeWithinRadius() uint64 {
	return reserveSizeWithinRadius.Load()
}

func (db *DB) IsWithinStorageRadius(addr swarm.Address) bool {
	if db.reserve == nil {
		return false
	}
	return swarm.Proximity(addr.Bytes(), db.baseAddr.Bytes()) >= db.reserve.Radius()
}

// BinC is the result returned from the SubscribeBin channel that contains the chunk address and the binID
type BinC struct {
	Address   swarm.Address
	BinID     uint64
	BatchID   []byte
	StampHash []byte
}

// SubscribeBin returns a channel that feeds all the chunks in the reserve from a certain bin between a start and end binIDs.
func (db *DB) SubscribeBin(ctx context.Context, bin uint8, start uint64) (<-chan *BinC, func(), <-chan error) {
	out := make(chan *BinC)
	done := make(chan struct{})
	errC := make(chan error, 1)

	db.inFlight.Go(func() {
		trigger, unsub := db.reserveBinEvents.Subscribe(string(bin))
		defer unsub()
		defer close(out)

		for {

			err := db.reserve.IterateBin(bin, start, func(a swarm.Address, binID uint64, batchID, stampHash []byte) (bool, error) {
				select {
				case out <- &BinC{Address: a, BinID: binID, BatchID: batchID, StampHash: stampHash}:
					start = binID + 1
				case <-done:
					return true, nil
				case <-db.quit:
					return false, ErrDBQuit
				case <-ctx.Done():
					return false, ctx.Err()
				}

				return false, nil
			})
			if err != nil {
				errC <- err
				return
			}

			select {
			case <-trigger:
			case <-done:
				return
			case <-db.quit:
				errC <- ErrDBQuit
				return
			case <-ctx.Done():
				errC <- err
				return
			}
		}
	})

	var doneOnce sync.Once
	return out, func() {
		doneOnce.Do(func() { close(done) })
	}, errC
}

type NeighborhoodStat struct {
	Neighborhood            swarm.Neighborhood
	ReserveSizeWithinRadius int
	Proximity               uint8
}

func (db *DB) NeighborhoodsStat(ctx context.Context) ([]*NeighborhoodStat, error) {
	radius := db.StorageRadius()
	committedDepth := db.CommittedDepth()

	prefixes := neighborhoodPrefixes(db.baseAddr, int(radius), db.reserveOptions.capacityDoubling)
	neighs := make([]*NeighborhoodStat, len(prefixes))
	for i, n := range prefixes {
		neighs[i] = &NeighborhoodStat{
			Neighborhood:            swarm.NewNeighborhood(n, committedDepth),
			ReserveSizeWithinRadius: 0,
			Proximity:               min(committedDepth, swarm.Proximity(n.Bytes(), db.baseAddr.Bytes())),
		}
	}

	err := db.reserve.IterateChunksItems(0, func(ch *reserve.ChunkBinItem) (bool, error) {
		for _, n := range neighs {
			if swarm.Proximity(ch.Address.Bytes(), n.Neighborhood.Bytes()) >= committedDepth {
				n.ReserveSizeWithinRadius++
				break
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	return neighs, err
}

func neighborhoodPrefixes(base swarm.Address, radius int, suffixLength int) []swarm.Address {
	bitCombinationsCount := int(math.Pow(2, float64(suffixLength)))
	bitSuffixes := make([]uint8, bitCombinationsCount)

	for i := range bitCombinationsCount {
		bitSuffixes[i] = uint8(i)
	}

	binPrefixes := make([]swarm.Address, bitCombinationsCount)

	// copy base address
	for i := range binPrefixes {
		binPrefixes[i] = base.Clone()
	}

	for j := range binPrefixes {
		pseudoAddrBytes := binPrefixes[j].Bytes()

		// set pseudo suffix
		bitSuffixPos := suffixLength - 1
		for l := radius + 0; l < radius+suffixLength+1; l++ {
			index, pos := l/8, l%8

			if hasBit(bitSuffixes[j], uint8(bitSuffixPos)) {
				pseudoAddrBytes[index] = bits.Reverse8(setBit(bits.Reverse8(pseudoAddrBytes[index]), uint8(pos)))
			} else {
				pseudoAddrBytes[index] = bits.Reverse8(clearBit(bits.Reverse8(pseudoAddrBytes[index]), uint8(pos)))
			}

			bitSuffixPos--
		}

		// clear rest of the bits
		for l := radius + suffixLength + 1; l < len(pseudoAddrBytes)*8; l++ {
			index, pos := l/8, l%8
			pseudoAddrBytes[index] = bits.Reverse8(clearBit(bits.Reverse8(pseudoAddrBytes[index]), uint8(pos)))
		}
	}

	return binPrefixes
}

// Clears the bit at pos in n.
func clearBit(n, pos uint8) uint8 {
	mask := ^(uint8(1) << pos)
	return n & mask
}

// Sets the bit at pos in the integer n.
func setBit(n, pos uint8) uint8 {
	return n | 1<<pos
}

func hasBit(n, pos uint8) bool {
	return n&(1<<pos) > 0
}

// expiredBatchItem is a storage.Item implementation for expired batches.
type expiredBatchItem struct {
	BatchID []byte
}

// ID implements storage.Item.
func (e *expiredBatchItem) ID() string {
	return string(e.BatchID)
}

// Namespace implements storage.Item.
func (e *expiredBatchItem) Namespace() string {
	return "expiredBatchItem"
}

// Marshal implements storage.Item.
// It is a no-op as expiredBatchItem is not serialized.
func (e *expiredBatchItem) Marshal() ([]byte, error) {
	return nil, nil
}

// Unmarshal implements storage.Item.
// It is a no-op as expiredBatchItem is not serialized.
func (e *expiredBatchItem) Unmarshal(_ []byte) error {
	return nil
}

// Clone implements storage.Item.
func (e *expiredBatchItem) Clone() storage.Item {
	if e == nil {
		return nil
	}
	return &expiredBatchItem{
		BatchID: slices.Clone(e.BatchID),
	}
}

// String implements storage.Item.
func (e *expiredBatchItem) String() string {
	return storageutil.JoinFields(e.Namespace(), e.ID())
}

func (db *DB) po(addr swarm.Address) uint8 {
	return swarm.Proximity(db.baseAddr.Bytes(), addr.Bytes())
}
