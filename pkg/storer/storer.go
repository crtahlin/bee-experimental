// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethersphere/bee/v2/pkg/log"
	m "github.com/ethersphere/bee/v2/pkg/metrics"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/pusher"
	"github.com/ethersphere/bee/v2/pkg/retrieval"
	"github.com/ethersphere/bee/v2/pkg/sharky"
	"github.com/ethersphere/bee/v2/pkg/stabilization"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
	"github.com/ethersphere/bee/v2/pkg/storage/migration"
	"github.com/ethersphere/bee/v2/pkg/storage/pebblestore"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/cache"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/events"
	pinstore "github.com/ethersphere/bee/v2/pkg/storer/internal/pinning"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/upload"
	localmigration "github.com/ethersphere/bee/v2/pkg/storer/migration"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	"github.com/ethersphere/bee/v2/pkg/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/afero"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"resenje.org/multex"
)

// PutterSession provides a session around the storage.Putter. The session on
// successful completion commits all the operations or in case of error, rolls back
// the state.
type PutterSession interface {
	storage.Putter
	// Done is used to close the session and optionally assign a swarm.Address to
	// this session.
	Done(swarm.Address) error
	// Cleanup is used to cleanup any state related to this session in case of
	// any error.
	Cleanup() error
}

// SessionInfo is a type which exports the storer tag object. This object
// stores all the relevant information about a particular session.
type SessionInfo = upload.TagItem

// UploadStore is a logical component of the storer which deals with the upload
// of data to swarm.
type UploadStore interface {
	// Upload provides a PutterSession which is tied to the tagID. Optionally if
	// users requests to pin the data, a new pinning collection is created.
	Upload(ctx context.Context, pin bool, tagID uint64) (PutterSession, error)
	// NewSession can be used to obtain a tag ID to use for a new Upload session.
	NewSession() (SessionInfo, error)
	// Session will show the information about the session.
	Session(tagID uint64) (SessionInfo, error)
	// DeleteSession will delete the session info associated with the tag id.
	DeleteSession(tagID uint64) error
	// ListSessions will list all the Sessions currently being tracked.
	ListSessions(offset, limit int) ([]SessionInfo, error)
}

// PinStore is a logical component of the storer which deals with pinning
// functionality.
type PinStore interface {
	// NewCollection can be used to create a new PutterSession which writes a new
	// pinning collection. The address passed in during the Done of the session is
	// used as the root referencce.
	NewCollection(context.Context) (PutterSession, error)
	// DeletePin deletes all the chunks associated with the collection pointed to
	// by the swarm.Address passed in.
	DeletePin(context.Context, swarm.Address) error
	// Pins returns all the root references of pinning collections.
	Pins() ([]swarm.Address, error)
	// HasPin is a helper which checks if a collection exists with the root
	// reference passed in.
	HasPin(swarm.Address) (bool, error)
}

// PinIterator is a helper interface which can be used to iterate over all the
// chunks in a pinning collection.
type PinIterator interface {
	IteratePinCollection(root swarm.Address, iterateFn func(swarm.Address) (bool, error)) error
}

// CacheStore is a logical component of the storer that deals with cache
// content.
type CacheStore interface {
	// Lookup method provides a storage.Getter wrapped around the underlying
	// ChunkStore which will update cache related indexes if required on successful
	// lookups.
	Lookup() storage.Getter
	// Cache method provides a storage.Putter which will add the chunks to cache.
	// This will add the chunk to underlying store as well as new indexes which
	// will keep track of the chunk in the cache.
	Cache() storage.Putter
}

// NetStore is a logical component of the storer that deals with network. It will
// push/retrieve chunks from the network.
type NetStore interface {
	// DirectUpload provides a session which can be used to push chunks directly
	// to the network.
	DirectUpload() PutterSession
	// Download provides a getter which can be used to download data. If the data
	// is found locally, its returned immediately, otherwise it is retrieved from
	// the network.
	Download(cache bool) storage.Getter
	// PusherFeed is the feed for direct push chunks. This can be used by the
	// pusher component to push out the chunks.
	PusherFeed() <-chan *pusher.Op
}

var _ Reserve = (*DB)(nil)

// Reserve is a logical component of the storer that deals with reserve
// content. It will implement all the core functionality required for the protocols.
type Reserve interface {
	ReserveStore
	EvictBatch(ctx context.Context, batchID []byte) error
	ReserveSample(context.Context, []byte, uint8, uint64, *big.Int) (Sample, error)
	WindowedSample(context.Context, []byte, uint8, uint64, *big.Int) (Sample, error)
	ReserveSize() int
}

// ReserveIterator is a helper interface which can be used to iterate over all
// the chunks in the reserve.
type ReserveIterator interface {
	ReserveIterateChunks(cb func(swarm.Chunk) (bool, error)) error
}

// ReserveStore is a logical component of the storer that deals with reserve
// content. It will implement all the core functionality required for the protocols.
type ReserveStore interface {
	ReserveGet(ctx context.Context, addr swarm.Address, batchID []byte, stampHash []byte) (swarm.Chunk, error)
	ReserveHas(addr swarm.Address, batchID []byte, stampHash []byte) (bool, error)
	ReservePutter() storage.Putter
	SubscribeBin(ctx context.Context, bin uint8, start uint64) (<-chan *BinC, func(), <-chan error)
	ReserveLastBinIDs() ([]uint64, uint64, error)
	RadiusChecker
}

// RadiusChecker provides the radius related functionality.
type RadiusChecker interface {
	IsWithinStorageRadius(addr swarm.Address) bool
	StorageRadius() uint8
	CommittedDepth() uint8
	CapacityDoubling() uint8
	// IsSampling reports whether a reserve sample is running, so a caller can
	// hold back work that would contend with it. See issue #23.
	IsSampling() bool
}

// LocalStore is a read-only ChunkStore. It can be used to check if chunk is known
// locally, but it cannot tell what is the context of the chunk (whether it is
// pinned, uploaded, etc.).
type LocalStore interface {
	ChunkStore() storage.ReadOnlyChunkStore
}

// Debugger is a helper interface which can be used to debug the storer.
type Debugger interface {
	DebugInfo(context.Context) (Info, error)
}

type NeighborhoodStats interface {
	NeighborhoodsStat(ctx context.Context) ([]*NeighborhoodStat, error)
}

type memFS struct {
	afero.Fs
}

func (m *memFS) Open(path string) (fs.File, error) {
	return m.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}

type dirFS struct {
	basedir string
}

func (d *dirFS) Open(path string) (fs.File, error) {
	return os.OpenFile(filepath.Join(d.basedir, path), os.O_RDWR|os.O_CREATE, 0o600)
}

var (
	sharkyNoOfShards = 32
	ErrDBQuit        = errors.New("db quit")
)

type closerFn func() error

func (c closerFn) Close() error { return c() }

func closer(closers ...io.Closer) io.Closer {
	return closerFn(func() error {
		var err error
		for _, closer := range closers {
			err = errors.Join(err, closer.Close())
		}
		return err
	})
}

func initInmemRepository() (transaction.Storage, io.Closer, error) {
	store, _, err := leveldbstore.New("", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating inmem levelDB index store: %w", err)
	}

	sharky, err := sharky.New(
		&memFS{Fs: afero.NewMemMapFs()},
		sharkyNoOfShards,
		swarm.SocMaxChunkSize,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating inmem sharky instance: %w", err)
	}

	return transaction.NewStorage(sharky, store), closer(store, sharky), nil
}

// loggerName is the tree path name of the logger for this package.
const loggerName = "storer"

// Default options for levelDB.
const (
	// defaultShutdownTimeout bounds how long Close waits for background
	// workers. Three seconds is the historical value; see Options.ShutdownTimeout
	// for why it is now overridable.
	defaultShutdownTimeout = 3 * time.Second
	// drainTimeout is how long each shutdown drain waits for its workers.
	//
	// This is the window #399 relies on. The store is not closed until both
	// drains have returned OR GIVEN UP, so shortening it closes the store
	// under live reserve work sooner, which is the pebble segfault #399
	// exists to prevent. An earlier draft of #428 shortened it to
	// ShutdownTimeout and would have narrowed the window from five seconds to
	// three without saying so.
	drainTimeout = 5 * time.Second

	defaultOpenFilesLimit     = uint64(256)
	defaultBlockCacheCapacity = uint64(32 * 1024 * 1024)
	// defaultPebbleUsableBlockCache replaces defaultBlockCacheCapacity as the
	// usable Pebble block cache when the option is left at the shared default.
	// Pebble charges memtables against its block cache, so
	// pebbleBlockCacheCapacity adds their reservation on top. See
	// docs/experiments/pebble-block-cache/spec.md (#555).
	defaultPebbleUsableBlockCache = uint64(64 * 1024 * 1024)
	defaultWriteBufferSize        = uint64(32 * 1024 * 1024)
	defaultDisableSeeksCompaction = false

	// The three goleveldb level-0 triggers for the index store, made explicit
	// so they can be configured. These are the values the store has always run
	// with: CompactionL0Trigger was set to 8 in code, and the other two were
	// left at goleveldb's own defaults (8 and 12). goleveldb reads a trigger of
	// 0 as "use my default", so these must be the literal current values rather
	// than 0, or the defaults would silently shift.
	//
	// A node stalls when level 0 reaches the pause trigger and compaction is
	// behind, with no in-process recovery (issue #176). Widening the gap
	// between slowdown and pause gives goleveldb's own per-write slowdown more
	// room to work before the hard stop; that is a slow-disk concern and is why
	// these are exposed, not changed. See docs/agent-playbooks/test-bench.md.
	defaultCompactionL0Trigger  = 8
	defaultWriteSlowdownTrigger = 8
	defaultWritePauseTrigger    = 12
	defaultCacheCapacity        = uint64(1_000_000)
	defaultBgCacheWorkers       = 32
	DefaultReserveCapacity      = 1 << 22 // 4194304 chunks

	indexPath  = "indexstore"
	sharkyPath = "sharky"
)

// DefaultSamplerReadConcurrency is the sampler's chunk-load concurrency when
// none is configured. It matches the worker count the sampler used before
// reading and hashing were separated, so the default changes nothing.
func DefaultSamplerReadConcurrency() int { return max(4, runtime.NumCPU()) }

// Storage-engine names for the index store. Pebble is the recommended engine
// and the default for a fresh data directory (issue #185); goleveldb stays
// supported and selectable, and an existing goleveldb store keeps goleveldb, so
// this binary opens an upstream data directory with no migration.
const (
	EngineLevelDB = "leveldb"
	EnginePebble  = "pebble"
)

func initStore(basePath string, opts *Options) (storage.BatchStore, error) {
	indexDir := path.Join(basePath, indexPath)

	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		err := os.MkdirAll(indexDir, 0o700)
		if err != nil {
			return nil, err
		}
	}

	engine, err := resolveEngine(indexDir, opts.StorageEngine)
	if err != nil {
		return nil, err
	}

	switch engine {
	case EngineLevelDB:
		store, _, err := leveldbstore.New(indexDir, indexStoreOptions(opts))
		if err != nil {
			return nil, fmt.Errorf("failed creating leveldb index store: %w", err)
		}
		return store, nil
	case EnginePebble:
		store, err := pebblestore.New(indexDir, pebbleIndexStoreOptions(opts))
		if err != nil {
			return nil, fmt.Errorf("failed creating pebble index store: %w", err)
		}
		return store, nil
	default:
		return nil, fmt.Errorf("unknown storage engine %q: use %q or %q",
			engine, EngineLevelDB, EnginePebble)
	}
}

// resolveEngine decides which engine to open the index store with, and binds a
// fresh directory to the engine that creates it.
//
// goleveldb and Pebble write mutually unreadable on-disk formats, so the engine
// is a property of the data directory, recorded in a marker file. requested is
// the operator's --storage-engine value, or empty when they did not set one:
//
//   - A directory with a marker uses that engine. An empty request accepts it,
//     so `bee db …` and a restart need no flag; a request that names a different
//     engine is refused, because it cannot be honoured without corruption.
//   - A directory with no marker is fresh (or a goleveldb store from before
//     engine selection). It binds to the requested engine, defaulting to
//     goleveldb, and the marker is written. An unmarked directory that already
//     holds data is goleveldb and may not be bound to Pebble.
//
// See issue #185.
func resolveEngine(indexDir, requested string) (string, error) {
	markerPath := filepath.Join(indexDir, ".storage-engine")

	existing, err := os.ReadFile(markerPath)
	switch {
	case err == nil:
		marker := strings.TrimSpace(string(existing))
		if requested != "" && requested != marker {
			return "", fmt.Errorf("index store at %s was created with the %q engine; "+
				"refusing to open it with %q because the on-disk formats are not "+
				"interchangeable — use a fresh data directory to switch engines",
				indexDir, marker, requested)
		}
		return marker, nil
	case !errors.Is(err, os.ErrNotExist):
		return "", fmt.Errorf("read storage-engine marker: %w", err)
	}

	engine := requested
	if engine == "" {
		// A fresh data directory defaults to pebble, the engine adopted in #185. An
		// existing goleveldb store from before engine markers keeps goleveldb, so an
		// upgrade never fails to open its own data; only a brand new store gets the
		// new default.
		if indexDirHasData(indexDir) {
			engine = EngineLevelDB
		} else {
			engine = EnginePebble
		}
	}
	if engine != EngineLevelDB && indexDirHasData(indexDir) {
		return "", fmt.Errorf("index store at %s holds data but has no engine marker, "+
			"so it is a goleveldb store from before engine selection; refusing to "+
			"open it with %q — use a fresh data directory", indexDir, engine)
	}
	if err := os.WriteFile(markerPath, []byte(engine+"\n"), 0o644); err != nil {
		return "", fmt.Errorf("write storage-engine marker: %w", err)
	}
	return engine, nil
}

// indexDirHasData reports whether the index directory already holds store files,
// ignoring the marker itself.
func indexDirHasData(indexDir string) bool {
	entries, err := os.ReadDir(indexDir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.Name() != ".storage-engine" {
			return true
		}
	}
	return false
}

// indexStoreOptions builds the goleveldb options for the index store from the
// storer Options. Kept separate from initStore so the mapping, including the
// level-0 triggers and goleveldb's "0 means default" quirk, is unit testable
// without opening a real database.
func indexStoreOptions(opts *Options) *opt.Options {
	return &opt.Options{
		OpenFilesCacheCapacity: int(opts.LdbOpenFilesLimit),
		BlockCacheCapacity:     int(opts.LdbBlockCacheCapacity),
		WriteBuffer:            int(opts.LdbWriteBufferSize),
		DisableSeeksCompaction: opts.LdbDisableSeeksCompaction,
		CompactionL0Trigger:    opts.LdbCompactionL0Trigger,
		WriteL0SlowdownTrigger: opts.LdbWriteSlowdownTrigger,
		WriteL0PauseTrigger:    opts.LdbWritePauseTrigger,
		Filter:                 filter.NewBloomFilter(64),
	}
}

// pebbleIndexStoreOptions maps the same db-* tuning knobs onto pebble.Options,
// so an operator's configuration applies whichever engine is selected. The
// mapping is deliberately partial: goleveldb's slowdown trigger has no clean
// analogue in Pebble's stall model, so it is left to Pebble's default rather
// than forced onto a field that means something different. A zero knob keeps the
// engine's own default. See docs/experiments/storage-engine-eval/spec.md.
//
// The block cache is created here and handed to Pebble; its one retained
// reference is released when the process exits, which for a store opened once
// per node lifetime is not worth threading a closer through for.
func pebbleIndexStoreOptions(opts *Options) *pebble.Options {
	o := pebblestore.DefaultOptions()
	if opts.LdbWriteBufferSize > 0 {
		o.MemTableSize = opts.LdbWriteBufferSize
	}
	// Sized after MemTableSize is known: the memtables reserve their size from
	// this cache.
	if c := pebbleBlockCacheCapacity(opts.LdbBlockCacheCapacity, o.MemTableSize, o.MemTableStopWritesThreshold); c > 0 {
		o.Cache = pebble.NewCache(int64(c))
	}
	// Honour an explicit db-compaction-l0-trigger, but do not let its goleveldb
	// oriented default clobber pebble's own shallow-L0 default, which is what makes
	// the reserve-sample read fast (#185). Only an operator value that differs from
	// the shared default overrides it.
	if opts.LdbCompactionL0Trigger > 0 && opts.LdbCompactionL0Trigger != defaultCompactionL0Trigger {
		o.L0CompactionThreshold = opts.LdbCompactionL0Trigger
	}
	if opts.LdbWritePauseTrigger > 0 {
		o.L0StopWritesThreshold = opts.LdbWritePauseTrigger
	}
	if opts.LdbOpenFilesLimit > 0 {
		o.MaxOpenFiles = int(opts.LdbOpenFilesLimit)
	}
	return o
}

// pebbleBlockCacheCapacity returns the Pebble block cache size for a configured
// db-block-cache-capacity, which is the usable cache the operator asks for.
//
// Pebble reserves each memtable's size from the block cache (Cache.Reserve),
// including a memtable being flushed and one kept for recycling, so up to
// (stopWritesThreshold+1) memtables. Without room for that, a grown memtable
// leaves the cache unable to hold the filter blocks a reserve sample needs, and
// the sample slows down several times (#555). The reservation is therefore
// added on top of the usable size.
//
// The shared default is sized for goleveldb, so it is replaced by
// defaultPebbleUsableBlockCache, the same way the L0 trigger's shared default
// is. Any other operator value is used as given; 0 leaves Pebble's own cache.
func pebbleBlockCacheCapacity(configured, memTableSize uint64, stopWritesThreshold int) uint64 {
	if configured == 0 {
		return 0
	}
	usable := configured
	if configured == defaultBlockCacheCapacity {
		usable = defaultPebbleUsableBlockCache
	}
	return usable + uint64(stopWritesThreshold+1)*memTableSize
}

func initDiskRepository(
	ctx context.Context,
	basePath string,
	opts *Options,
) (transaction.Storage, *PinIntegrity, io.Closer, int, error) {
	store, err := initStore(basePath, opts)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("failed creating levelDB index store: %w", err)
	}

	err = migration.Migrate(store, "core-migration", localmigration.BeforeInitSteps(store, opts.Logger))
	if err != nil {
		return nil, nil, nil, 0, errors.Join(store.Close(), fmt.Errorf("failed core migration: %w", err))
	}

	if opts.LdbStats.Load() != nil {
		go func() {
			ldbStats := opts.LdbStats.Load()
			logger := log.NewLogger(loggerName).Register()
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()

			// Whether the store was paused at the previous tick, so the state
			// is logged once on each edge rather than every tick. A write pause
			// is a node-down condition: goleveldb has stopped accepting writes
			// because level 0 has reached its pause trigger and compaction is
			// behind, and it does not recover on its own (see issue #176). The
			// only prior signal was a Prometheus gauge, which helps nobody who
			// is not already scraping and alerting on it.
			wasPaused := false

			// The write-stall log line is engine-neutral: it reads level-0 file
			// count through a small interface both stores satisfy, and treats
			// level 0 at or above the pause trigger as stalled. The richer
			// per-level histogram below only goleveldb provides, so it is gated
			// on the concrete leveldb handle. See issue #185.
			level0Reader, _ := store.(interface{ Level0Files() int })
			leveldbHandle, isLeveldb := store.(interface{ DB() *leveldb.DB })

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if level0Reader != nil {
						level0 := level0Reader.Level0Files()
						stalled := opts.LdbWritePauseTrigger > 0 && level0 >= opts.LdbWritePauseTrigger
						switch writePauseEdge(wasPaused, stalled) {
						case writePauseEntered:
							logger.Warning("index store has stopped accepting writes: "+
								"level 0 has too many files and compaction is behind; "+
								"the node will not make progress until compaction catches up",
								"level_0_files", level0,
								"pause_trigger", opts.LdbWritePauseTrigger)
						case writePauseLeft:
							logger.Warning("index store is accepting writes again")
						}
						wasPaused = stalled
					}

					if !isLeveldb {
						continue
					}
					stats := new(leveldb.DBStats)
					switch err := leveldbHandle.DB().Stats(stats); {
					case errors.Is(err, leveldb.ErrClosed):
						return
					case err != nil:
						logger.Error(err, "snapshot levelDB stats")
					default:
						ldbStats.WithLabelValues("write_delay_count").Observe(float64(stats.WriteDelayCount))
						ldbStats.WithLabelValues("write_delay_duration").Observe(stats.WriteDelayDuration.Seconds())
						ldbStats.WithLabelValues("alive_snapshots").Observe(float64(stats.AliveSnapshots))
						ldbStats.WithLabelValues("alive_iterators").Observe(float64(stats.AliveIterators))
						ldbStats.WithLabelValues("io_write").Observe(float64(stats.IOWrite))
						ldbStats.WithLabelValues("io_read").Observe(float64(stats.IORead))
						ldbStats.WithLabelValues("block_cache_size").Observe(float64(stats.BlockCacheSize))
						ldbStats.WithLabelValues("opened_tables_count").Observe(float64(stats.OpenedTablesCount))
						ldbStats.WithLabelValues("mem_comp").Observe(float64(stats.MemComp))
						ldbStats.WithLabelValues("level_0_comp").Observe(float64(stats.Level0Comp))
						ldbStats.WithLabelValues("non_level_0_comp").Observe(float64(stats.NonLevel0Comp))
						ldbStats.WithLabelValues("seek_comp").Observe(float64(stats.SeekComp))
						for i := 0; i < len(stats.LevelSizes); i++ {
							ldbStats.WithLabelValues(fmt.Sprintf("level_%d_size", i)).Observe(float64(stats.LevelSizes[i]))
							ldbStats.WithLabelValues(fmt.Sprintf("level_%d_tables_count", i)).Observe(float64(stats.LevelTablesCounts[i]))
							ldbStats.WithLabelValues(fmt.Sprintf("level_%d_read", i)).Observe(float64(stats.LevelRead[i]))
							ldbStats.WithLabelValues(fmt.Sprintf("level_%d_write", i)).Observe(float64(stats.LevelWrite[i]))
							ldbStats.WithLabelValues(fmt.Sprintf("level_%d_duration", i)).Observe(stats.LevelDurations[i].Seconds())
						}
					}
				}
			}
		}()
	}

	sharkyBasePath := path.Join(basePath, sharkyPath)

	if _, err := os.Stat(sharkyBasePath); os.IsNotExist(err) {
		err := os.Mkdir(sharkyBasePath, 0o700)
		if err != nil {
			return nil, nil, nil, 0, err
		}
	}

	recoveryCloser, pruned, err := sharkyRecovery(ctx, sharkyBasePath, store, opts)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("failed to recover sharky: %w", err)
	}

	sharky, err := sharky.New(
		&dirFS{basedir: sharkyBasePath},
		sharkyNoOfShards,
		swarm.SocMaxChunkSize,
	)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("failed creating sharky instance: %w", err)
	}

	pinIntegrity := &PinIntegrity{
		Store:  store,
		Sharky: sharky,
	}

	return transaction.NewStorage(sharky, store), pinIntegrity, closer(store, sharky, recoveryCloser), pruned, nil
}

const lockKeyNewSession string = "new_session"

// Options provides a container to configure different things in the storer.
type Options struct {
	// These are options related to levelDB. Currently, the underlying storage used is levelDB.
	LdbStats atomic.Pointer[prometheus.HistogramVec]
	// StorageEngine selects the index-store engine: "pebble" (default) or
	// "leveldb". Empty defaults a fresh data directory to pebble; an existing
	// store keeps the engine it was created with. See issue #185.
	StorageEngine             string
	LdbOpenFilesLimit         uint64
	LdbBlockCacheCapacity     uint64
	LdbWriteBufferSize        uint64
	LdbDisableSeeksCompaction bool
	// The three goleveldb level-0 triggers for the index store, in files. A
	// value of 0 means goleveldb's own default, which is not the same as the
	// wasp default; see the default* constants and issue #176.
	LdbCompactionL0Trigger  int
	LdbWriteSlowdownTrigger int
	LdbWritePauseTrigger    int
	Logger                  log.Logger
	Tracer                  *tracing.Tracer

	Address           swarm.Address
	StartupStabilizer stabilization.Subscriber
	Batchstore        postage.Storer
	ValidStamp        postage.ValidStampFn
	RadiusSetter      topology.SetStorageRadiuser
	StateStore        storage.StateStorer

	// LocalIngestLimit is the most chunks this node will hold from local
	// ingests (issue #326). Zero means no limit, and nothing then stops an
	// ingest filling the disk; the node warns at startup when the feature is
	// enabled with no limit.
	LocalIngestLimit uint64

	ReserveCapacity       int
	ReserveWakeUpDuration time.Duration
	// ReserveBatchSweepInterval is how often the reserve reconciles chunks
	// against the batches they claim, evicting any whose batch has vanished.
	// Zero runs it on every reserve wake-up, which is the previous behaviour.
	// A longer interval keeps the frequent wake-up scan to the cheap
	// within-radius count. See issue #28.
	ReserveBatchSweepInterval time.Duration
	// SamplerReadConcurrency is how many chunk loads the reserve sampler keeps
	// in flight. Zero means the default, which preserves the previous behaviour
	// of one pool sized to the core count.
	SamplerReadConcurrency int
	// SamplerSortWindow is how many chunks the reserve sampler buffers and
	// sorts by physical position before reading them. Zero disables ordering
	// and issues the reads in bin order, which is the previous behaviour.
	SamplerSortWindow int
	// ReserveHasConcurrency bounds how many ReserveHas lookups may run at
	// once. Zero leaves them unbounded, which is the previous behaviour.
	ReserveHasConcurrency int
	// ReserveEvictionRate is the most chunks per second reserve eviction
	// deletes, raised to the rate at which chunks arrive when that is
	// higher. Zero means no limit and runtime.NumCPU() goroutines, the
	// previous behaviour apart from eviction working in rounds (#623).
	ReserveEvictionRate     int
	ReserveMinEvictCount    uint64
	ReserveCapacityDoubling int

	CacheCapacity      uint64
	CacheMinEvictCount uint64

	MinimumStorageRadius uint

	// ShutdownTimeout bounds how long Close waits for background cache and
	// reserve workers to finish before giving up and reporting an error.
	// Zero means defaultShutdownTimeout.
	//
	// It is configurable because the right value depends on how much work is
	// in flight: a node with a large reserve, or one on slow storage, can
	// legitimately need longer than a small one, and reporting a shutdown
	// error in that case is a false alarm rather than a leak.
	ShutdownTimeout time.Duration
}

func defaultOptions() *Options {
	return &Options{
		StorageEngine:             "",
		LdbOpenFilesLimit:         defaultOpenFilesLimit,
		LdbBlockCacheCapacity:     defaultBlockCacheCapacity,
		LdbWriteBufferSize:        defaultWriteBufferSize,
		LdbDisableSeeksCompaction: defaultDisableSeeksCompaction,
		LdbCompactionL0Trigger:    defaultCompactionL0Trigger,
		LdbWriteSlowdownTrigger:   defaultWriteSlowdownTrigger,
		LdbWritePauseTrigger:      defaultWritePauseTrigger,
		CacheCapacity:             defaultCacheCapacity,
		Logger:                    log.Noop,
		ReserveCapacity:           DefaultReserveCapacity,
		SamplerReadConcurrency:    DefaultSamplerReadConcurrency(),
		ReserveWakeUpDuration:     time.Minute * 30,
		ShutdownTimeout:           defaultShutdownTimeout,
	}
}

// cacheLimiter is used to limit the number
// of concurrent cache background workers.
type cacheLimiter struct {
	wg     sync.WaitGroup
	sem    chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

// DB implements all the component stores described above.
type DB struct {
	logger log.Logger
	tracer *tracing.Tracer

	metrics             metrics
	storage             transaction.Storage
	multex              *multex.Multex
	cacheObj            *cache.Cache
	retrieval           retrieval.Interface
	pusherFeed          chan *pusher.Op
	quit                chan struct{}
	quitOnce            sync.Once
	cacheLimiter        cacheLimiter
	dbCloser            io.Closer
	subscriptionsWG     sync.WaitGroup
	events              *events.Subscriber
	directUploadLimiter chan struct{}

	// localIngest is the node-wide accounting for content stored by
	// POST /wasp/ingest, which pays no postage and so has no rent to bound
	// it. See issue #326 and pkg/storer/localingest.go.
	localIngest localIngestState

	reserve          *reserve.Reserve
	inFlight         sync.WaitGroup
	reserveBinEvents *events.Subscriber
	baseAddr         swarm.Address
	batchstore       postage.Storer
	validStamp       postage.ValidStampFn
	setSyncerOnce    sync.Once
	syncer           Syncer
	reserveOptions   reserveOpts
	shutdownTimeout  time.Duration
	// drainWindow is how long each shutdown drain waits. Zero means
	// drainTimeout. A field rather than a bare constant so a test can
	// shorten it per store, instead of mutating package state that
	// parallel tests would race. See #428.
	drainWindow time.Duration
	// reserveHasLimiter bounds concurrent ReserveHas lookups. Nil when
	// unbounded, which is the default.
	reserveHasLimiter chan struct{}
	// evictionPacer paces reserve eviction. Nil when reserve-eviction-rate
	// is 0, which is the default (#623).
	evictionPacer *evictionPacer

	// samplingInProgress is set while ReserveSample runs, so the puller can
	// pause pulling and leave the store quiet for the sample. See issue #23.
	samplingInProgress atomic.Bool

	pinIntegrity *PinIntegrity
}

type reserveOpts struct {
	startupStabilizer  stabilization.Subscriber
	wakeupDuration     time.Duration
	batchSweepInterval time.Duration
	minEvictCount      uint64
	cacheMinEvictCount uint64
	minimumRadius      uint8
	capacityDoubling   int
	// samplerReadConcurrency is how many chunk loads the sampler has in flight.
	// Separate from the hasher count because loading is disk-bound and hashing
	// is CPU-bound; see issue #9.
	samplerReadConcurrency int
	// samplerSortWindow is how many chunks the sampler buffers and sorts into
	// disk order before reading them. Zero issues the reads in bin order, which
	// is what the sampler did before ordering existed; see issue #11.
	samplerSortWindow int
}

// reserveHasSlots returns the semaphore bounding concurrent ReserveHas
// lookups, or nil when they are unbounded.
//
// The bound exists because pullsync calls ReserveHas once per offered chunk,
// from one goroutine per syncing peer, so the concurrency the reserve sees is
// the peer count. On a node with a large reserve and a hundred peers that is a
// hundred concurrent index lookups competing with everything else the store is
// doing. See issue #20.
func reserveHasSlots(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

// acquireSlot takes a slot from limiter, returning a function that gives it
// back. A nil limiter is unbounded and returns immediately.
//
// It abandons the wait when quit closes. ReserveHas has no context to cancel,
// so without that escape a node whose reserve had stopped answering could not
// be shut down: every waiting caller would block on a slot that never frees.
func acquireSlot(limiter chan struct{}, quit chan struct{}) (func(), error) {
	if limiter == nil {
		return func() {}, nil
	}
	select {
	case limiter <- struct{}{}:
		return func() { <-limiter }, nil
	case <-quit:
		return nil, ErrDBQuit
	}
}

// New returns a newly constructed DB object which implements all the above
// component stores.
// writePauseEdge classifies the change in the index store's write-pause state
// between two consecutive observations, so the state is logged once on each
// edge rather than on every tick it stays the same.
type writePauseChange int

const (
	writePauseUnchanged writePauseChange = iota
	writePauseEntered
	writePauseLeft
)

func writePauseEdge(prev, cur bool) writePauseChange {
	switch {
	case cur && !prev:
		return writePauseEntered
	case !cur && prev:
		return writePauseLeft
	default:
		return writePauseUnchanged
	}
}

func New(ctx context.Context, dirPath string, opts *Options) (*DB, error) {
	var (
		err          error
		pinIntegrity *PinIntegrity
		st           transaction.Storage
		dbCloser     io.Closer
		pruned       int
	)
	if opts == nil {
		opts = defaultOptions()
	}

	if opts.Logger == nil {
		opts.Logger = log.Noop
	}

	lock := multex.New()
	metrics := newMetrics()
	opts.LdbStats.CompareAndSwap(nil, metrics.LevelDBStats)

	if dirPath == "" {
		st, dbCloser, err = initInmemRepository()
		if err != nil {
			return nil, err
		}
	} else {
		st, pinIntegrity, dbCloser, pruned, err = initDiskRepository(ctx, dirPath, opts)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if err != nil && dbCloser != nil {
			err = errors.Join(err, dbCloser.Close())
		}
	}()

	sharkyBasePath := ""
	if dirPath != "" {
		sharkyBasePath = path.Join(dirPath, sharkyPath)
	}

	err = st.Run(ctx, func(s transaction.Store) error {
		return migration.Migrate(
			s.IndexStore(),
			"migration",
			localmigration.AfterInitSteps(sharkyBasePath, sharkyNoOfShards, st, opts.Logger),
		)
	})
	if err != nil {
		return nil, fmt.Errorf("failed regular migration: %w", err)
	}

	cacheObj, err := cache.New(ctx, st.IndexStore(), opts.CacheCapacity)
	if err != nil {
		return nil, err
	}

	logger := opts.Logger.WithName(loggerName).Register()

	clCtx, clCancel := context.WithCancel(ctx)
	db := &DB{
		metrics:  metrics,
		storage:  st,
		logger:   logger,
		tracer:   opts.Tracer,
		baseAddr: opts.Address,
		shutdownTimeout: func() time.Duration {
			if opts.ShutdownTimeout > 0 {
				return opts.ShutdownTimeout
			}
			return defaultShutdownTimeout
		}(),
		multex:     lock,
		cacheObj:   cacheObj,
		retrieval:  noopRetrieval{},
		pusherFeed: make(chan *pusher.Op),
		quit:       make(chan struct{}),
		cacheLimiter: cacheLimiter{
			sem:    make(chan struct{}, defaultBgCacheWorkers),
			ctx:    clCtx,
			cancel: clCancel,
		},
		dbCloser:         dbCloser,
		batchstore:       opts.Batchstore,
		validStamp:       opts.ValidStamp,
		events:           events.NewSubscriber(),
		reserveBinEvents: events.NewSubscriber(),
		reserveOptions: reserveOpts{
			startupStabilizer:      opts.StartupStabilizer,
			wakeupDuration:         opts.ReserveWakeUpDuration,
			batchSweepInterval:     opts.ReserveBatchSweepInterval,
			minEvictCount:          opts.ReserveMinEvictCount,
			cacheMinEvictCount:     opts.CacheMinEvictCount,
			minimumRadius:          uint8(opts.MinimumStorageRadius),
			capacityDoubling:       opts.ReserveCapacityDoubling,
			samplerReadConcurrency: opts.SamplerReadConcurrency,
			samplerSortWindow:      opts.SamplerSortWindow,
		},
		directUploadLimiter: make(chan struct{}, pusher.ConcurrentPushes),
		reserveHasLimiter:   reserveHasSlots(opts.ReserveHasConcurrency),
		pinIntegrity:        pinIntegrity,
	}

	if db.validStamp == nil {
		db.validStamp = postage.ValidStamp(db.batchstore)
	}

	if opts.ReserveCapacity > 0 {
		rs, err := reserve.New(
			opts.Address,
			st,
			opts.ReserveCapacity,
			opts.RadiusSetter,
			logger,
		)
		if err != nil {
			return nil, err
		}
		db.reserve = rs

		if opts.ReserveEvictionRate < 0 {
			return nil, fmt.Errorf("reserve eviction rate %d: must be 0 or more", opts.ReserveEvictionRate)
		}
		if p := newEvictionPacer(opts.ReserveEvictionRate, rs.Arrivals); p != nil {
			p.setWorkers = func(n int) { db.metrics.ReserveEvictionWorkers.Set(float64(n)) }
			p.setArrivalRate = db.metrics.ReserveArrivalRate.Set
			db.metrics.ReserveEvictionWorkers.Set(float64(p.Workers()))
			db.evictionPacer = p
		}

		db.metrics.StorageRadius.Set(float64(rs.Radius()))
		db.metrics.ReserveSize.Set(float64(rs.Size()))
	}
	db.metrics.CacheSize.Set(float64(db.cacheObj.Size()))
	if pruned > 0 {
		db.metrics.RecoveryPrunedChunkCount.Add(float64(pruned))
	}

	// Cleanup any dirty state in upload and pinning stores, this could happen
	// in case of dirty shutdowns
	err = errors.Join(
		upload.CleanupDirty(db.storage),
		pinstore.CleanupDirty(db.storage),
	)
	if err != nil {
		return nil, err
	}

	// Rebuild the local ingest usage total from the index (issue #326). This
	// completes before the API is built and before the listener opens, so the
	// route never serves against an unbuilt total. It reports its own
	// failures and does not stop the node; see the function.
	db.localIngest.limit = opts.LocalIngestLimit
	db.localIngest.gauge = db.metrics.LocalIngestChunks
	db.rebuildLocalIngestTotal(ctx)

	db.inFlight.Add(1)
	go db.cacheWorker(ctx)

	return db, nil
}

// Reset removes all entries
func (db *DB) ResetReserve(ctx context.Context) error {
	return db.reserve.Reset(ctx)
}

// Metrics returns set of prometheus collectors.
func (db *DB) Metrics() []prometheus.Collector {
	collectors := m.PrometheusCollectorsFromFields(db.metrics)
	if v, ok := db.storage.(m.Collector); ok {
		collectors = append(collectors, v.Metrics()...)
	}
	return collectors
}

// StatusMetrics exposes metrics that are exposed on the status protocol.
func (db *DB) StatusMetrics() []prometheus.Collector {
	collectors := []prometheus.Collector{
		db.metrics.MethodCallsDuration,
	}

	type Collector interface {
		StatusMetrics() []prometheus.Collector
	}

	if v, ok := db.storage.(Collector); ok {
		collectors = append(collectors, v.StatusMetrics()...)
	}

	return collectors
}

func (db *DB) Close() error {
	// Idempotent: TriggerQuit (tests) and a double Close share this guard.
	db.quitOnce.Do(func() { close(db.quit) })

	// wasp #428: Close used to give up on ShutdownTimeout, three seconds by
	// default, while each drain waited five. A drain between the two made
	// Close return while dbCloser.Close had not run, so the store was left
	// open and the next start replayed the write-ahead log.
	//
	// The fix is that Close WAITS for the close, not that the drains are
	// shortened. Shortening them to ShutdownTimeout was the first draft and it
	// silently narrowed the #399 window from five seconds to three; the drain
	// keeps its own bound, and ShutdownTimeout may only extend it.
	drain := db.drainWindow
	if drain <= 0 {
		drain = drainTimeout
	}
	if db.shutdownTimeout > drain {
		drain = db.shutdownTimeout
	}

	reserveDrained := make(chan bool, 1)
	go func() {
		reserveDrained <- syncutil.WaitWithTimeout(&db.inFlight, drain)
	}()

	cacheDrained := make(chan bool, 1)
	go func() {
		cacheDrained <- syncutil.WaitWithTimeout(&db.cacheLimiter.wg, drain)
	}()

	// Both waits are bounded by the same window, so this returns within it.
	reserveOK, cacheOK := <-reserveDrained, <-cacheDrained
	if !reserveOK {
		db.logger.Warning("db shutting down with running goroutines")
	}
	if !cacheOK {
		db.logger.Warning("cache goroutines still running after the wait timeout; force closing")
		db.cacheLimiter.cancel()
	}

	// Close the underlying store only after the workers that use it have
	// stopped. Closing it while a reserve scan is still iterating pulls the
	// store out from under a live iterator, which segfaults under pebble.
	// See issue #399.
	//
	// A drain that timed out still reaches here, exactly as before: the wait
	// reports failure but returns either way. What changed is that Close now
	// waits for the close rather than returning while it happens.
	// The store's own Close is NOT given a timer. Bounding it and returning
	// early is the defect this issue is about, just with a different number:
	// an earlier draft used five seconds and CI found a real store on Windows
	// takes longer than that, so Close reported the store as unclosed while it
	// was still closing and the test could not delete its files.
	//
	// The drains above are bounded, so this is the last step and nothing is
	// racing it. A pathological store close blocks shutdown, which is visible,
	// and cmd/bee still exits on a second interrupt. That is a better failure
	// than reporting either success or failure while the store is open.
	err := db.dbCloser.Close()

	if !reserveOK || !cacheOK {
		return errors.Join(
			fmt.Errorf("storer closed, but bg goroutines were still running after %s", drain),
			err,
		)
	}

	return err
}

func (db *DB) SetRetrievalService(r retrieval.Interface) {
	db.retrieval = r
}

// StartReserveWorker starts the reserve worker. It takes an optional ready channel that is closed whenever the reserve
// worker has finished starting, as this synchronization is needed for some tests. The channel is not used for writing anywhere.
func (db *DB) StartReserveWorker(ctx context.Context, s Syncer, radius func() (uint8, error), ready chan<- struct{}) {
	db.setSyncerOnce.Do(func() {
		db.syncer = s
		go db.startReserveWorkers(ctx, radius, ready)
	})
}

type noopRetrieval struct{}

func (noopRetrieval) RetrieveChunk(_ context.Context, _ swarm.Address, _ swarm.Address) (swarm.Chunk, error) {
	return nil, storage.ErrNotFound
}

func (db *DB) ChunkStore() storage.ReadOnlyChunkStore {
	return db.storage.ChunkStore()
}

func (db *DB) PinIntegrity() *PinIntegrity {
	return db.pinIntegrity
}

func (db *DB) Lock(strs ...string) func() {
	for _, s := range strs {
		db.multex.Lock(s)
	}
	return func() {
		for _, s := range strs {
			db.multex.Unlock(s)
		}
	}
}

func (db *DB) Storage() transaction.Storage {
	return db.storage
}

type putterSession struct {
	storage.Putter
	done    func(swarm.Address) error
	cleanup func() error
}

func (p *putterSession) Done(addr swarm.Address) error { return p.done(addr) }

func (p *putterSession) Cleanup() error { return p.cleanup() }
