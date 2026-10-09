// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/bmt"
	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/safe"
	"github.com/ethersphere/bee/v2/pkg/sharky"
	"github.com/ethersphere/bee/v2/pkg/soc"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/chunkstamp"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/chunkstore"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/reserve"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/sync/errgroup"
)

const SampleSize = 16

type SampleItem struct {
	TransformedAddress swarm.Address
	ChunkAddress       swarm.Address
	ChunkData          []byte
	Stamp              *postage.Stamp
}

type Sample struct {
	Stats SampleStats
	Items []SampleItem
}

// ReserveSample generates the sample of reserve storage of a node required for the
// storage incentives agent to participate in the lottery round. In order to generate
// this sample we need to iterate through all the chunks in the node's reserve and
// calculate the transformed hashes of all the chunks using the anchor as the salt.
// In order to generate the transformed hashes, we will use the std hmac keyed-hash
// implementation by using the anchor as the key. Nodes need to calculate the sample
// in the most optimal way and there are time restrictions. The lottery round is a
// time based round, so nodes participating in the round need to perform this
// calculation within the round limits.
// In order to optimize this we use a simple pipeline pattern:
// Iterate chunk addresses -> Get the chunk data and calculate transformed hash -> Assemble the sample
// If the node has doubled their capacity by some factor, sampling process need to only pertain to the
// chunks of the selected neighborhood as determined by the anchor and the "committed depth" and NOT the whole reserve.
// The committed depth is the sum of the radius and the doubling factor.
// For example, the committed depth is 11, but the local node has a doubling factor of 3, so the
// local radius will eventually drop to 8. The sampling must only consider chunks with proximity 11 to the anchor.
// maxLoadedChunkBuffer caps how many loaded chunks may sit between the reading
// and hashing stages. 4096 chunks is roughly 16 MiB, which is ample for any
// reader count worth configuring and stops an extreme one from turning into
// memory pressure during a redistribution round.
const maxLoadedChunkBuffer = 4096

// loadedChunk carries a chunk from the reading stage to the hashing stage,
// keeping the bin item alongside it because the hasher needs its type and batch.
type loadedChunk struct {
	item  *reserve.ChunkBinItem
	chunk swarm.Chunk
}

// maxSamplerSortWindow caps how many bin items the ordering stage may hold
// while it sorts them. Each entry is an item pointer plus a location, so a
// million of them is tens of megabytes; the cap exists so a mistyped setting
// cannot turn into memory pressure during a redistribution round.
const maxSamplerSortWindow = 1 << 20

// locatedItem pairs a bin item with where its chunk data physically sits, so a
// window of them can be sorted into disk order before being read.
//
// located records whether the lookup succeeded. An item whose location could
// not be read is still passed on rather than dropped, because the ordering
// stage must not change which chunks the sampler sees.
type locatedItem struct {
	item    *reserve.ChunkBinItem
	loc     sharky.Location
	located bool
}

// orderSampleReads buffers windows of bin items, sorts each window by where
// its chunk data physically sits, and emits it in that order.
//
// Items whose location cannot be read are kept and emitted after the sorted
// ones. Dropping them would make the ordering stage change which chunks the
// sampler considers, which is exactly what an ordering stage must not do — the
// read that follows will fail on its own and be counted there, as it is when
// this stage is switched off.
func (db *DB) orderSampleReads(
	ctx context.Context,
	g *errgroup.Group,
	in <-chan *reserve.ChunkBinItem,
	window int,
	readers int,
	addStats func(SampleStats),
) <-chan *reserve.ChunkBinItem {
	out := make(chan *reserve.ChunkBinItem, 3*readers)

	g.Go(func() error {
		// Reported from inside the stage rather than from the caller, so that
		// a window which never reached the stage reads as 0 and a test can
		// tell the difference between configured and used.
		stats := SampleStats{SortWindow: window}
		defer func() {
			close(out)
			addStats(stats)
		}()

		idx := db.storage.IndexStore()
		buf := make([]locatedItem, 0, window)

		flush := func() error {
			sortStart := time.Now()
			// Stable, so that items which could not be located keep their
			// arrival order rather than being shuffled by an arbitrary
			// comparison among equals.
			sort.SliceStable(buf, func(i, j int) bool {
				a, b := buf[i], buf[j]
				if a.located != b.located {
					return a.located
				}
				if !a.located {
					return false
				}
				if a.loc.Shard != b.loc.Shard {
					return a.loc.Shard < b.loc.Shard
				}
				return a.loc.Slot < b.loc.Slot
			})
			stats.SortDuration += time.Since(sortStart)

			for _, li := range buf {
				select {
				case out <- li.item:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			buf = buf[:0]
			return nil
		}

		for item := range in {
			rIdx := &chunkstore.RetrievalIndexItem{Address: item.Address}
			lookupStart := time.Now()
			err := idx.Get(rIdx)
			stats.LocateDuration += time.Since(lookupStart)

			li := locatedItem{item: item}
			if err != nil {
				stats.LocateFailed++
				db.logger.Debug("failed locating chunk", "chunk_address", item.Address, "error", err)
			} else {
				li.loc = rIdx.Location
				li.located = true
			}
			buf = append(buf, li)

			if len(buf) >= window {
				if err := flush(); err != nil {
					return err
				}
			}
		}

		return flush()
	})

	return out
}

func (db *DB) reserveSample(
	ctx context.Context,
	anchor []byte,
	committedDepth uint8,
	consensusTime uint64,
	minBatchBalance *big.Int,
	inWindow func([]byte) bool,
) (Sample, error) {
	// Signal that a sample is running so the puller pauses pulling and
	// eviction pauses, leaving the store quiet. Cleared on every return,
	// including error and cancellation, so a failed sample never leaves
	// pulling or eviction paused. See issues #23 and #649. sampleStarted
	// also waits for an eviction round or radius step in progress, so the
	// radius read below is the one the whole sample sees.
	db.sampleStarted()
	defer db.sampleDone()

	g, ctx := errgroup.WithContext(ctx)

	allStats := &SampleStats{}
	statsLock := sync.Mutex{}
	addStats := func(stats SampleStats) {
		statsLock.Lock()
		allStats.add(stats)
		statsLock.Unlock()
	}

	workers := max(4, runtime.NumCPU())
	t := time.Now()

	defer func() {
		duration := time.Since(t)
		err := g.Wait()
		db.recordReserveSampleMetrics(duration, allStats, workers, err)
	}()

	excludedBatchIDs, err := db.batchesBelowValue(minBatchBalance)
	if err != nil {
		db.logger.Error(err, "get batches below value")
	}

	allStats.BatchesBelowValueDuration = time.Since(t)

	chunkC := make(chan *reserve.ChunkBinItem, 3*workers)

	// Phase 1: Iterate chunk addresses
	g.Go(safe.RunFunc(db.logger, "storer-sample-iterate-chunks", func() error {
		start := time.Now()
		stats := SampleStats{}
		defer func() {
			stats.IterationDuration = time.Since(start)
			close(chunkC)
			addStats(stats)
		}()

		// Walk only the bins that can hold the anchor's neighbourhood. On a
		// node with reserve-capacity-doubling d the reserve holds 2^d
		// neighbourhoods, and walking all of them to sample one cost 8x the
		// index reads at d=3. When the anchor is far from the overlay the
		// range is still one bin holding several neighbourhoods, up to 2^(d-1)
		// of them in bin d below the committed depth. See issue #568.
		lo, hi, ok := sampleBins(db.baseAddr.Bytes(), anchor, committedDepth, db.StorageRadius())
		if !ok {
			return nil
		}

		err := db.reserve.IterateChunksItems(lo, func(ch *reserve.ChunkBinItem) (bool, error) {
			// Keys are ordered by bin first, so nothing after this is in range.
			if ch.Bin > hi {
				return true, nil
			}
			stats.ChunkBinScanned++

			// Required when p < committedDepth: the range is then bin p, which
			// also holds the other neighbourhoods that share p bits with the
			// overlay, and this drops them. When p >= committedDepth every
			// entry in range passes. It cannot recover chunks a range that is
			// too narrow would miss; the tests guard that side.
			if swarm.Proximity(ch.Address.Bytes(), anchor) < committedDepth {
				return false, nil
			}
			// Windowed proof (#273): when a window predicate is set, keep only
			// the chunks inside the anchor-derived sub-window. Placed before the
			// count below so TotalIterated stays the honest cost measure, the
			// number of chunks actually read. A nil predicate is classic mode,
			// the whole neighbourhood.
			if inWindow != nil && !inWindow(ch.Address.Bytes()) {
				return false, nil
			}
			// Counted before the filters below, so that TotalIterated keeps
			// meaning "chunks in the neighbourhood" rather than silently
			// becoming "chunks in the neighbourhood that were also eligible"
			// now that the filters run here instead of in the reader pool.
			stats.TotalIterated++

			// Decided here rather than in the reader pool, because neither
			// decision depends on anything a later stage learns. Deciding late
			// meant every stage after this one paid for chunks that were always
			// going to be discarded — since #11 that includes an index lookup
			// per chunk and a slot in the sort window.
			if _, found := excludedBatchIDs[string(ch.BatchID)]; found {
				stats.BelowBalanceIgnored++
				return false, nil
			}
			if ch.ChunkType != swarm.ChunkTypeSingleOwner &&
				ch.ChunkType != swarm.ChunkTypeContentAddressed {
				stats.RogueChunk++
				return false, nil
			}

			select {
			case chunkC <- ch:
				return false, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		})
		return err
	}))

	readers := db.reserveOptions.samplerReadConcurrency
	if readers <= 0 {
		readers = workers
	}

	// Phase 1b: order the reads by physical position.
	//
	// Bin order comes from the chunk address, which is a hash, so it says
	// nothing about where the data sits in the sharky shard files. Every read
	// is therefore a seek to an unrelated position, and raising the reader
	// count does not help: more concurrent random reads is still random reads.
	//
	// This stage buffers a window of items, reads each one's location from
	// retrievalIdx, sorts the window by (shard, slot), and emits it in that
	// order. The location is used only as a sort key and then discarded — the
	// readers still go through ChunkStore().Get, which repeats the lookup.
	//
	// The repeat is deliberate. Get holds a per-address lock across the lookup
	// and the read; splitting them would open a window as wide as this one, in
	// which the chunk can be evicted and its slot reused by an unrelated Put.
	// The read would then return another chunk's bytes labelled with the
	// address that was asked for, and the sampler would hash those. The second
	// lookup is for a key read moments earlier, so it is served from cache
	// rather than the device. See docs/experiments/sampler-read-ordering/spec.md.
	readC := (<-chan *reserve.ChunkBinItem)(chunkC)
	sortWindow := db.reserveOptions.samplerSortWindow
	if sortWindow > maxSamplerSortWindow {
		db.logger.Warning("reserve sampler sort window capped",
			"requested", sortWindow, "used", maxSamplerSortWindow)
		sortWindow = maxSamplerSortWindow
	}
	if sortWindow > 0 {
		readC = db.orderSampleReads(ctx, g, chunkC, sortWindow, readers, addStats)
	}

	// Phase 2: load chunk data, then hash it.
	//
	// These are separated because they are limited by different things. Loading
	// is disk-bound and spends most of its time blocked; hashing is CPU-bound.
	// Running both in one pool means every worker holds a core while waiting on
	// the disk, so the concurrency offered to the storage layer is pinned to the
	// core count whether or not that is the right number. See issue #9 and
	// docs/experiments/sampler-io-split/spec.md.
	//
	// Sample selection takes the smallest transformed addresses and does not
	// depend on the order items arrive in, so decoupling the stages is safe.
	sampleItemChan := make(chan SampleItem, 3*workers)
	// Bound the hand-off buffer. Unlike chunkC, which carries bin items, this
	// carries loaded chunk data, so its depth is memory an operator can set by
	// raising the reader count: at 3 per reader and ~4 KiB a chunk, a careless
	// value costs hundreds of megabytes. The cap is generous enough that it
	// never binds at sane settings.
	loadedBuf := min(3*readers, maxLoadedChunkBuffer)
	loadedC := make(chan loadedChunk, loadedBuf)

	db.logger.Debug("reserve sampler workers",
		"readers", readers, "hashers", workers, "sort_window", sortWindow)
	statsLock.Lock()
	allStats.ReadConcurrency = readers
	statsLock.Unlock()

	var readersWg sync.WaitGroup
	for range readers {
		readersWg.Add(1)
		g.Go(safe.RunFunc(db.logger, "storer-sample-reader", func() error {
			defer readersWg.Done()
			wstat := SampleStats{}
			defer func() { addStats(wstat) }()

			for chItem := range readC {
				chunkLoadStart := time.Now()
				chunk, err := db.ChunkStore().Get(ctx, chItem.Address)
				chunkLoadDuration := time.Since(chunkLoadStart)
				if err != nil {
					wstat.ChunkLoadFailed++
					db.logger.Debug("failed loading chunk", "chunk_address", chItem.Address, "error", err)
					continue
				}
				wstat.ChunkLoadDuration += chunkLoadDuration

				select {
				case loadedC <- loadedChunk{item: chItem, chunk: chunk}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}))
	}

	go func() {
		readersWg.Wait()
		close(loadedC)
	}()

	for range workers {
		g.Go(safe.RunFunc(db.logger, "storer-sample-hasher", func() error {
			wstat := SampleStats{}
			// One hasher per goroutine: bmt hashers carry state and are not
			// safe to share.
			hasher := bmt.NewPrefixHasher(anchor)
			defer func() { addStats(wstat) }()

			for lc := range loadedC {
				taddrStart := time.Now()
				taddr, err := transformedAddress(hasher, lc.chunk, lc.item.ChunkType)
				if err != nil {
					return err
				}
				wstat.TaddrDuration += time.Since(taddrStart)

				select {
				case sampleItemChan <- SampleItem{
					TransformedAddress: taddr,
					ChunkAddress:       lc.chunk.Address(),
					ChunkData:          lc.chunk.Data(),
					Stamp:              postage.NewStamp(lc.item.BatchID, nil, nil, nil),
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}))
	}

	go func() {
		_ = g.Wait()
		close(sampleItemChan)
	}()

	sampleItems := make([]SampleItem, 0, SampleSize)

	// insert function will insert the new item in its correct place. If the sample
	// size goes beyond what we need we omit the last item.
	insert := func(item SampleItem) {
		added := false
		for i, sItem := range sampleItems {
			if le(item.TransformedAddress, sItem.TransformedAddress) {
				sampleItems = append(sampleItems[:i+1], sampleItems[i:]...)
				sampleItems[i] = item
				added = true
				break
			} else if item.TransformedAddress.Compare(sItem.TransformedAddress) == 0 { // ensuring to pass the check order function of redistribution contract
				// replace the chunk at index if the chunk is CAC
				ch := swarm.NewChunk(item.ChunkAddress, item.ChunkData)
				_, err := soc.FromChunk(ch)
				if err != nil {
					sampleItems[i] = item
				}
				return
			}
		}
		if len(sampleItems) > SampleSize {
			sampleItems = sampleItems[:SampleSize]
		}
		if len(sampleItems) < SampleSize && !added {
			sampleItems = append(sampleItems, item)
		}
	}

	// Phase 3: Assemble the sample. Here we need to assemble only the first SampleSize
	// no of items from the results of the 2nd phase.
	// In this step stamps are loaded and validated only if chunk will be added to sample.
	stats := SampleStats{}
	for item := range sampleItemChan {
		currentMaxAddr := swarm.EmptyAddress
		if len(sampleItems) > 0 {
			currentMaxAddr = sampleItems[len(sampleItems)-1].TransformedAddress
		}

		if le(item.TransformedAddress, currentMaxAddr) || len(sampleItems) < SampleSize {
			stamp, err := chunkstamp.LoadWithBatchID(db.storage.IndexStore(), "reserve", item.ChunkAddress, item.Stamp.BatchID())
			if err != nil {
				stats.StampLoadFailed++
				db.logger.Debug("failed loading stamp", "chunk_address", item.ChunkAddress, "error", err)
				continue
			}

			ch := swarm.NewChunk(item.ChunkAddress, item.ChunkData).WithStamp(stamp)

			// check if the timestamp on the postage stamp is not later than the consensus time.
			if binary.BigEndian.Uint64(ch.Stamp().Timestamp()) > consensusTime {
				stats.NewIgnored++
				continue
			}

			stampValidStart := time.Now()
			if _, err := db.validStamp(ch); err != nil {
				stats.InvalidStamp++
				db.logger.Debug("invalid stamp for chunk", "chunk_address", ch.Address(), "error", err)
				continue
			}

			stampValidDuration := time.Since(stampValidStart)
			stats.ValidStampDuration += stampValidDuration

			item.Stamp = postage.NewStamp(stamp.BatchID(), stamp.Index(), stamp.Timestamp(), stamp.Sig())

			insert(item)
			stats.SampleInserts++
		}
	}
	addStats(stats)

	allStats.TotalDuration = time.Since(t)

	if err := g.Wait(); err != nil {
		db.logger.Info("reserve sampler finished with error", "err", err, "duration", time.Since(t), "storage_radius", committedDepth, "consensus_time_ns", consensusTime, "stats", fmt.Sprintf("%+v", allStats))

		return Sample{}, fmt.Errorf("sampler: failed creating sample: %w", err)
	}

	db.logger.Info("reserve sampler finished", "duration", time.Since(t), "storage_radius", committedDepth, "consensus_time_ns", consensusTime, "stats", fmt.Sprintf("%+v", allStats))

	return Sample{Stats: *allStats, Items: sampleItems}, nil
}

// ReserveSample generates the classic whole-neighbourhood sample the
// redistribution game uses today. It is the default reserve-proof-mode and is
// unchanged from upstream: it delegates to the shared reserveSample with no
// window predicate. WindowedSample (#273) is the experimental sublinear variant.
func (db *DB) ReserveSample(
	ctx context.Context,
	anchor []byte,
	committedDepth uint8,
	consensusTime uint64,
	minBatchBalance *big.Int,
) (Sample, error) {
	return db.reserveSample(ctx, anchor, committedDepth, consensusTime, minBatchBalance, nil)
}

// less function uses the byte compare to check for lexicographic ordering
func le(a, b swarm.Address) bool {
	return bytes.Compare(a.Bytes(), b.Bytes()) == -1
}

func (db *DB) batchesBelowValue(until *big.Int) (map[string]struct{}, error) {
	res := make(map[string]struct{})

	if until == nil {
		return res, nil
	}

	err := db.batchstore.Iterate(func(b *postage.Batch) (bool, error) {
		if b.Value.Cmp(until) < 0 {
			res[string(b.ID)] = struct{}{}
		}
		return false, nil
	})

	return res, err
}

func transformedAddress(hasher bmt.Hasher, chunk swarm.Chunk, chType swarm.ChunkType) (swarm.Address, error) {
	switch chType {
	case swarm.ChunkTypeContentAddressed:
		return transformedAddressCAC(hasher, chunk)
	case swarm.ChunkTypeSingleOwner:
		return transformedAddressSOC(hasher, chunk)
	default:
		return swarm.ZeroAddress, fmt.Errorf("chunk type [%v] is not valid", chType)
	}
}

func transformedAddressCAC(hasher bmt.Hasher, chunk swarm.Chunk) (swarm.Address, error) {
	hasher.Reset()
	hasher.SetHeader(chunk.Data()[:bmt.SpanSize])

	_, err := hasher.Write(chunk.Data()[bmt.SpanSize:])
	if err != nil {
		return swarm.ZeroAddress, err
	}

	return swarm.NewAddress(hasher.Sum(nil)), nil
}

func transformedAddressSOC(hasher bmt.Hasher, socChunk swarm.Chunk) (swarm.Address, error) {
	// Calculate transformed address from wrapped chunk
	cacChunk, err := soc.UnwrapCAC(socChunk)
	if err != nil {
		return swarm.ZeroAddress, err
	}
	taddrCac, err := transformedAddressCAC(hasher, cacChunk)
	if err != nil {
		return swarm.ZeroAddress, err
	}

	// Hash address and transformed address to make transformed address for this SOC
	sHasher := swarm.NewHasher()
	if _, err := sHasher.Write(socChunk.Address().Bytes()); err != nil {
		return swarm.ZeroAddress, err
	}
	if _, err := sHasher.Write(taddrCac.Bytes()); err != nil {
		return swarm.ZeroAddress, err
	}

	return swarm.NewAddress(sHasher.Sum(nil)), nil
}

type SampleStats struct {
	TotalDuration             time.Duration
	TotalIterated             int64
	IterationDuration         time.Duration
	SampleInserts             int64
	NewIgnored                int64
	InvalidStamp              int64
	BelowBalanceIgnored       int64
	TaddrDuration             time.Duration
	ValidStampDuration        time.Duration
	BatchesBelowValueDuration time.Duration
	RogueChunk                int64
	ChunkLoadDuration         time.Duration
	ChunkLoadFailed           int64
	StampLoadFailed           int64
	// ReadConcurrency is how many chunk loads were kept in flight. Recorded so
	// a measurement can say what it measured, and so a test can tell that the
	// setting reached the sampler at all rather than being quietly ignored.
	ReadConcurrency int
	// SortWindow is the read-ordering window actually used, after the cap. Zero
	// means the reads were issued in bin order, as they were before ordering
	// existed.
	SortWindow int
	// LocateDuration is time spent reading retrievalIdx for the sort key. It is
	// the cost side of read ordering and is reported separately so it can be
	// weighed against what the ordering saves, rather than the two being
	// visible only as one net number.
	LocateDuration time.Duration
	// SortDuration is time spent sorting the windows. Expected to be small
	// beside LocateDuration; recorded so that "small" is a measurement.
	SortDuration time.Duration
	// LocateFailed counts items whose location could not be read. These are
	// still passed to the readers, so they are not lost — they are counted here
	// and will usually be counted again in ChunkLoadFailed.
	LocateFailed int64
	// ChunkBinScanned is how many chunkBin entries the iteration walked, counted
	// before any filter. TotalIterated counts only the entries in the anchor's
	// neighbourhood, so this is what shows the cost of the index scan itself.
	// See issue #568.
	ChunkBinScanned int64
}

// sampleBins returns the range of reserve bins that can hold chunks within
// committedDepth of the anchor, and false when the range lies entirely below
// the storage radius. A chunk's bin is its proximity to the overlay. With p the
// proximity of the overlay to the anchor: if p < committedDepth, every such
// chunk shares exactly p bits with the overlay, so only bin p can hold them;
// otherwise they are exactly bins committedDepth to MaxPO. The derivation is in
// docs/experiments/sampler-bin-range/spec.md.
func sampleBins(overlay, anchor []byte, committedDepth, storageRadius uint8) (lo, hi uint8, ok bool) {
	p := swarm.Proximity(overlay, anchor)
	lo, hi = committedDepth, swarm.MaxPO
	if p < committedDepth {
		lo, hi = p, p
	}
	lo = max(lo, storageRadius)
	return lo, hi, lo <= hi
}

func (s *SampleStats) add(other SampleStats) {
	s.TotalDuration += other.TotalDuration
	s.IterationDuration += other.IterationDuration
	s.SampleInserts += other.SampleInserts
	s.NewIgnored += other.NewIgnored
	s.InvalidStamp += other.InvalidStamp
	s.BelowBalanceIgnored += other.BelowBalanceIgnored
	s.TaddrDuration += other.TaddrDuration
	s.ValidStampDuration += other.ValidStampDuration
	s.BatchesBelowValueDuration += other.BatchesBelowValueDuration
	s.RogueChunk += other.RogueChunk
	s.ChunkLoadDuration += other.ChunkLoadDuration
	s.ChunkLoadFailed += other.ChunkLoadFailed
	s.StampLoadFailed += other.StampLoadFailed
	s.TotalIterated += other.TotalIterated
	s.ChunkBinScanned += other.ChunkBinScanned
	// Assigned, not summed: exactly one ordering stage runs, and a sum would
	// still read correctly today while quietly becoming a multiple if that ever
	// stopped being true.
	if other.SortWindow > 0 {
		s.SortWindow = other.SortWindow
	}
	s.LocateDuration += other.LocateDuration
	s.SortDuration += other.SortDuration
	s.LocateFailed += other.LocateFailed
}

// RandSample returns Sample with random values.
func RandSample(t *testing.T, anchor []byte) Sample {
	t.Helper()

	chunks := make([]swarm.Chunk, SampleSize)
	for i := range SampleSize {
		ch := chunk.GenerateTestRandomChunk()
		if i%3 == 0 {
			ch = chunk.GenerateTestRandomSoChunk(t, ch)
		}
		chunks[i] = ch
	}

	sample, err := MakeSampleUsingChunks(chunks, anchor)
	if err != nil {
		t.Fatal(err)
	}

	return sample
}

// MakeSampleUsingChunks returns Sample constructed using supplied chunks.
func MakeSampleUsingChunks(chunks []swarm.Chunk, anchor []byte) (Sample, error) {
	items := make([]SampleItem, len(chunks))
	for i, ch := range chunks {
		tr, err := transformedAddress(bmt.NewPrefixHasher(anchor), ch, getChunkType(ch))
		if err != nil {
			return Sample{}, err
		}

		items[i] = SampleItem{
			TransformedAddress: tr,
			ChunkAddress:       ch.Address(),
			ChunkData:          ch.Data(),
			Stamp:              newStamp(ch.Stamp()),
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].TransformedAddress.Compare(items[j].TransformedAddress) == -1
	})

	return Sample{Items: items}, nil
}

func newStamp(s swarm.Stamp) *postage.Stamp {
	return postage.NewStamp(s.BatchID(), s.Index(), s.Timestamp(), s.Sig())
}

func getChunkType(chunk swarm.Chunk) swarm.ChunkType {
	if cac.Valid(chunk) {
		return swarm.ChunkTypeContentAddressed
	} else if soc.Valid(chunk) {
		return swarm.ChunkTypeSingleOwner
	}
	return swarm.ChunkTypeUnspecified
}

func (db *DB) recordReserveSampleMetrics(duration time.Duration, stats *SampleStats, workers int, err error) {
	status := "success"
	if err != nil {
		status = "failure"
	}
	db.metrics.ReserveSampleDuration.WithLabelValues(status).Observe(duration.Seconds())

	summaryMetrics := map[string]float64{
		"duration_seconds":                     duration.Seconds(),
		"chunks_iterated":                      float64(stats.TotalIterated),
		"chunk_bin_scanned":                    float64(stats.ChunkBinScanned),
		"chunks_load_failed":                   float64(stats.ChunkLoadFailed),
		"stamp_validations":                    float64(stats.SampleInserts),
		"invalid_stamps":                       float64(stats.InvalidStamp),
		"below_balance_ignored":                float64(stats.BelowBalanceIgnored),
		"workers":                              float64(workers),
		"chunks_per_second":                    float64(stats.TotalIterated) / duration.Seconds(),
		"stamp_validation_duration_seconds":    stats.ValidStampDuration.Seconds(),
		"batches_below_value_duration_seconds": stats.BatchesBelowValueDuration.Seconds(),
		"taddr_duration_seconds":               stats.TaddrDuration.Seconds(),
		"chunk_load_duration_seconds":          stats.ChunkLoadDuration.Seconds(),
		"iteration_duration_seconds":           stats.IterationDuration.Seconds(),
	}

	for metric, value := range summaryMetrics {
		db.metrics.ReserveSampleRunSummary.WithLabelValues(metric).Set(value)
	}

	db.metrics.ReserveSampleLastRunTimestamp.Set(float64(time.Now().Unix()))
}
