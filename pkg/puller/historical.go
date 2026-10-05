// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller

import (
	"context"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// This file tracks, for each peer and bin that is in scope at the current
// storage radius, whether the historical sync of that bin is finished. The
// reserve worker reads it to decide when the radius can be lowered. See
// docs/experiments/radius-on-sync-done/spec.md and issue #588.
//
// Locking. The map is guarded by histMtx. The lock order is syncPeersMtx,
// then peer.mtx, then histMtx. histMtx is a leaf lock: nothing else is
// acquired while it is held, it is never held across a wait or a network
// call, and the signal is sent without blocking. Sync workers take only
// histMtx, never peer.mtx, because stop() waits for the workers while
// peer.mtx is held.

const (
	// historicalStallBound is how long an entry may go without progress
	// before it is treated as stalled. A stalled entry neither blocks the
	// radius decision nor counts as a finished neighbour.
	historicalStallBound = 10 * time.Minute

	// historicalStallCheckInterval is how often the puller looks for
	// entries that have passed historicalStallBound, so that a worker that
	// is blocked, and so cannot report anything itself, is still noticed
	// and signalled.
	historicalStallCheckInterval = time.Minute

	// noHistoricalRadius is the published radius before the first
	// publication. Radius 0 is valid, so it cannot be the sentinel.
	noHistoricalRadius = -1
)

// histState is the state of the historical sync of one peer and bin. The
// stalled state is not stored: it is derived from the time of the last
// progress when the entry is read.
type histState uint8

const (
	histRunning histState = iota + 1
	histNotStarted
	histFinished
	histAborted
)

// Label values of the historical_bins metric.
const (
	histLabelRunning    = "running"
	histLabelNotStarted = "not_started"
	histLabelFinished   = "finished"
	histLabelStalled    = "stalled"
	histLabelAborted    = "aborted"
)

var histLabels = []string{histLabelRunning, histLabelNotStarted, histLabelFinished, histLabelStalled, histLabelAborted}

type histKey struct {
	peer string // peer address as a byte string
	bin  uint8
}

type histEntry struct {
	state histState
	// token is a generation number that is new for each historical
	// worker. A worker writes to the entry only while the entry carries
	// its own token, so a worker that exits late cannot change or
	// recreate an entry that belongs to someone else.
	token uint64
	// since is the time of the last progress of a running entry, or the
	// time a not started entry was first seen.
	since time.Time
	// sampling is set while the worker waits for a reserve sample to
	// finish. Waiting for a sample is not a stall.
	sampling bool
	// stalled is set once the entry has passed historicalStallBound, so
	// that the stall is logged and signalled once.
	stalled bool
}

func (e *histEntry) label() string {
	switch {
	case e.stalled:
		return histLabelStalled
	case e.state == histRunning:
		return histLabelRunning
	case e.state == histNotStarted:
		return histLabelNotStarted
	case e.state == histFinished:
		return histLabelFinished
	default:
		return histLabelAborted
	}
}

// histPeer is what the end of onChange learns about one peer, read under
// the peer's own lock before histMtx is taken.
type histPeer struct {
	address    swarm.Address
	po         uint8
	cursorsSet bool // the peer has cursors of the right length
}

// historicalBins returns the bins of a peer at proximity order po that are
// in scope at radius r, following syncPeer: every bin from r for a
// neighbour, only the bin equal to po for a peer up to maxPODelta below r,
// and none otherwise.
func (p *Puller) historicalBins(po, r uint8) []uint8 {
	if po >= r {
		bins := make([]uint8, 0, int(p.bins)-int(r))
		for b := r; b < p.bins; b++ {
			bins = append(bins, b)
		}
		return bins
	}
	if r-po <= maxPODelta && po < p.bins {
		return []uint8{po}
	}
	return nil
}

// histSignalLocked tells the reserve worker that the historical state may
// have changed. While onChange recalculates, the signal is held back and
// sent when the recalculation ends, because a reader during the
// recalculation sees "not done" and would otherwise not look again.
// Must be called with histMtx held.
func (p *Puller) histSignalLocked() {
	if p.histRecalculating {
		p.histPendingSignal = true
		return
	}
	select {
	case p.histChanged <- struct{}{}:
	default:
	}
}

// histUpdateMetricsLocked recounts the entries by state for the
// historical_bins metric. Called only when a state changes, which is rare
// next to the size of the map. Must be called with histMtx held.
func (p *Puller) histUpdateMetricsLocked() {
	counts := make(map[string]int, len(histLabels))
	for _, e := range p.histEntries {
		counts[e.label()]++
	}
	for _, l := range histLabels {
		p.metrics.HistoricalBins.WithLabelValues(l).Set(float64(counts[l]))
	}
}

// histBeginRecalc marks the start of onChange. Until histEndRecalc, the
// historical state reads as not done.
func (p *Puller) histBeginRecalc() {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()
	p.histRecalculating = true
}

// histEndRecalc runs at the end of onChange, after recalcPeers has
// returned. It removes the entries of bins that are no longer in scope,
// gives each in-scope bin of a peer without usable cursors a not started
// entry, publishes the radius the map now belongs to, and clears the
// recalculating flag.
func (p *Puller) histEndRecalc(radius uint8, peers []histPeer) {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()

	now := time.Now()
	changed := false

	inScope := make(map[histKey]struct{})
	p.histPeers = make(map[string]uint8, len(peers))
	for _, peer := range peers {
		addr := peer.address.ByteString()
		p.histPeers[addr] = peer.po
		for _, bin := range p.historicalBins(peer.po, radius) {
			key := histKey{peer: addr, bin: bin}
			inScope[key] = struct{}{}
			if peer.cursorsSet {
				continue
			}
			// An entry that is already not started keeps its first-seen
			// time, so a peer that keeps failing does reach the stall
			// bound.
			if _, ok := p.histEntries[key]; !ok {
				p.histNextToken++
				p.histEntries[key] = &histEntry{state: histNotStarted, token: p.histNextToken, since: now}
				changed = true
			}
		}
	}

	for key := range p.histEntries {
		if _, ok := inScope[key]; !ok {
			delete(p.histEntries, key)
			changed = true
		}
	}

	if p.histRadius != int(radius) {
		p.histRadius = int(radius)
		changed = true
	}

	if changed {
		p.histUpdateMetricsLocked()
		p.histSignalLocked()
	}

	p.histRecalculating = false
	if p.histPendingSignal {
		p.histPendingSignal = false
		p.histSignalLocked()
	}
}

// histRemovePeer deletes every entry of a peer. It is called by
// disconnectPeer after stop() has returned, when no worker of the peer can
// write any more.
func (p *Puller) histRemovePeer(addr swarm.Address) {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()

	a := addr.ByteString()
	delete(p.histPeers, a)
	removed := false
	for key := range p.histEntries {
		if key.peer == a {
			delete(p.histEntries, key)
			removed = true
		}
	}
	if removed {
		p.histUpdateMetricsLocked()
		p.histSignalLocked()
	}
}

// histStartWorker records that a historical worker starts for a peer and
// bin, and returns the worker's token. Called from syncPeerBin, under
// peer.mtx.
func (p *Puller) histStartWorker(addr swarm.Address, bin uint8) uint64 {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()

	p.histNextToken++
	p.histEntries[histKey{peer: addr.ByteString(), bin: bin}] = &histEntry{
		state: histRunning,
		token: p.histNextToken,
		since: time.Now(),
	}
	p.histUpdateMetricsLocked()
	return p.histNextToken
}

// histNothingToSync records a bin whose cursor is 0: there is no history
// to pull, so it is finished at once. Called from syncPeerBin, under
// peer.mtx.
func (p *Puller) histNothingToSync(addr swarm.Address, bin uint8) {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()

	p.histNextToken++
	p.histEntries[histKey{peer: addr.ByteString(), bin: bin}] = &histEntry{
		state: histFinished,
		token: p.histNextToken,
		since: time.Now(),
	}
	p.histUpdateMetricsLocked()
	p.histSignalLocked()
}

// histWorkerUpdate applies f to the entry of a historical worker, but only
// if the entry still exists and carries the worker's token. f reports
// whether the change is one that is signalled. remove deletes the entry
// instead.
func (p *Puller) histWorkerUpdate(addr swarm.Address, bin uint8, token uint64, remove bool, f func(*histEntry) (signal bool)) {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()

	key := histKey{peer: addr.ByteString(), bin: bin}
	e, ok := p.histEntries[key]
	if !ok || e.token != token {
		return
	}
	before := e.label()
	signal := false
	if remove {
		delete(p.histEntries, key)
		signal = true
	} else {
		signal = f(e)
	}
	if remove || e.label() != before {
		p.histUpdateMetricsLocked()
	}
	if signal {
		p.histSignalLocked()
	}
}

// histWorker is the view of the map that one sync worker has. Every
// method does nothing for a live worker.
type histWorker struct {
	p          *Puller
	historical bool
	addr       swarm.Address
	bin        uint8
	token      uint64
}

func (w histWorker) update(remove bool, f func(*histEntry) bool) {
	if !w.historical {
		return
	}
	w.p.histWorkerUpdate(w.addr, w.bin, w.token, remove, f)
}

// finished records the exit at start > cursor.
func (w histWorker) finished() {
	w.update(false, func(e *histEntry) bool {
		e.state = histFinished
		e.stalled = false
		e.sampling = false
		return true
	})
}

// aborted records an exit that leaves the bin unfinished while its live
// worker keeps running, so the bin is not restarted.
func (w histWorker) aborted(reason string) {
	w.update(false, func(e *histEntry) bool {
		if e.state == histAborted {
			return false
		}
		e.state = histAborted
		e.stalled = false
		e.sampling = false
		w.p.logger.Info("historical sync of a bin stopped before it finished; it no longer counts for the radius decision", "peer_address", w.addr, "bin", w.bin, "reason", reason)
		return true
	})
}

// gone records the exit on ErrPeerNotFound. The entry is removed.
func (w histWorker) gone() {
	w.update(true, nil)
}

// progress records a call that advanced the interval. A stalled entry
// counts as running again.
func (w histWorker) progress() {
	w.update(false, func(e *histEntry) bool {
		e.since = time.Now()
		e.stalled = false
		return false
	})
}

// sampling records that the worker waits for, or has stopped waiting for,
// a reserve sample. The time spent waiting does not count towards the
// stall bound.
func (w histWorker) sampling(waiting bool) {
	w.update(false, func(e *histEntry) bool {
		e.sampling = waiting
		if !waiting {
			e.since = time.Now()
		}
		return false
	})
}

// histMarkStalledLocked turns running and not started entries that have
// made no progress for longer than historicalStallBound into stalled
// ones, logs each once, and signals. Must be called with histMtx held.
func (p *Puller) histMarkStalledLocked(now time.Time) {
	changed := false
	for key, e := range p.histEntries {
		if e.stalled || e.sampling || (e.state != histRunning && e.state != histNotStarted) {
			continue
		}
		if now.Sub(e.since) <= historicalStallBound {
			continue
		}
		e.stalled = true
		changed = true
		state := histLabelRunning
		if e.state == histNotStarted {
			state = histLabelNotStarted
		}
		p.logger.Info("historical sync of a bin made no progress within the bound; it no longer counts for the radius decision", "peer_address", swarm.NewAddress([]byte(key.peer)), "bin", key.bin, "state", state, "bound", historicalStallBound)
	}
	if changed {
		p.histUpdateMetricsLocked()
		p.histSignalLocked()
	}
}

// histStallChecker looks for stalled entries on a fixed interval, so that
// a worker blocked in a call is noticed without anyone asking.
func (p *Puller) histStallChecker(ctx context.Context) {
	defer p.wg.Done()

	t := time.NewTicker(historicalStallCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			p.histMtx.Lock()
			p.histMarkStalledLocked(now)
			p.histMtx.Unlock()
		}
	}
}

// HistoricalSyncDone reports whether the historical sync for the given
// storage radius is finished. It is false while onChange recalculates, and
// when the radius differs from the one the puller last published. Otherwise
// it is true when no in-scope bin of any current peer is running or not
// started, and at least one neighbour (a peer at or above the radius) has
// every in-scope bin finished. Stalled and aborted bins do not block it and
// do not make a neighbour finished. A bin without an entry counts as not
// finished.
func (p *Puller) HistoricalSyncDone(radius uint8) bool {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()

	if p.histRecalculating {
		// The reader may have taken the signal for a completion that
		// happened before this recalculation. Signal again when it ends, so
		// the state is read once more instead of waiting for the ticker.
		p.histPendingSignal = true
		return false
	}
	if p.histRadius != int(radius) {
		return false
	}

	p.histMarkStalledLocked(time.Now())

	finishedNeighbour := false
	for addr, po := range p.histPeers {
		bins := p.historicalBins(po, radius)
		allFinished := len(bins) > 0
		for _, bin := range bins {
			e, ok := p.histEntries[histKey{peer: addr, bin: bin}]
			if !ok {
				allFinished = false
				continue
			}
			if e.stalled {
				allFinished = false
				continue
			}
			switch e.state {
			case histRunning, histNotStarted:
				return false
			case histFinished:
			default:
				allFinished = false
			}
		}
		if po >= radius && allFinished {
			finishedNeighbour = true
		}
	}
	return finishedNeighbour
}

// HasNeighbour reports whether at least one current peer is at or above the
// given radius. The reserve worker's fallback to the sync rate rule requires
// it, so that a node with no neighbours does not lower its radius on a rate
// that is 0 only because nothing is synced. See issue #588.
func (p *Puller) HasNeighbour(radius uint8) bool {
	p.histMtx.Lock()
	defer p.histMtx.Unlock()

	for _, po := range p.histPeers {
		if po >= radius {
			return true
		}
	}
	return false
}

// HistoricalSyncChanged returns a channel that receives a value, without
// blocking the sender, when the result of HistoricalSyncDone may have
// changed. The receiver re-reads the state, so a duplicate does no harm.
func (p *Puller) HistoricalSyncChanged() <-chan struct{} {
	return p.histChanged
}
