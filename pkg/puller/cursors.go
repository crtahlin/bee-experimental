// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// Defaults for the cursors request deadline and the blocked-run diagnostic.
// See #695.
const (
	// defaultCursorsTimeout bounds one cursors request, including the stream
	// open, negotiation, write and read. A correct peer answers from its
	// reserve's last BinIDs in milliseconds plus a round trip.
	defaultCursorsTimeout = 30 * time.Second

	// defaultBlockedRunThreshold is how long a recalculation may run before
	// the watcher writes a goroutine profile, once per run.
	defaultBlockedRunThreshold = 2 * time.Minute

	// defaultRunWatchInterval is how often the watcher checks the run in
	// progress.
	defaultRunWatchInterval = 10 * time.Second

	// cursorTimeoutLogInterval limits the info line naming a peer whose
	// cursors request timed out to once per peer in this interval.
	cursorTimeoutLogInterval = 10 * time.Minute

	// maxBlockedRunDumps is how many goroutine profile files are kept.
	maxBlockedRunDumps = 3

	blockedRunDumpPrefix = "puller-blocked-"
)

const (
	cursorFailTimeout = "timeout"
	cursorFailError   = "error"
)

// getCursors asks a peer for its cursors with a deadline. A failure is
// counted as a timeout or an error; a failure because the puller is shutting
// down is counted as neither.
func (p *Puller) getCursors(ctx context.Context, peer swarm.Address) ([]uint64, uint64, error) {
	cctx, cancel := context.WithTimeout(ctx, p.cursorsTimeout)
	defer cancel()

	cursors, epoch, err := p.syncer.GetCursors(cctx, peer)
	if err == nil {
		return cursors, epoch, nil
	}

	switch {
	case ctx.Err() != nil:
		// shutdown: neither a timeout nor an error of the peer
	case errors.Is(cctx.Err(), context.DeadlineExceeded):
		p.metrics.CursorRequestsFailed.WithLabelValues(cursorFailTimeout).Inc()
		if p.cursorTimeoutLog.allow(peer.ByteString(), time.Now()) {
			p.logger.Info("cursors request timed out", "peer_address", peer, "timeout", p.cursorsTimeout)
		}
	default:
		p.metrics.CursorRequestsFailed.WithLabelValues(cursorFailError).Inc()
	}
	return nil, 0, err
}

// rateLog allows one event per key per interval.
type rateLog struct {
	mu       sync.Mutex
	interval time.Duration
	last     map[string]time.Time
}

func newRateLog(interval time.Duration) *rateLog {
	return &rateLog{interval: interval, last: make(map[string]time.Time)}
}

func (r *rateLog) allow(key string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.last[key]; ok && now.Sub(t) < r.interval {
		return false
	}
	for k, t := range r.last {
		if now.Sub(t) >= r.interval {
			delete(r.last, k)
		}
	}
	r.last[key] = now
	return true
}

// runStarted records the start of a recalculation, for the gauge and the
// watcher.
func (p *Puller) runStarted(now time.Time) {
	p.runStart.Store(now.UnixNano())
	p.metrics.OnChangeStarted.Set(float64(now.UnixNano()) / 1e9)
}

// runFinished records the end of a recalculation.
func (p *Puller) runFinished(started time.Time) {
	p.metrics.OnChangeDuration.Observe(time.Since(started).Seconds())
	p.runStart.Store(0)
	p.metrics.OnChangeStarted.Set(0)
}

// watchRuns writes a goroutine profile once per recalculation that runs
// longer than blockedRunThreshold. It runs in its own goroutine because the
// recalculation itself is the code that is blocked.
func (p *Puller) watchRuns(ctx context.Context) {
	defer p.wg.Done()

	t := time.NewTicker(p.runWatchInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.checkRun(time.Now())
		}
	}
}

func (p *Puller) checkRun(now time.Time) {
	start := p.runStart.Load()
	if start == 0 || p.dumpedRun.Load() == start {
		return
	}
	age := now.Sub(time.Unix(0, start))
	if age < p.blockedRunThreshold {
		return
	}
	p.dumpedRun.Store(start)

	path, err := p.writeRunDump(now)
	if err != nil {
		p.logger.Warning("puller recalculation blocked; goroutine profile not written", "running_for", age, "error", err)
		return
	}
	p.logger.Warning("puller recalculation blocked; goroutine profile written", "running_for", age, "path", path)
}

// writeRunDump writes the goroutine profile (debug=1) to a file in dumpDir
// and keeps at most maxBlockedRunDumps such files.
func (p *Puller) writeRunDump(now time.Time) (string, error) {
	if p.dumpDir == "" {
		return "", errors.New("no data directory")
	}
	prof := pprof.Lookup("goroutine")
	if prof == nil {
		return "", errors.New("no goroutine profile")
	}
	path := filepath.Join(p.dumpDir, fmt.Sprintf("%s%d.txt", blockedRunDumpPrefix, now.Unix()))
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	if err := prof.WriteTo(f, 1); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	pruneRunDumps(p.dumpDir, maxBlockedRunDumps)
	return path, nil
}

// pruneRunDumps removes the oldest goroutine profile files beyond keep.
func pruneRunDumps(dir string, keep int) {
	files, err := filepath.Glob(filepath.Join(dir, blockedRunDumpPrefix+"*.txt"))
	if err != nil || len(files) <= keep {
		return
	}
	type entry struct {
		path string
		mod  time.Time
	}
	entries := make([]entry, 0, len(files))
	for _, f := range files {
		if fi, err := os.Stat(f); err == nil {
			entries = append(entries, entry{f, fi.ModTime()})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].mod.Equal(entries[j].mod) {
			return entries[i].path < entries[j].path
		}
		return entries[i].mod.Before(entries[j].mod)
	})
	for i := 0; i < len(entries)-keep; i++ {
		_ = os.Remove(entries[i].path)
	}
}
