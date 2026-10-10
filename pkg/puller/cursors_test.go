// Copyright 2026 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/puller"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
)

// cursorStub answers the cursors request per peer with a configurable
// behaviour, and records which bins were synced.
type cursorStub struct {
	bins int

	mu       sync.Mutex
	behave   map[string]string // "silent", "error", "block" or "" (answer)
	release  chan struct{}     // closed to release "block"
	calls    map[string]int
	synced   map[string]map[uint8]bool
	blocking atomic.Int64
}

func newCursorStub(bins int) *cursorStub {
	return &cursorStub{bins: bins, behave: map[string]string{}, release: make(chan struct{}), calls: map[string]int{}, synced: map[string]map[uint8]bool{}}
}

func (s *cursorStub) set(peer swarm.Address, b string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.behave[peer.ByteString()] = b
}

func (s *cursorStub) callCount(peer swarm.Address) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[peer.ByteString()]
}

func (s *cursorStub) binSynced(peer swarm.Address, bin uint8) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.synced[peer.ByteString()][bin]
}

func (s *cursorStub) Sync(ctx context.Context, peer swarm.Address, bin uint8, _ uint64) (uint64, int, error) {
	s.mu.Lock()
	if s.synced[peer.ByteString()] == nil {
		s.synced[peer.ByteString()] = map[uint8]bool{}
	}
	s.synced[peer.ByteString()][bin] = true
	s.mu.Unlock()
	<-ctx.Done()
	return 0, 0, ctx.Err()
}

func (s *cursorStub) GetCursors(ctx context.Context, peer swarm.Address) ([]uint64, uint64, error) {
	s.mu.Lock()
	s.calls[peer.ByteString()]++
	b := s.behave[peer.ByteString()]
	s.mu.Unlock()
	switch b {
	case "silent":
		<-ctx.Done()
		return nil, 0, ctx.Err()
	case "error":
		return nil, 0, errors.New("cursors refused")
	case "block":
		s.blocking.Add(1)
		defer s.blocking.Add(-1)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
	return make([]uint64, s.bins), 0, nil
}

type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, string(p))
	return len(p), nil
}

func (l *logLines) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if strings.Contains(s, substr) {
			n++
		}
	}
	return n
}

type cursorPuller struct {
	p    *puller.Puller
	ps   *cursorStub
	kad  *kadMock.Mock
	logs *logLines
	rs   *resMock.ReserveStore
}

func newCursorPuller(t *testing.T, timeout time.Duration, dumpDir string, peers ...kadMock.AddrTuple) *cursorPuller {
	t.Helper()
	const bins = 3
	ps := newCursorStub(bins)
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(peers...))
	logs := &logLines{}
	logger := log.NewLogger("test", log.WithSink(logs), log.WithVerbosity(log.VerbosityInfo))
	rs := resMock.NewReserve(resMock.WithRadius(0))
	p := puller.New(swarm.RandAddress(t), mock.NewStateStore(), kad, rs, ps, nil, logger, puller.Options{Bins: bins, DumpDir: dumpDir})
	p.SetCursorsTimeout(timeout)
	return &cursorPuller{p: p, ps: ps, kad: kad, logs: logs, rs: rs}
}

func (c *cursorPuller) start(t *testing.T) {
	t.Helper()
	c.p.Start(context.Background())
	t.Cleanup(func() { _ = c.p.Close() })
}

func TestCursorTimeoutCountedAndLogged(t *testing.T) {
	t.Parallel()
	a, b := swarm.RandAddress(t), swarm.RandAddress(t)
	c := newCursorPuller(t, 50*time.Millisecond, "", kadMock.AddrTuple{Addr: a, PO: 1}, kadMock.AddrTuple{Addr: b, PO: 1})
	c.ps.set(a, "silent")
	c.ps.set(b, "error")
	c.start(t)

	waitFor(t, 5*time.Second, func() bool { return c.p.CursorRequestsFailed(puller.CursorFailTimeout) >= 1 }, "timeout not counted")
	waitFor(t, 5*time.Second, func() bool { return c.p.CursorRequestsFailed(puller.CursorFailError) >= 1 }, "error not counted")
	if got := c.p.CursorRequestsFailed(puller.CursorFailTimeout); got != 1 {
		t.Fatalf("timeouts %v after one run, want 1", got)
	}
	waitFor(t, 5*time.Second, func() bool { return c.logs.count("cursors request timed out") == 1 }, "timeout not logged once")

	// A second run within the log interval counts again but does not log again.
	c.kad.Trigger()
	waitFor(t, 5*time.Second, func() bool { return c.p.CursorRequestsFailed(puller.CursorFailTimeout) >= 2 }, "second timeout not counted")
	if n := c.logs.count("cursors request timed out"); n != 1 {
		t.Fatalf("timeout logged %d times within the interval, want 1", n)
	}
}

func TestCursorShutdownNotCounted(t *testing.T) {
	t.Parallel()
	a := swarm.RandAddress(t)
	c := newCursorPuller(t, time.Hour, "", kadMock.AddrTuple{Addr: a, PO: 1})
	c.ps.set(a, "silent")
	c.p.Start(context.Background())
	waitFor(t, 5*time.Second, func() bool { return c.ps.callCount(a) >= 1 }, "peer A never asked")
	_ = c.p.Close()
	if got := c.p.CursorRequestsFailed(puller.CursorFailTimeout) + c.p.CursorRequestsFailed(puller.CursorFailError); got != 0 {
		t.Fatalf("shutdown counted as %v failures, want 0", got)
	}
}

func TestCursorRetryAfterTimeout(t *testing.T) {
	t.Parallel()
	a := swarm.RandAddress(t)
	c := newCursorPuller(t, 50*time.Millisecond, "", kadMock.AddrTuple{Addr: a, PO: 1})
	c.ps.set(a, "silent")
	c.start(t)
	waitFor(t, 5*time.Second, func() bool { return c.p.CursorRequestsFailed(puller.CursorFailTimeout) >= 1 }, "timeout not counted")

	c.ps.set(a, "")
	waitFor(t, 5*time.Second, func() bool {
		c.kad.Trigger()
		return c.ps.binSynced(a, 1)
	}, "peer A's bins never started after it answered")
}

func TestOnChangeGaugeAndDuration(t *testing.T) {
	t.Parallel()
	a := swarm.RandAddress(t)
	c := newCursorPuller(t, time.Hour, "", kadMock.AddrTuple{Addr: a, PO: 1})
	c.ps.set(a, "block")
	c.start(t)

	waitFor(t, 5*time.Second, func() bool { return c.ps.blocking.Load() == 1 }, "run never blocked")
	if c.p.OnChangeStarted() == 0 {
		t.Fatal("start gauge 0 while a run is in progress")
	}
	if n := c.p.OnChangeDurationCount(); n != 0 {
		t.Fatalf("duration observed %d times before the run completed, want 0", n)
	}
	close(c.ps.release)
	waitFor(t, 5*time.Second, func() bool { return c.p.OnChangeDurationCount() == 1 }, "duration not observed")
	if g := c.p.OnChangeStarted(); g != 0 {
		t.Fatalf("start gauge %v after the run, want 0", g)
	}
}

func TestBlockedRunDump(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	a := swarm.RandAddress(t)
	c := newCursorPuller(t, time.Hour, dir, kadMock.AddrTuple{Addr: a, PO: 1})
	c.p.SetBlockedRunWatch(50*time.Millisecond, 10*time.Millisecond)
	c.ps.set(a, "block")
	c.start(t)

	waitFor(t, 5*time.Second, func() bool { return c.logs.count("goroutine profile written") >= 1 }, "no dump warning")
	time.Sleep(200 * time.Millisecond) // many watcher ticks while the same run stays blocked
	if n := c.logs.count("goroutine profile written"); n != 1 {
		t.Fatalf("dump warning %d times for one blocked run, want 1", n)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "puller-blocked-*.txt"))
	if len(files) != 1 {
		t.Fatalf("%d profile files, want 1", len(files))
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "goroutine profile:") {
		t.Fatal("file is not a goroutine profile")
	}
	close(c.ps.release)
}

func TestBlockedRunDumpsPruned(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for i := 0; i < 5; i++ {
		name := filepath.Join(dir, "puller-blocked-"+strings.Repeat("1", i+1)+".txt")
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		mod := time.Now().Add(time.Duration(i-10) * time.Minute)
		if err := os.Chtimes(name, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
	puller.PruneRunDumps(dir, 3)
	files, _ := filepath.Glob(filepath.Join(dir, "puller-blocked-*.txt"))
	if len(files) != 3 {
		t.Fatalf("%d files kept, want 3", len(files))
	}
	for _, f := range files {
		if strings.HasSuffix(f, "-1.txt") || strings.HasSuffix(f, "-11.txt") {
			t.Fatalf("oldest file %s kept", f)
		}
	}
}

func TestRadiusDecreaseActedOnWithSilentPeer(t *testing.T) {
	t.Parallel()
	a, b := swarm.RandAddress(t), swarm.RandAddress(t)
	c := newCursorPuller(t, 50*time.Millisecond, "", kadMock.AddrTuple{Addr: a, PO: 2}, kadMock.AddrTuple{Addr: b, PO: 2})
	c.rs.SetStorageRadius(2)
	c.ps.set(a, "silent")
	c.start(t)

	waitFor(t, 5*time.Second, func() bool { return c.ps.binSynced(b, 2) }, "bin 2 of B not synced at radius 2")
	if c.ps.binSynced(b, 1) {
		t.Fatal("bin 1 of B synced at radius 2")
	}
	c.rs.SetStorageRadius(1)
	waitFor(t, 10*time.Second, func() bool {
		c.kad.Trigger()
		return c.ps.binSynced(b, 1)
	}, "bin 1 opened by the radius decrease never synced from B")
}
