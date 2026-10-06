// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package puller_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/puller"
	mockps "github.com/ethersphere/bee/v2/pkg/pullsync/mock"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storage"
	resMock "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	kadMock "github.com/ethersphere/bee/v2/pkg/topology/kademlia/mock"
)

// These tests cover the historical sync state the reserve worker reads to
// lower the storage radius. See docs/experiments/radius-on-sync-done/spec.md
// and issue #588. They run in a synctest bubble, so the 10 minute stall
// bound and the 5 minute recalculation interval pass in fake time.

const histCursor = 10

// cursorSource answers GetCursors per peer: a fixed error, other cursors,
// or a gate that holds the call in flight until the test opens it.
type cursorSource struct {
	mtx     sync.Mutex
	bins    int
	errs    map[string]error
	cursors map[string][]uint64
	gates   map[string]chan struct{}
}

func newCursorSource(bins int) *cursorSource {
	return &cursorSource{
		bins:    bins,
		errs:    make(map[string]error),
		cursors: make(map[string][]uint64),
		gates:   make(map[string]chan struct{}),
	}
}

func (c *cursorSource) get(ctx context.Context, peer swarm.Address) ([]uint64, uint64, error) {
	c.mtx.Lock()
	gate := c.gates[peer.ByteString()]
	c.mtx.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()
	if err := c.errs[peer.ByteString()]; err != nil {
		return nil, 0, err
	}
	if cur, ok := c.cursors[peer.ByteString()]; ok {
		return cur, 0, nil
	}
	cur := make([]uint64, c.bins)
	for i := range cur {
		cur[i] = histCursor
	}
	return cur, 0, nil
}

func (c *cursorSource) setErr(peer swarm.Address, err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.errs[peer.ByteString()] = err
}

func (c *cursorSource) setCursors(peer swarm.Address, cur []uint64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.cursors[peer.ByteString()] = cur
}

// hold makes the peer's next GetCursors calls wait until the returned
// function is called.
func (c *cursorSource) hold(peer swarm.Address) (open func()) {
	gate := make(chan struct{})
	c.mtx.Lock()
	c.gates[peer.ByteString()] = gate
	c.mtx.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mtx.Lock()
			delete(c.gates, peer.ByteString())
			c.mtx.Unlock()
			close(gate)
		})
	}
}

type histEnv struct {
	p       *puller.Puller
	kad     *kadMock.Mock
	ps      *mockps.PullSyncMock
	rs      *resMock.ReserveStore
	cursors *cursorSource
}

type histOpts struct {
	radius  uint8
	bins    uint8
	peers   []kadMock.AddrTuple
	replies []mockps.SyncReply
	store   storage.StateStorer
	backoff time.Duration
	// before runs after the puller is built and before it starts.
	before func(*histEnv)
}

// newHistEnv starts a puller inside the current synctest bubble and waits
// for its first recalculation to finish.
func newHistEnv(t *testing.T, o histOpts) *histEnv {
	t.Helper()

	st := o.store
	if st == nil {
		st = mock.NewStateStore()
	}
	cs := newCursorSource(int(o.bins))
	ps := mockps.NewPullSync(mockps.WithGetCursorsFunc(cs.get), mockps.WithReplies(o.replies...))
	kad := kadMock.NewMockKademlia(kadMock.WithEachPeerRevCalls(o.peers...))
	rs := resMock.NewReserve(resMock.WithRadius(o.radius))
	p := puller.New(swarm.RandAddress(t), st, kad, rs, ps, nil, log.Noop, puller.Options{Bins: o.bins})
	if o.backoff > 0 {
		p.SetRetryBackoff(o.backoff, time.Minute)
	}
	env := &histEnv{p: p, kad: kad, ps: ps, rs: rs, cursors: cs}
	if o.before != nil {
		o.before(env)
	}
	p.Start(context.Background())
	t.Cleanup(func() { _ = p.Close() })
	synctest.Wait()
	return env
}

// finish returns replies that make the historical workers of the given
// bins finish: one call covers the whole range up to the cursor.
func finish(addr swarm.Address, bins ...uint8) []mockps.SyncReply {
	r := make([]mockps.SyncReply, 0, len(bins))
	for _, b := range bins {
		r = append(r, mockps.SyncReply{Peer: addr, Bin: b, Start: 1, Topmost: histCursor})
	}
	return r
}

// signalled reports whether a signal is waiting, and consumes it.
func signalled(p *puller.Puller) bool {
	synctest.Wait()
	select {
	case <-p.HistoricalSyncChanged():
		return true
	default:
		return false
	}
}

func drain(p *puller.Puller) { _ = signalled(p) }

func wantDone(t *testing.T, p *puller.Puller, radius uint8, want bool, why string) {
	t.Helper()
	synctest.Wait()
	if got := p.HistoricalSyncDone(radius); got != want {
		t.Fatalf("HistoricalSyncDone(%d) = %v, want %v: %s", radius, got, want, why)
	}
}

func wantEntry(t *testing.T, p *puller.Puller, addr swarm.Address, bin uint8, want string) {
	t.Helper()
	got, ok := p.HistoricalEntry(addr, bin)
	if want == "" {
		if ok {
			t.Fatalf("bin %d has entry %q, want none", bin, got)
		}
		return
	}
	if !ok || got != want {
		t.Fatalf("bin %d entry = %q (exists %v), want %q", bin, got, ok, want)
	}
}

// Test 1: not done while a historical worker runs, including one that
// pulls a range the node already holds and so stores nothing.
func TestHistoricalNotDoneWhileWorkerRuns(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a := swarm.RandAddress(t)
		replies := append(finish(a, 2),
			// Bin 3 advances to 5 storing nothing (count 0), then waits.
			mockps.SyncReply{Peer: a, Bin: 3, Start: 1, Topmost: 5, Count: 0})
		env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}}, replies: replies})

		wantEntry(t, env.p, a, 2, "finished")
		wantEntry(t, env.p, a, 3, "running")
		if r := env.p.SyncRate(); r != 0 {
			t.Fatalf("sync rate %v, want 0: the running worker stored nothing", r)
		}
		wantDone(t, env.p, 2, false, "a worker that runs, even one storing nothing, is pending work")
	})
}

// Test 2: done once every in-scope worker has finished.
func TestHistoricalDoneWhenAllFinished(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a := swarm.RandAddress(t)
		band := swarm.RandAddress(t)
		replies := append(finish(a, 2, 3), finish(band, 1)...)
		env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}, {Addr: band, PO: 1}}, replies: replies})

		wantDone(t, env.p, 2, true, "every in-scope bin finished")
		if got := env.p.HistoricalBinsMetric("finished"); got != 3 {
			t.Fatalf("historical_bins{state=finished} = %v, want 3", got)
		}
	})
}

// Test 3: not done without a neighbour, whether there are no peers at all
// or only peers in the band below the radius.
func TestHistoricalNotDoneWithoutNeighbour(t *testing.T) {
	t.Parallel()
	t.Run("no peers", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			env := newHistEnv(t, histOpts{radius: 2, bins: 4})
			if r := env.p.PublishedHistoricalRadius(); r != 2 {
				t.Fatalf("published radius %d, want 2", r)
			}
			wantDone(t, env.p, 2, false, "with no peers nothing has been synced")
		})
	})
	t.Run("band peers only", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			b1, b0 := swarm.RandAddress(t), swarm.RandAddress(t)
			replies := append(finish(b1, 1), finish(b0, 0)...)
			env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: b1, PO: 1}, {Addr: b0, PO: 0}}, replies: replies})
			wantEntry(t, env.p, b1, 1, "finished")
			wantEntry(t, env.p, b0, 0, "finished")
			wantDone(t, env.p, 2, false, "finished band peers alone do not make the sync done")
		})
	})
}

// Test 4: not done when every neighbour is stalled, even when every other
// bin is finished.
func TestHistoricalNotDoneWhenEveryNeighbourStalled(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, band := swarm.RandAddress(t), swarm.RandAddress(t)
		// Bin 3 of the neighbour gets no reply, so its worker waits in Sync.
		replies := append(finish(a, 2), finish(band, 1)...)
		env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}, {Addr: band, PO: 1}}, replies: replies})

		wantDone(t, env.p, 2, false, "the neighbour's bin 3 is running")
		time.Sleep(puller.HistoricalStallBound + 2*time.Minute)
		wantDone(t, env.p, 2, false, "the only neighbour is stalled, so no neighbour has finished")
		if got := env.p.HistoricalBinsMetric("stalled"); got != 1 {
			t.Fatalf("historical_bins{state=stalled} = %v, want 1", got)
		}
	})
}

// Test 5: not done for a radius other than the published one.
func TestHistoricalNotDoneForOtherRadius(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a := swarm.RandAddress(t)
		env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 3}}, replies: finish(a, 2, 3)})
		wantDone(t, env.p, 2, true, "published radius, all finished")
		wantDone(t, env.p, 1, false, "radius 1 is not the published radius")
		wantDone(t, env.p, 3, false, "radius 3 is not the published radius")
	})
}

// Test 6: not done during recalcPeers, and the radius is published only
// after it.
func TestHistoricalNotDoneBeforePublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, b := swarm.RandAddress(t), swarm.RandAddress(t)
		var openA func()
		env := newHistEnv(t, histOpts{
			radius: 2, bins: 4,
			peers:   []kadMock.AddrTuple{{Addr: a, PO: 3}},
			replies: append(finish(a, 2, 3), finish(b, 3)...),
			before:  func(e *histEnv) { openA = e.cursors.hold(a) },
		})

		// The first recalculation waits in a's GetCursors.
		if r := env.p.PublishedHistoricalRadius(); r != -1 {
			t.Fatalf("published radius %d during the first recalculation, want none (-1)", r)
		}
		wantDone(t, env.p, 2, false, "the first recalculation has not finished")
		openA()
		wantDone(t, env.p, 2, true, "the recalculation finished and a's bins finished")

		// A radius increase while a new peer's GetCursors is in flight: the
		// old radius stays published until the recalculation ends.
		openB := env.cursors.hold(b)
		env.kad.AddRevPeers(kadMock.AddrTuple{Addr: b, PO: 3})
		env.rs.SetStorageRadius(3)
		env.kad.Trigger()
		synctest.Wait()
		if r := env.p.PublishedHistoricalRadius(); r != 2 {
			t.Fatalf("published radius %d while recalcPeers runs for radius 3, want 2: the radius is published after recalcPeers", r)
		}
		wantDone(t, env.p, 3, false, "recalcPeers for radius 3 has not finished")
		openB()
		synctest.Wait()
		if r := env.p.PublishedHistoricalRadius(); r != 3 {
			t.Fatalf("published radius %d after the recalculation, want 3", r)
		}
		wantDone(t, env.p, 3, true, "both neighbours finished bin 3")
	})
}

// Test 7: a worker without progress is stalled after the bound, whether it
// is blocked or keeps failing, and counts as running again after progress.
func TestHistoricalStallAndRecovery(t *testing.T) {
	t.Parallel()
	t.Run("blocked", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			a, b := swarm.RandAddress(t), swarm.RandAddress(t)
			release := make(chan struct{})
			replies := append(finish(a, 2, 3), finish(b, 2)...)
			replies = append(replies, mockps.SyncReply{Peer: b, Bin: 3, Start: 1, Topmost: 5, Release: release})
			env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}, {Addr: b, PO: 2}}, replies: replies})

			wantDone(t, env.p, 2, false, "b's bin 3 is blocked in Sync, within the bound")
			drain(env.p)
			time.Sleep(puller.HistoricalStallBound - time.Minute)
			wantDone(t, env.p, 2, false, "b's bin 3 is still within the bound")
			time.Sleep(3 * time.Minute)
			if !signalled(env.p) {
				t.Fatal("no signal when b's bin 3 became stalled")
			}
			wantDone(t, env.p, 2, true, "b's bin 3 is stalled and a has finished")
			wantEntry(t, env.p, b, 3, "stalled")

			// Progress: the call returns with the range advanced to 5, and
			// the next call waits again.
			close(release)
			wantDone(t, env.p, 2, false, "b's bin 3 made progress and is running again")
			wantEntry(t, env.p, b, 3, "running")

			time.Sleep(puller.HistoricalStallBound + 2*time.Minute)
			wantDone(t, env.p, 2, true, "b's bin 3 is stalled again")
		})
	})
	t.Run("failing", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			a, b := swarm.RandAddress(t), swarm.RandAddress(t)
			replies := append(finish(a, 2, 3), finish(b, 2)...)
			for range 100 {
				replies = append(replies, mockps.SyncReply{Peer: b, Bin: 3, Start: 1, Topmost: 0, Err: errStreamReset})
			}
			env := newHistEnv(t, histOpts{radius: 2, bins: 4, backoff: time.Second, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}, {Addr: b, PO: 2}}, replies: replies})

			wantDone(t, env.p, 2, false, "b's bin 3 keeps failing, within the bound")
			time.Sleep(puller.HistoricalStallBound + 2*time.Minute)
			if n := len(env.ps.SyncCalls(b)); n < 5 {
				t.Fatalf("got %d sync calls to b, want the failing worker to keep retrying", n)
			}
			wantDone(t, env.p, 2, true, "b's bin 3 failed for longer than the bound and is stalled")
		})
	})
}

// Test 8: a peer whose GetCursors keeps failing is not started, and is
// excluded after the bound.
func TestHistoricalCursorsFailing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, b := swarm.RandAddress(t), swarm.RandAddress(t)
		env := newHistEnv(t, histOpts{
			radius: 2, bins: 4,
			peers:   []kadMock.AddrTuple{{Addr: a, PO: 2}, {Addr: b, PO: 2}},
			replies: finish(a, 2, 3),
			before:  func(e *histEnv) { e.cursors.setErr(b, errors.New("cursors failed")) },
		})

		wantEntry(t, env.p, b, 2, "not_started")
		wantEntry(t, env.p, b, 3, "not_started")
		if got := env.p.HistoricalBinsMetric("not_started"); got != 2 {
			t.Fatalf("historical_bins{state=not_started} = %v, want 2", got)
		}
		wantDone(t, env.p, 2, false, "b has no cursors, its bins are not started")

		// The puller retries GetCursors every 5 minutes; the first-seen time
		// must survive the retries for the bound to be reached.
		time.Sleep(puller.HistoricalStallBound + 2*time.Minute)
		if got := env.p.HistoricalBinsMetric("stalled"); got != 2 {
			t.Fatalf("historical_bins{state=stalled} = %v, want 2", got)
		}
		wantDone(t, env.p, 2, true, "b's bins passed the bound and are excluded")
	})
}

// Test 9: a peer that returns the wrong number of cursors is not started.
func TestHistoricalCursorsWrongLength(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, b := swarm.RandAddress(t), swarm.RandAddress(t)
		env := newHistEnv(t, histOpts{
			radius: 2, bins: 4,
			peers:   []kadMock.AddrTuple{{Addr: a, PO: 2}, {Addr: b, PO: 3}},
			replies: finish(a, 2, 3),
			before:  func(e *histEnv) { e.cursors.setCursors(b, []uint64{histCursor, histCursor, histCursor}) },
		})
		wantEntry(t, env.p, b, 2, "not_started")
		wantEntry(t, env.p, b, 3, "not_started")
		wantDone(t, env.p, 2, false, "b's cursors have the wrong length, its bins are not started")
	})
}

// failStore fails reads or writes of chosen keys, to drive the worker's
// abort exits.
type failStore struct {
	storage.StateStorer
	mtx      sync.Mutex
	failGet  string
	failPut  string
	putsSeen int
}

func (s *failStore) Get(key string, i any) error {
	s.mtx.Lock()
	fail := key == s.failGet
	s.mtx.Unlock()
	if fail {
		return errors.New("injected read failure")
	}
	return s.StateStorer.Get(key, i)
}

// Put fails writes of failPut after the first, so the interval is created
// and the failure hits the write after progress.
func (s *failStore) Put(key string, i any) error {
	s.mtx.Lock()
	fail := false
	if key == s.failPut {
		s.putsSeen++
		fail = s.putsSeen > 1
	}
	s.mtx.Unlock()
	if fail {
		return errors.New("injected write failure")
	}
	return s.StateStorer.Put(key, i)
}

// Test 10: each abort exit is excluded, and ErrPeerNotFound removes the
// entry. Neither makes its neighbour finished.
func TestHistoricalAbortExits(t *testing.T) {
	t.Parallel()

	b := swarm.MustParseHexAddress("0200000000000000000000000000000000000000000000000000000000000000")
	for _, tc := range []struct {
		name  string
		reply []mockps.SyncReply
		store func() *failStore
		want  string // entry state of b's bin 3, "" for none
	}{
		{
			name:  "next interval failure",
			store: func() *failStore { return &failStore{failGet: puller.PeerIntervalKey(b, 3)} },
			want:  "aborted",
		},
		{
			name:  "max uint64 topmost",
			reply: []mockps.SyncReply{{Peer: b, Bin: 3, Start: 1, Topmost: math.MaxUint64}},
			want:  "aborted",
		},
		{
			name:  "persist interval failure",
			reply: []mockps.SyncReply{{Peer: b, Bin: 3, Start: 1, Topmost: 5}},
			store: func() *failStore { return &failStore{failPut: puller.PeerIntervalKey(b, 3)} },
			want:  "aborted",
		},
		{
			name:  "peer not found",
			reply: []mockps.SyncReply{{Peer: b, Bin: 3, Start: 1, Topmost: 0, Err: p2p.ErrPeerNotFound}},
			want:  "",
		},
	} {
		for _, withA := range []bool{true, false} {
			name := tc.name + "/alone"
			if withA {
				name = tc.name + "/with finished neighbour"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					a := swarm.RandAddress(t)
					peers := []kadMock.AddrTuple{{Addr: b, PO: 2}}
					replies := append(finish(b, 2), tc.reply...)
					if withA {
						peers = append(peers, kadMock.AddrTuple{Addr: a, PO: 2})
						replies = append(replies, finish(a, 2, 3)...)
					}
					var st storage.StateStorer
					if tc.store != nil {
						fs := tc.store()
						fs.StateStorer = mock.NewStateStore()
						st = fs
					}
					env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: peers, replies: replies, store: st})

					wantEntry(t, env.p, b, 3, tc.want)
					if tc.want == "aborted" {
						if got := env.p.HistoricalBinsMetric("aborted"); got != 1 {
							t.Fatalf("historical_bins{state=aborted} = %v, want 1", got)
						}
					}
					wantDone(t, env.p, 2, withA, "b's bin 3 did not finish: excluded, and b is not a finished neighbour")
				})
			})
		}
	}
}

// Test 11: an entry removed after its bin was cancelled is not recreated
// by the worker that exits late, whether it exits as finished, aborted or
// cancelled. The token keeps a late worker off a newer entry.
func TestHistoricalCancelledWorkerWritesNothing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		reply func(addr swarm.Address, release chan struct{}) []mockps.SyncReply
	}{
		{"finished", func(addr swarm.Address, r chan struct{}) []mockps.SyncReply {
			return []mockps.SyncReply{{Peer: addr, Bin: 2, Start: 1, Topmost: histCursor, Release: r}}
		}},
		{"aborted", func(addr swarm.Address, r chan struct{}) []mockps.SyncReply {
			return []mockps.SyncReply{{Peer: addr, Bin: 2, Start: 1, Topmost: math.MaxUint64, Release: r}}
		}},
		{"cancelled", func(swarm.Address, chan struct{}) []mockps.SyncReply { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				a := swarm.RandAddress(t)
				release := make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				replies := append(finish(a, 3), tc.reply(a, release)...)
				env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 3}}, replies: replies})
				wantEntry(t, env.p, a, 2, "running")

				// The radius rises, bin 2 leaves the scope: its worker is
				// cancelled and its entry removed.
				env.rs.SetStorageRadius(3)
				env.kad.Trigger()
				synctest.Wait()
				wantEntry(t, env.p, a, 2, "")

				// Now the worker returns from Sync.
				close(release)
				synctest.Wait()
				wantEntry(t, env.p, a, 2, "")
				wantDone(t, env.p, 3, true, "bin 3 finished; the cancelled bin 2 is out of scope")
			})
		})
	}

	t.Run("token", func(t *testing.T) {
		t.Parallel()
		p := puller.New(swarm.RandAddress(t), nil, nil, nil, nil, nil, log.Noop, puller.Options{Bins: 4})
		a := swarm.RandAddress(t)
		old := p.HistStartWorker(a, 2)
		_ = p.HistStartWorker(a, 2) // a new worker for the same bin
		p.HistWorkerFinished(a, 2, old)
		wantEntry(t, p, a, 2, "running")

		p.HistRemovePeer(a)
		p.HistWorkerFinished(a, 2, old)
		wantEntry(t, p, a, 2, "")
	})
}

// Test 11b: after a radius decrease where re-fetching a peer's cursors
// fails, no finished entry from the old radius remains.
func TestHistoricalRadiusDecreaseCursorsFail(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a := swarm.RandAddress(t)
		env := newHistEnv(t, histOpts{radius: 3, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 3}}, replies: finish(a, 3)})
		wantEntry(t, env.p, a, 3, "finished")
		wantDone(t, env.p, 3, true, "bin 3 finished at radius 3")

		env.cursors.setErr(a, errors.New("cursors failed"))
		env.rs.SetStorageRadius(2)
		env.kad.Trigger()
		synctest.Wait()

		wantEntry(t, env.p, a, 2, "not_started")
		wantEntry(t, env.p, a, 3, "not_started")
		wantDone(t, env.p, 2, false, "a's cursors could not be fetched at the new radius")
	})
}

// Test 11c: a completion during a recalculation is signalled when the
// recalculation ends.
func TestHistoricalCompletionDuringRecalcSignalled(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, far := swarm.RandAddress(t), swarm.RandAddress(t)
		release := make(chan struct{})
		replies := append(finish(a, 3), mockps.SyncReply{Peer: a, Bin: 4, Start: 1, Topmost: histCursor, Release: release})
		env := newHistEnv(t, histOpts{radius: 3, bins: 5, peers: []kadMock.AddrTuple{{Addr: a, PO: 3}}, replies: replies})
		wantDone(t, env.p, 3, false, "a's bin 4 is running")
		drain(env.p)

		// A peer outside the band joins; its GetCursors call holds the
		// recalculation open.
		openFar := env.cursors.hold(far)
		env.kad.AddRevPeers(kadMock.AddrTuple{Addr: far, PO: 0})
		env.kad.Trigger()
		synctest.Wait()

		// a's last bin finishes while the recalculation runs.
		close(release)
		synctest.Wait()
		wantEntry(t, env.p, a, 4, "finished")
		if signalled(env.p) {
			t.Fatal("signal sent while the recalculation runs; it must wait for the end, when the state can be read as done")
		}
		wantDone(t, env.p, 3, false, "the recalculation still runs")

		openFar()
		if !signalled(env.p) {
			t.Fatal("no signal when the recalculation ended after a completion during it")
		}
		wantDone(t, env.p, 3, true, "a finished, the peer outside the band has no work")
	})
}

// Test 11d: a completion signalled before a recalculation, but read while
// the recalculation runs, is signalled again when it ends. Otherwise the
// reader sees "not done" and nothing asks again until the ticker.
func TestHistoricalReadDuringRecalcSignalledAgain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, far := swarm.RandAddress(t), swarm.RandAddress(t)
		release := make(chan struct{})
		replies := append(finish(a, 3), mockps.SyncReply{Peer: a, Bin: 4, Start: 1, Topmost: histCursor, Release: release})
		env := newHistEnv(t, histOpts{radius: 3, bins: 5, peers: []kadMock.AddrTuple{{Addr: a, PO: 3}}, replies: replies})
		wantDone(t, env.p, 3, false, "a's bin 4 is running")
		drain(env.p)

		// a's last bin finishes; the reader takes the signal.
		close(release)
		synctest.Wait()
		if !signalled(env.p) {
			t.Fatal("no signal when the last bin finished")
		}

		// Before the reader looks, a recalculation starts and is held open.
		openFar := env.cursors.hold(far)
		env.kad.AddRevPeers(kadMock.AddrTuple{Addr: far, PO: 0})
		env.kad.Trigger()
		synctest.Wait()
		wantDone(t, env.p, 3, false, "the recalculation runs")

		openFar()
		if !signalled(env.p) {
			t.Fatal("no signal when the recalculation ended after a read during it returned not done")
		}
		wantDone(t, env.p, 3, true, "a finished, the peer outside the band has no work")
	})
}

// Test 12: a neighbour's last bin finishing between two recalculations
// makes the state done at once.
func TestHistoricalDoneAtOnceBetweenRecalcs(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a := swarm.RandAddress(t)
		release := make(chan struct{})
		replies := append(finish(a, 2), mockps.SyncReply{Peer: a, Bin: 3, Start: 1, Topmost: histCursor, Release: release})
		env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}}, replies: replies})
		wantDone(t, env.p, 2, false, "bin 3 is running")
		drain(env.p)

		close(release)
		if !signalled(env.p) {
			t.Fatal("no signal when the last bin finished")
		}
		wantDone(t, env.p, 2, true, "the last bin finished, no recalculation needed")
	})
}

// Test 13: a new neighbour at an unchanged radius makes the state not done
// while its GetCursors call is in flight.
func TestHistoricalNewNeighbourNotDone(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, b := swarm.RandAddress(t), swarm.RandAddress(t)
		env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}}, replies: append(finish(a, 2, 3), finish(b, 2, 3)...)})
		wantDone(t, env.p, 2, true, "a finished")

		openB := env.cursors.hold(b)
		env.kad.AddRevPeers(kadMock.AddrTuple{Addr: b, PO: 2})
		env.kad.Trigger()
		synctest.Wait()
		if !env.ps.CursorsCalls(b) {
			t.Fatal("b's GetCursors was not called")
		}
		wantDone(t, env.p, 2, false, "b's GetCursors is in flight")

		openB()
		wantDone(t, env.p, 2, true, "b finished too")
	})
}

// Test 14: no entry is left for a peer after it disconnects, across
// cancels and radius changes.
func TestHistoricalNoEntriesAfterDisconnect(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		a, b := swarm.RandAddress(t), swarm.RandAddress(t)
		env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}, {Addr: b, PO: 3}}, replies: append(finish(a, 2), finish(b, 2, 3)...)})
		if n := env.p.HistoricalEntries(a); n != 2 {
			t.Fatalf("a has %d entries, want 2", n)
		}

		// At radius 3, a (PO 2) is in the band and keeps only bin 2; b keeps
		// only bin 3. The other bins are cancelled.
		env.rs.SetStorageRadius(3)
		env.kad.Trigger()
		synctest.Wait()
		if n := env.p.HistoricalEntries(a); n != 1 {
			t.Fatalf("a has %d entries at radius 3, want 1 (its PO bin)", n)
		}
		if n := env.p.HistoricalEntries(b); n != 1 {
			t.Fatalf("b has %d entries at radius 3, want 1", n)
		}

		env.rs.SetStorageRadius(2) // a decrease disconnects and reconnects every peer
		env.kad.Trigger()
		synctest.Wait()
		if n := env.p.HistoricalEntries(a); n != 2 {
			t.Fatalf("a has %d entries back at radius 2, want 2", n)
		}

		env.kad.ResetPeers()
		env.kad.AddRevPeers(kadMock.AddrTuple{Addr: b, PO: 3})
		env.kad.Trigger()
		synctest.Wait()
		if n := env.p.HistoricalEntries(a); n != 0 {
			t.Fatalf("a has %d entries after it disconnected, want 0", n)
		}
		if n := env.p.HistoricalEntriesTotal(); n != 2 {
			t.Fatalf("the map has %d entries, want 2 (b's bins 2 and 3)", n)
		}

		env.kad.ResetPeers()
		env.kad.Trigger()
		synctest.Wait()
		if n := env.p.HistoricalEntriesTotal(); n != 0 {
			t.Fatalf("the map has %d entries with no peers, want 0", n)
		}
	})
}

// Test 15: every signal case sends a signal, and a recalculation that
// changes nothing sends none.
func TestHistoricalSignals(t *testing.T) {
	t.Parallel()

	t.Run("first publication and radius change", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			env := newHistEnv(t, histOpts{radius: 2, bins: 4})
			if !signalled(env.p) {
				t.Fatal("no signal on the first publication")
			}
			env.kad.Trigger()
			if signalled(env.p) {
				t.Fatal("signal from a recalculation that changed nothing")
			}
			env.rs.SetStorageRadius(3)
			env.kad.Trigger()
			if !signalled(env.p) {
				t.Fatal("no signal when the published radius changed")
			}
		})
	})

	t.Run("finished with nothing to sync", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			b := swarm.RandAddress(t)
			env := newHistEnv(t, histOpts{radius: 2, bins: 4})
			drain(env.p)
			env.cursors.setCursors(b, []uint64{0, 0, 0, 0})
			env.kad.AddRevPeers(kadMock.AddrTuple{Addr: b, PO: 2})
			env.kad.Trigger()
			if !signalled(env.p) {
				t.Fatal("no signal when bins with cursor 0 were finished")
			}
			wantDone(t, env.p, 2, true, "cursor 0 means nothing to sync")
		})
	})

	t.Run("worker finished, aborted, gone", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			a := swarm.RandAddress(t)
			r1, r2, r3 := make(chan struct{}), make(chan struct{}), make(chan struct{})
			replies := []mockps.SyncReply{
				{Peer: a, Bin: 1, Start: 1, Topmost: histCursor, Release: r1},
				{Peer: a, Bin: 2, Start: 1, Topmost: math.MaxUint64, Release: r2},
				{Peer: a, Bin: 3, Start: 1, Topmost: 0, Err: p2p.ErrPeerNotFound, Release: r3},
			}
			env := newHistEnv(t, histOpts{radius: 1, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 1}}, replies: replies})
			drain(env.p)

			for _, step := range []struct {
				name    string
				release chan struct{}
			}{{"finished", r1}, {"aborted", r2}, {"gone", r3}} {
				if signalled(env.p) {
					t.Fatalf("signal before %s", step.name)
				}
				close(step.release)
				if !signalled(env.p) {
					t.Fatalf("no signal when a worker exited as %s", step.name)
				}
			}
		})
	})

	t.Run("stalled", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			a := swarm.RandAddress(t)
			env := newHistEnv(t, histOpts{radius: 2, bins: 4, peers: []kadMock.AddrTuple{{Addr: a, PO: 2}}, replies: finish(a, 2)})
			drain(env.p)
			time.Sleep(puller.HistoricalStallBound - time.Minute)
			if signalled(env.p) {
				t.Fatal("signal before the stall bound")
			}
			time.Sleep(2 * time.Minute)
			if !signalled(env.p) {
				t.Fatal("no signal when the blocked worker became stalled, without anyone reading the state")
			}
		})
	})

	t.Run("not started set and disconnect", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			b := swarm.RandAddress(t)
			env := newHistEnv(t, histOpts{radius: 2, bins: 4})
			drain(env.p)
			env.cursors.setErr(b, errors.New("cursors failed"))
			env.kad.AddRevPeers(kadMock.AddrTuple{Addr: b, PO: 2})
			env.kad.Trigger()
			if !signalled(env.p) {
				t.Fatal("no signal when not started entries were added")
			}
			// The retry fails again: the set is unchanged.
			env.kad.Trigger()
			if signalled(env.p) {
				t.Fatal("signal from a recalculation that left the not started set unchanged")
			}
			env.kad.ResetPeers()
			env.kad.Trigger()
			if !signalled(env.p) {
				t.Fatal("no signal when a disconnect removed entries")
			}
		})
	})
}

// TestHistoricalLockOrder runs recalculations, worker exits and readers at
// the same time, for the race detector and to catch a deadlock between
// syncPeersMtx, peer.mtx and histMtx.
func TestHistoricalLockOrder(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		peers := make([]kadMock.AddrTuple, 0, 8)
		replies := make([]mockps.SyncReply, 0, 32)
		for i := range 8 {
			a := swarm.RandAddress(t)
			peers = append(peers, kadMock.AddrTuple{Addr: a, PO: uint8(i % 4)})
			replies = append(replies, finish(a, 0, 1, 2, 3)...)
		}
		env := newHistEnv(t, histOpts{radius: 1, bins: 4, peers: peers, replies: replies})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range 50 {
				_ = env.p.HistoricalSyncDone(1)
				time.Sleep(time.Second)
			}
		}()
		for r := range 4 {
			env.rs.SetStorageRadius(uint8(3 - r))
			env.kad.Trigger()
			time.Sleep(5 * time.Second)
		}
		<-done
	})
}
