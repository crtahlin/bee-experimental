// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kademlia_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/addressbook"
	"github.com/ethersphere/bee/v2/pkg/bzz"
	beeCrypto "github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/discovery/mock"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/p2p"
	p2pmock "github.com/ethersphere/bee/v2/pkg/p2p/mock"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	"github.com/ethersphere/bee/v2/pkg/stabilization"
	mockstate "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/kademlia"
	"github.com/ethersphere/bee/v2/pkg/util/testutil"
	ma "github.com/multiformats/go-multiaddr"
)

// dialOutcome is what the flapping peer does on its n-th dial (from 1): fail
// with err, or accept and drop the connection after stay.
type dialOutcome struct {
	err  error
	stay time.Duration
}

// retrySnapshot is the peer's retry entry right after one of its
// connections ended.
type retrySnapshot struct {
	at         time.Time
	shortLived int
	attempts   int
	tryAfter   time.Time
}

// flapNet is a kademlia with one peer whose dials follow outcome.
type flapNet struct {
	kad    *kademlia.Kad
	ab     addressbook.Interface
	signer beeCrypto.Signer
	base   swarm.Address
	peer   swarm.Address

	done     chan struct{} // closed when the test ends
	stopOnce sync.Once
	drops    sync.WaitGroup // the goroutines that drop connections

	mu      sync.Mutex
	dials   []time.Time
	outcome func(n int) dialOutcome
	snaps   []retrySnapshot
}

func newFlapNet(t *testing.T, disc *mock.Discovery, timeToRetry time.Duration, outcome func(n int) dialOutcome) *flapNet {
	t.Helper()

	detector, err := stabilization.NewDetector(stabilization.Config{
		PeriodDuration:             time.Second,
		NumPeriodsForStabilization: 2,
		StabilizationFactor:        1,
	})
	if err != nil {
		t.Fatal(err)
	}

	pk, _ := beeCrypto.GenerateSecp256k1Key()
	f := &flapNet{
		ab:      addressbook.New(mockstate.NewStateStore()),
		signer:  beeCrypto.NewDefaultSigner(pk),
		base:    swarm.RandAddress(t),
		outcome: outcome,
		done:    make(chan struct{}),
	}
	f.peer = swarm.RandAddressAt(t, f.base, 1)

	p2ps := p2pmock.New(
		p2pmock.WithConnectFunc(func(_ context.Context, addrs []ma.Multiaddr) (*bzz.Address, error) {
			addr, _, err := f.ab.Get(f.peer)
			if err != nil || !bzz.AreUnderlaysEqual(addr.Underlays, addrs) {
				return nil, errors.New("unknown peer")
			}
			f.mu.Lock()
			f.dials = append(f.dials, time.Now())
			o := f.outcome(len(f.dials))
			f.mu.Unlock()
			if o.err != nil {
				return nil, o.err
			}
			f.drops.Go(func() {
				select {
				case <-time.After(o.stay):
				case <-f.done:
					return
				}
				f.kad.Disconnected(p2p.Peer{Address: f.peer})
				ta, _ := f.kad.RetryWaitTryAfter(f.peer)
				f.mu.Lock()
				f.snaps = append(f.snaps, retrySnapshot{
					at:         time.Now(),
					shortLived: f.kad.RetryWaitShortLived(f.peer),
					attempts:   f.kad.RetryWaitAttempts(f.peer),
					tryAfter:   ta,
				})
				f.mu.Unlock()
			})
			return addr, nil
		}),
		p2pmock.WithDisconnectFunc(func(swarm.Address, string) error { return nil }),
	)

	f.kad, err = kademlia.New(f.base, f.ab, disc, p2ps, detector, log.Noop, kademlia.Options{
		TimeToRetry: new(timeToRetry),
	})
	if err != nil {
		t.Fatal(err)
	}
	p2ps.SetPickyNotifier(f.kad)
	f.kad.SetStorageRadius(0)

	if err := f.kad.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	testutil.CleanupCloser(t, f.kad)

	// Run a manage pass often, so a dial happens as soon as the peer's
	// wait ends rather than at the loop's own interval.
	t.Cleanup(f.stop)
	f.drops.Go(func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-f.done:
				return
			case <-ticker.C:
				f.kad.Trigger()
			}
		}
	})

	return f
}

// stop ends the manage-pass trigger and the goroutines that drop
// connections, and waits for them, so counters and snapshots read after it
// agree.
func (f *flapNet) stop() {
	f.stopOnce.Do(func() { close(f.done) })
	f.drops.Wait()
}

// announce puts the peer into the address book and tells kademlia about it.
func (f *flapNet) announce(t *testing.T) {
	t.Helper()
	addOne(t, f.signer, f.kad, f.ab, f.peer)
}

func (f *flapNet) dialTimes() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.dials...)
}

func (f *flapNet) snapshots() []retrySnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]retrySnapshot(nil), f.snaps...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if err := spinlock.Wait(10*time.Second, cond); err != nil {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestShortLivedConnectionsBackOff checks that a peer that accepts every
// dial and drops the connection at once is dialled with growing gaps, and
// that each such connection is counted once. Before the change, every
// accepted dial reset the failure count and the peer was dialled again
// after TimeToRetry, at a steady rate.
func TestShortLivedConnectionsBackOff(t *testing.T) {
	kademlia.SetStableConnection(t, time.Second)
	kademlia.SetMaxShortLivedBackoff(t, 800*time.Millisecond)

	f := newFlapNet(t, mock.NewDiscovery(), 100*time.Millisecond, func(int) dialOutcome {
		return dialOutcome{stay: 30 * time.Millisecond}
	})
	f.announce(t)

	time.Sleep(3 * time.Second)
	f.stop()

	dials := f.dialTimes()
	// With waits of 100, 200, 400, 800, 800 ms the peer is dialled about
	// 6 times in 3 s; at a steady 100 ms it would be dialled about 20.
	if len(dials) < 3 || len(dials) > 10 {
		t.Fatalf("got %d dials in 3s, want 3 to 10", len(dials))
	}
	first := dials[1].Sub(dials[0])
	last := dials[len(dials)-1].Sub(dials[len(dials)-2])
	t.Logf("%d dials in 3s, first gap %v, last gap %v", len(dials), first, last)
	if last < 3*first {
		t.Fatalf("gaps did not grow: first %v, last %v", first, last)
	}

	snaps := f.snapshots()
	if got := f.kad.ShortLivedConnections(); int(got) != len(snaps) {
		t.Fatalf("short-lived counter %v, want one per ended connection (%d)", got, len(snaps))
	}
	for i, s := range snaps {
		if s.shortLived != i+1 {
			t.Fatalf("connection %d: short-lived count %d, want %d", i+1, s.shortLived, i+1)
		}
	}
}

// TestFailedAnnouncementCountsOnce checks that a peer whose announcement
// fails, which kademlia disconnects inside connect, counts one short-lived
// connection, so its next wait is TimeToRetry and not twice that.
func TestFailedAnnouncementCountsOnce(t *testing.T) {
	kademlia.SetStableConnection(t, time.Minute)

	var flapping swarm.Address
	disc := mock.NewDiscovery(mock.WithBroadcastPeers(func(_ context.Context, addressee swarm.Address, _ ...swarm.Address) error {
		if addressee.Equal(flapping) {
			return errors.New("peer gone during the announcement")
		}
		return nil
	}))

	const timeToRetry = time.Minute
	f := newFlapNet(t, disc, timeToRetry, func(int) dialOutcome {
		return dialOutcome{stay: time.Hour} // dropped by the failed announcement instead
	})
	flapping = f.peer

	// A connected peer, so that the announcement to the flapping peer has
	// something to send.
	connectOne(t, f.signer, f.kad, f.ab, swarm.RandAddressAt(t, f.base, 1), nil)
	f.announce(t)

	waitFor(t, "the short-lived connection", func() bool { return f.kad.RetryWaitShortLived(f.peer) == 1 })
	dials := f.dialTimes()
	ta, ok := f.kad.RetryWaitTryAfter(f.peer)
	if !ok {
		t.Fatal("no retry entry")
	}
	if wait := ta.Sub(dials[0]); wait < timeToRetry || wait > timeToRetry+timeToRetry/2 {
		t.Fatalf("next dial waits %v after the dial, want about %v (counted once)", wait, timeToRetry)
	}
	if got := f.kad.ShortLivedConnections(); got != 1 {
		t.Fatalf("short-lived counter %v, want 1", got)
	}

	// libp2p's own handler calls Disconnected again for the same
	// connection; that call must count nothing.
	f.kad.Disconnected(p2p.Peer{Address: f.peer})
	if got := f.kad.ShortLivedConnections(); got != 1 {
		t.Fatalf("short-lived counter after a second disconnect %v, want 1", got)
	}
	if got := f.kad.RetryWaitShortLived(f.peer); got != 1 {
		t.Fatalf("short-lived count after a second disconnect %d, want 1", got)
	}
}

// TestPrunedPeerKeepsItsWait checks that a peer that alternates two failed
// dials and one short connection is pruned from the address book, and that
// when it is announced again at once it is not dialled before the wait set
// at pruning.
func TestPrunedPeerKeepsItsWait(t *testing.T) {
	kademlia.SetStableConnection(t, time.Second)

	f := newFlapNet(t, mock.NewDiscovery(), 200*time.Millisecond, func(n int) dialOutcome {
		if n%3 == 0 {
			return dialOutcome{stay: 30 * time.Millisecond}
		}
		return dialOutcome{err: errors.New("connection refused")}
	})
	f.announce(t)

	waitFor(t, "pruning", func() bool {
		_, _, err := f.ab.Get(f.peer)
		return errors.Is(err, addressbook.ErrNotFound)
	})
	prunedAfter := len(f.dialTimes())

	if got := f.kad.RetryWaitShortLived(f.peer); got < 1 {
		t.Fatalf("pruned peer lost its short-lived count: %d", got)
	}
	if got := f.kad.RetryWaitAttempts(f.peer); got != 0 {
		t.Fatalf("pruned peer kept %d failed attempts, want 0", got)
	}
	ta, ok := f.kad.RetryWaitTryAfter(f.peer)
	if !ok || !ta.After(time.Now()) {
		t.Fatal("pruned peer has no wait running")
	}

	f.announce(t)
	time.Sleep(time.Until(ta) - 50*time.Millisecond)
	if got := len(f.dialTimes()); got != prunedAfter {
		t.Fatalf("re-announced peer dialled %d times before its wait ended", got-prunedAfter)
	}
	waitFor(t, "the dial after the wait", func() bool { return len(f.dialTimes()) > prunedAfter })
	if first := f.dialTimes()[prunedAfter]; first.Before(ta) {
		t.Fatalf("dialled %v before the wait ended", ta.Sub(first))
	}
}

// TestBreakerRefusalKeepsCounts checks that a dial refused by the
// connection breaker neither increases nor resets the failure count, and
// keeps the short-lived count.
func TestBreakerRefusalKeepsCounts(t *testing.T) {
	kademlia.SetStableConnection(t, time.Second)

	breakerUntil := time.Now().Add(time.Hour)
	f := newFlapNet(t, mock.NewDiscovery(), 100*time.Millisecond, func(n int) dialOutcome {
		switch n {
		case 1:
			return dialOutcome{err: errors.New("connection refused")}
		case 2:
			return dialOutcome{stay: 30 * time.Millisecond}
		default:
			return dialOutcome{err: p2p.NewConnectionBackoffError(errors.New("breaker open"), breakerUntil)}
		}
	})
	f.announce(t)

	waitFor(t, "the breaker refusal", func() bool {
		ta, _ := f.kad.RetryWaitTryAfter(f.peer)
		return !ta.Before(breakerUntil)
	})
	if got := f.kad.RetryWaitAttempts(f.peer); got != 1 {
		t.Fatalf("failed attempts after a breaker refusal: got %d, want 1", got)
	}
	if got := f.kad.RetryWaitShortLived(f.peer); got != 1 {
		t.Fatalf("short-lived count after a breaker refusal: got %d, want 1", got)
	}
}

// TestStableConnectionResetsCounts checks that a connection that lasts past
// the threshold resets both counts, and the peer is retried after
// TimeToRetry.
func TestStableConnectionResetsCounts(t *testing.T) {
	kademlia.SetStableConnection(t, 200*time.Millisecond)

	const timeToRetry = 100 * time.Millisecond
	f := newFlapNet(t, mock.NewDiscovery(), timeToRetry, func(n int) dialOutcome {
		switch n {
		case 1:
			return dialOutcome{err: errors.New("connection refused")}
		case 2:
			return dialOutcome{stay: 30 * time.Millisecond}
		default:
			return dialOutcome{stay: 400 * time.Millisecond}
		}
	})
	f.announce(t)

	waitFor(t, "the stable connection to end", func() bool { return len(f.snapshots()) >= 2 })
	f.stop()
	s := f.snapshots()[1]
	if s.shortLived != 0 || s.attempts != 0 {
		t.Fatalf("after a stable connection: short-lived %d attempts %d, want 0 and 0", s.shortLived, s.attempts)
	}
	if wait := s.tryAfter.Sub(s.at); wait > timeToRetry {
		t.Fatalf("next dial waits %v after a stable connection, want at most %v", wait, timeToRetry)
	}
	// The short connection before it was counted, so the counter shows
	// that the stable one was not.
	if got := f.kad.ShortLivedConnections(); got != 1 {
		t.Fatalf("short-lived counter %v, want 1", got)
	}
}
