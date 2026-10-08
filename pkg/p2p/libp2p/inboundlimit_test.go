// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

// fakeClock is a clock tests move by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// fakeResourceManager stands for the inner resource manager: it counts the
// connections it holds open, refuses every connection while refuse is set,
// and, when quota has an entry for an address, admits only that many from
// it, as the per-IP limits would.
type fakeResourceManager struct {
	*network.NullResourceManager
	mu     sync.Mutex
	open   int
	refuse bool
	quota  map[string]int
}

var errPerIP = errors.New("test: per-IP limit")

func (f *fakeResourceManager) OpenConnection(_ network.Direction, _ bool, endpoint ma.Multiaddr) (network.ConnManagementScope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse {
		return nil, errPerIP
	}
	if n, ok := f.quota[endpoint.String()]; ok {
		if n == 0 {
			return nil, errPerIP
		}
		f.quota[endpoint.String()] = n - 1
	}
	f.open++
	return &fakeScope{f: f}, nil
}

func (f *fakeResourceManager) setQuota(addr ma.Multiaddr, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.quota == nil {
		f.quota = make(map[string]int)
	}
	f.quota[addr.String()] = n
}

func (f *fakeResourceManager) held() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open
}

type fakeScope struct {
	*network.NullScope
	f *fakeResourceManager
}

func (s *fakeScope) Done() {
	s.f.mu.Lock()
	s.f.open--
	s.f.mu.Unlock()
}

func mustAddr(t *testing.T, s string) ma.Multiaddr {
	t.Helper()
	m, err := ma.NewMultiaddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func newTestLimiter(t *testing.T, rate float64, burst int) (libp2p.InboundLimiter, *fakeResourceManager, libp2p.KnownFullPeers, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	inner := &fakeResourceManager{NullResourceManager: &network.NullResourceManager{}}
	known := libp2p.NewKnownFullPeers(libp2p.KnownFullPeersMax, libp2p.KnownFullPeerTTL, clock.Now)
	return libp2p.NewInboundLimiter(inner, rate, burst, known, clock.Now), inner, known, clock
}

func admitN(t *testing.T, l libp2p.InboundLimiter, addr ma.Multiaddr, n int) (admitted int) {
	t.Helper()
	for range n {
		if _, err := l.OpenConnection(network.DirInbound, true, addr); err == nil {
			admitted++
		} else if !errors.Is(err, libp2p.ErrInboundRateLimited) && !errors.Is(err, errPerIP) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	return admitted
}

// TestInboundLimitBurstThenRate checks that the general bucket admits its
// burst, then its rate, and refuses beyond it.
func TestInboundLimitBurstThenRate(t *testing.T) {
	t.Parallel()

	l, _, _, clock := newTestLimiter(t, 2, 5)
	a := mustAddr(t, "/ip4/203.0.113.1/tcp/1634")

	if got := admitN(t, l, a, 8); got != 5 {
		t.Fatalf("got %d admitted at once, want the burst of 5", got)
	}
	clock.Add(time.Second)
	if got := admitN(t, l, a, 5); got != 2 {
		t.Fatalf("got %d admitted after one second, want the rate of 2", got)
	}
	if got := l.Refused(libp2p.InboundBucketGeneral); got != 6 {
		t.Fatalf("got %v refusals, want 6", got)
	}
	if got := l.Admitted(libp2p.InboundBucketGeneral); got != 7 {
		t.Fatalf("got %v admissions, want 7", got)
	}
}

// TestInboundLimitKnownFullBucket checks that a known full peer's key uses
// its own bucket and is admitted while the general bucket is empty.
func TestInboundLimitKnownFullBucket(t *testing.T) {
	t.Parallel()

	l, _, known, _ := newTestLimiter(t, 1, 2)
	unknown := mustAddr(t, "/ip4/203.0.113.1/tcp/1634")
	full := mustAddr(t, "/ip4/198.51.100.7/tcp/1634")
	known.Record(full)

	if got := admitN(t, l, unknown, 5); got != 2 {
		t.Fatalf("got %d unknown admitted, want 2", got)
	}
	if got := admitN(t, l, full, 2); got != 2 {
		t.Fatalf("got %d known full admitted while the general bucket is empty, want 2", got)
	}
	if got := l.Admitted(libp2p.InboundBucketKnownFull); got != 2 {
		t.Fatalf("got %v known-full admissions, want 2", got)
	}
}

// TestInboundLimitAfterPerIP checks that an attempt the inner resource
// manager refuses takes no token.
func TestInboundLimitAfterPerIP(t *testing.T) {
	t.Parallel()

	l, inner, _, _ := newTestLimiter(t, 1, 2)
	a := mustAddr(t, "/ip4/203.0.113.1/tcp/1634")

	inner.refuse = true
	if got := admitN(t, l, a, 10); got != 0 {
		t.Fatalf("got %d admitted while the inner manager refuses, want 0", got)
	}
	inner.refuse = false
	if got := admitN(t, l, a, 3); got != 2 {
		t.Fatalf("got %d admitted after the inner refusals, want the full burst of 2", got)
	}
}

// TestInboundLimitReleasesScope checks that a refused connection gives
// back what the inner resource manager counted for it.
func TestInboundLimitReleasesScope(t *testing.T) {
	t.Parallel()

	l, inner, _, _ := newTestLimiter(t, 1, 1)
	a := mustAddr(t, "/ip4/203.0.113.1/tcp/1634")

	admitN(t, l, a, 5)
	if got := inner.held(); got != 1 {
		t.Fatalf("inner manager holds %d connections, want only the admitted one", got)
	}
}

// TestInboundLimitPerKeyFairness checks that one flooding key, held to the
// per-IP rate by the inner manager, cannot starve a second key.
func TestInboundLimitPerKeyFairness(t *testing.T) {
	t.Parallel()

	l, inner, _, clock := newTestLimiter(t, 3, 3)
	flood := mustAddr(t, "/ip4/203.0.113.1/tcp/1634")
	other := mustAddr(t, "/ip4/203.0.113.2/tcp/1634")

	// The inner per-IP rate holds the flooding key to a third of the
	// bucket each second, so a second key still gets in.
	for range 5 {
		inner.setQuota(flood, 1)
		admitN(t, l, flood, 100)
		if got := admitN(t, l, other, 1); got != 1 {
			t.Fatal("second key starved by the first")
		}
		clock.Add(time.Second)
	}
}

// TestInboundLimitExemptions checks loopback and outbound connections.
func TestInboundLimitExemptions(t *testing.T) {
	t.Parallel()

	l, inner, _, _ := newTestLimiter(t, 1, 1)
	loop := mustAddr(t, "/ip4/127.0.0.1/tcp/1634")
	loop6 := mustAddr(t, "/ip6/::1/tcp/1634")
	remote := mustAddr(t, "/ip4/203.0.113.1/tcp/1634")

	if got := admitN(t, l, loop, 5); got != 5 {
		t.Fatalf("got %d IPv4 loopback admitted, want all 5", got)
	}
	if got := admitN(t, l, loop6, 5); got != 5 {
		t.Fatalf("got %d IPv6 loopback admitted, want all 5", got)
	}
	for range 5 {
		if _, err := l.OpenConnection(network.DirOutbound, true, remote); err != nil {
			t.Fatalf("outbound refused: %v", err)
		}
	}
	if got := admitN(t, l, remote, 2); got != 1 {
		t.Fatalf("got %d inbound admitted, want the burst of 1 untouched by loopback and outbound", got)
	}
	// 10 loopback, 5 outbound and 1 admitted inbound stay open in the
	// inner manager; the refused inbound one was given back.
	if got := inner.held(); got != 16 {
		t.Fatalf("inner manager holds %d connections, want 16", got)
	}
}

// TestInboundLimitAddressKey checks the key function.
func TestInboundLimitAddressKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ addr, want string }{
		{"/ip4/203.0.113.1/tcp/1634", "203.0.113.1/32"},
		{"/ip6/::ffff:203.0.113.1/tcp/1634", "203.0.113.1/32"},
		{"/ip6/2001:db8:1234:5678::1/tcp/1634", "2001:db8:1234:5600::/56"},
	} {
		key, ok := libp2p.AddressKey(mustAddr(t, tc.addr))
		if !ok || key.String() != tc.want {
			t.Errorf("%s: got %v (%v), want %s", tc.addr, key, ok, tc.want)
		}
	}
	if _, ok := libp2p.AddressKey(mustAddr(t, "/dns4/example.com/tcp/1634")); ok {
		t.Error("a name without an IP got a key")
	}
}

// TestInboundLimitSettings checks the setting semantics.
func TestInboundLimitSettings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		rate          float64
		burst         int
		bootnode, set bool
		want          libp2p.InboundLimitConfig
		wantErr       bool
	}{
		{name: "defaults", want: libp2p.InboundLimitConfig{Enabled: true, Rate: libp2p.DefaultInboundConnectionRate, Burst: libp2p.DefaultInboundConnectionBurst}},
		{name: "configured", rate: 2, burst: 10, set: true, want: libp2p.InboundLimitConfig{Enabled: true, Rate: 2, Burst: 10}},
		{name: "rate off", rate: -1, set: true},
		{name: "burst off alone", burst: -1, set: true},
		{name: "negative rate refused", rate: -0.5, wantErr: true},
		{name: "negative burst refused", burst: -2, wantErr: true},
		{name: "bootnode unset", bootnode: true},
		{name: "bootnode with only the burst set", burst: 50, bootnode: true, set: true, want: libp2p.InboundLimitConfig{Enabled: true, Rate: libp2p.DefaultInboundConnectionRate, Burst: 50}},
	} {
		got, err := libp2p.BuildInboundLimit(tc.rate, tc.burst, tc.bootnode, tc.set)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: got error %v, want error %v", tc.name, err, tc.wantErr)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// TestKnownFullPeersExpiry checks the 24-hour expiry, the refresh and the
// bound.
func TestKnownFullPeersExpiry(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	k := libp2p.NewKnownFullPeers(3, libp2p.KnownFullPeerTTL, clock.Now)
	stale := mustAddr(t, "/ip4/198.51.100.1/tcp/1634")
	refreshed := mustAddr(t, "/ip4/198.51.100.2/tcp/1634")
	k.Record(stale)
	k.Record(refreshed)

	clock.Add(libp2p.KnownFullPeerTTL - time.Hour)
	k.Record(refreshed) // as the hourly pass does for a connected peer
	clock.Add(time.Hour)

	staleKey, _ := libp2p.AddressKey(stale)
	refreshedKey, _ := libp2p.AddressKey(refreshed)
	if k.Contains(staleKey) {
		t.Fatal("key not seen for 24 hours still counts as known")
	}
	if !k.Contains(refreshedKey) {
		t.Fatal("refreshed key expired")
	}
	k.Sweep()
	if got := k.Len(); got != 1 {
		t.Fatalf("got %d keys after the sweep, want 1", got)
	}

	for i := range 10 {
		k.Record(mustAddr(t, fmt.Sprintf("/ip4/192.0.2.%d/tcp/1634", i+1)))
	}
	if got := k.Len(); got != 3 {
		t.Fatalf("got %d keys, want the bound of 3", got)
	}
}

// TestKnownFullPeersRefreshDoesNotAdd checks that a disconnect refreshes a
// known address but never adds one, so a full peer that kademlia refused
// and then disconnected is not recorded.
func TestKnownFullPeersRefreshDoesNotAdd(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	k := libp2p.NewKnownFullPeers(10, libp2p.KnownFullPeerTTL, clock.Now)
	unknown := mustAddr(t, "/ip4/198.51.100.1/tcp/1634")
	known := mustAddr(t, "/ip4/198.51.100.2/tcp/1634")

	k.Refresh(unknown)
	if got := k.Len(); got != 0 {
		t.Fatalf("refresh added an unknown address: %d keys", got)
	}

	k.Record(known)
	clock.Add(libp2p.KnownFullPeerTTL - time.Minute)
	k.Refresh(known)
	clock.Add(time.Hour)
	key, _ := libp2p.AddressKey(known)
	if !k.Contains(key) {
		t.Fatal("refresh did not extend a known address")
	}
}

// TestInboundLimitPrivateAfterRefusal checks the window of the warning.
func TestInboundLimitPrivateAfterRefusal(t *testing.T) {
	t.Parallel()

	l, _, _, clock := newTestLimiter(t, 1, 1)
	a := mustAddr(t, "/ip4/203.0.113.1/tcp/1634")

	if _, ok := l.PrivateAfterRefusal(); ok {
		t.Fatal("warning without any refusal")
	}
	admitN(t, l, a, 2)
	clock.Add(libp2p.RecentRefusalWindow - time.Second)
	if _, ok := l.PrivateAfterRefusal(); !ok {
		t.Fatal("no warning inside the window")
	}
	clock.Add(2 * time.Second)
	if _, ok := l.PrivateAfterRefusal(); ok {
		t.Fatal("warning after the window")
	}
}
