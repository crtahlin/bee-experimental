// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	"github.com/ethersphere/bee/v2/pkg/spinlock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// announceRecorder counts Announce and AnnounceTo calls per overlay.
type announceRecorder struct {
	mu         sync.Mutex
	announce   map[string]int
	announceTo map[string]int
	fail       int // number of Announce calls that return an error
}

func newAnnounceRecorder() *announceRecorder {
	return &announceRecorder{announce: map[string]int{}, announceTo: map[string]int{}}
}

func (r *announceRecorder) Announce(_ context.Context, peer swarm.Address, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.announce[peer.ByteString()]++
	if r.fail > 0 {
		r.fail--
		return errors.New("test: announce refused")
	}
	return nil
}

func (r *announceRecorder) AnnounceTo(_ context.Context, addressee, _ swarm.Address, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.announceTo[addressee.ByteString()]++
	return nil
}

func (r *announceRecorder) announced(peer swarm.Address) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.announce[peer.ByteString()]
}

func (r *announceRecorder) announcedTo(peer swarm.Address) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.announceTo[peer.ByteString()]
}

func expectCount(t *testing.T, what string, got func() int, want int) {
	t.Helper()
	var n int
	if err := spinlock.Wait(10*time.Second, func() bool {
		n = got()
		return n == want
	}); err != nil {
		t.Fatalf("%s: got %d, want %d", what, n, want)
	}
}

// connectLight connects a light node to addr and waits until the full node
// holds it.
func connectLight(t *testing.T, sf, sl *libp2p.Service, overlay swarm.Address) {
	t.Helper()
	if _, err := sl.Connect(t.Context(), serviceUnderlayAddress(t, sf)); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf, overlay)
}

// TestLightAnnouncementOncePerInterval checks that a light peer that
// reconnects soon after an announcement gets no new one.
func TestLightAnnouncementOncePerInterval(t *testing.T) {
	t.Parallel()

	rec := newAnnounceRecorder()
	sf, _ := newLightLimitedNode(t, 10, mockAnnouncingNotifier(rec.Announce, rec.AnnounceTo), libp2p.Options{})
	sl, overlay := newLightNode(t)

	connectLight(t, sf, sl, overlay)
	expectCount(t, "announcements", func() int { return rec.announced(overlay) }, 1)

	if err := sf.Disconnect(overlay, "test: reconnect"); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf)
	expectPeersEventually(t, sl)
	connectLight(t, sf, sl, overlay)

	expectCount(t, "skipped announcements", func() int { return int(sf.LightAnnouncementsSkipped()) }, 1)
	if got := rec.announced(overlay); got != 1 {
		t.Fatalf("got %d announcements, want 1", got)
	}
}

// TestLightAnnouncementBootnodeNotGated checks that a bootnode announces to
// a light peer on every connection: light clients get their first peer list
// from bootnodes, and bootnode mode keeps today's behaviour.
func TestLightAnnouncementBootnodeNotGated(t *testing.T) {
	t.Parallel()

	rec := newAnnounceRecorder()
	sf, _ := newLightLimitedNode(t, 10, mockAnnouncingNotifier(rec.Announce, rec.AnnounceTo), libp2p.Options{BootnodeMode: true})
	sl, overlay := newLightNode(t)

	connectLight(t, sf, sl, overlay)
	expectCount(t, "announcements", func() int { return rec.announced(overlay) }, 1)

	if err := sf.Disconnect(overlay, "test: reconnect"); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf)
	expectPeersEventually(t, sl)
	connectLight(t, sf, sl, overlay)

	expectCount(t, "announcements", func() int { return rec.announced(overlay) }, 2)
	if got := sf.LightAnnouncementsSkipped(); got != 0 {
		t.Fatalf("got %v skipped announcements on a bootnode, want 0", got)
	}
}

// TestLightAnnouncementFailureNotRecorded checks that a failed announcement
// does not suppress the next one.
func TestLightAnnouncementFailureNotRecorded(t *testing.T) {
	t.Parallel()

	rec := newAnnounceRecorder()
	rec.fail = 1
	sf, _ := newLightLimitedNode(t, 10, mockAnnouncingNotifier(rec.Announce, rec.AnnounceTo), libp2p.Options{})
	sl, overlay := newLightNode(t)

	connectLight(t, sf, sl, overlay)
	expectCount(t, "announcements", func() int { return rec.announced(overlay) }, 1)

	if err := sf.Disconnect(overlay, "test: reconnect"); err != nil {
		t.Fatal(err)
	}
	expectPeersEventually(t, sf)
	expectPeersEventually(t, sl)
	connectLight(t, sf, sl, overlay)

	expectCount(t, "announcements", func() int { return rec.announced(overlay) }, 2)
	if got := sf.LightAnnouncementsSkipped(); got != 0 {
		t.Fatalf("got %v skipped announcements, want 0", got)
	}
}

// TestLightAnnouncementRefusedPeer checks that a light peer refused at the
// light limit gets no announcement.
func TestLightAnnouncementRefusedPeer(t *testing.T) {
	t.Parallel()

	rec := newAnnounceRecorder()
	sf, _ := newLightLimitedNode(t, 1, mockAnnouncingNotifier(rec.Announce, rec.AnnounceTo), libp2p.Options{})

	first, firstOverlay := newLightNode(t)
	connectLight(t, sf, first, firstOverlay)

	second, secondOverlay := newLightNode(t)
	_, _ = second.Connect(t.Context(), serviceUnderlayAddress(t, sf))
	expectPeersEventually(t, second)
	expectPeersEventually(t, sf, firstOverlay)

	if got := rec.announced(secondOverlay); got != 0 {
		t.Fatalf("refused peer got %d announcements, want 0", got)
	}
}

// TestLightAnnounceToNotGated checks that a light peer that has just
// received an announcement still hears about a new full peer.
func TestLightAnnounceToNotGated(t *testing.T) {
	t.Parallel()

	rec := newAnnounceRecorder()
	sf, _ := newLightLimitedNode(t, 10, mockAnnouncingNotifier(rec.Announce, rec.AnnounceTo), libp2p.Options{})
	sl, lightOverlay := newLightNode(t)
	connectLight(t, sf, sl, lightOverlay)
	expectCount(t, "announcements", func() int { return rec.announced(lightOverlay) }, 1)

	full, _ := newService(t, 1, libp2pServiceOpts{
		notifier:   mockNotifier(noopCf, noopDf, true),
		libp2pOpts: libp2p.Options{FullNode: true},
	})
	if _, err := full.Connect(t.Context(), serviceUnderlayAddress(t, sf)); err != nil {
		t.Fatal(err)
	}

	expectCount(t, "announcements to the light peer", func() int { return rec.announcedTo(lightOverlay) }, 1)
}

// TestLightAnnounceGate checks the interval and the bound of the gate.
func TestLightAnnounceGate(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_000_000, 0)
	g := libp2p.NewLightAnnounceGate(3, libp2p.LightAnnounceInterval, func() time.Time { return now })

	peer := swarm.RandAddress(t)
	if g.Recent(peer) {
		t.Fatal("unknown peer reported as recent")
	}
	g.Record(peer)

	now = now.Add(libp2p.LightAnnounceInterval - time.Second)
	if !g.Recent(peer) {
		t.Fatal("peer announced within the interval not reported as recent")
	}
	now = now.Add(time.Second)
	if g.Recent(peer) {
		t.Fatal("peer announced a full interval ago reported as recent")
	}

	for range 10 {
		g.Record(swarm.RandAddress(t))
	}
	if got := g.Len(); got != 3 {
		t.Fatalf("got %d remembered peers, want the bound of 3", got)
	}
	if libp2p.LightAnnounceMaxPeers != 10_000 || libp2p.LightAnnounceInterval != 10*time.Minute {
		t.Fatalf("got bound %d and interval %v, want 10000 and 10m", libp2p.LightAnnounceMaxPeers, libp2p.LightAnnounceInterval)
	}
}
