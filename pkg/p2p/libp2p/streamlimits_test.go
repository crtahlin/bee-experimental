// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	golibp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	libp2ptest "github.com/libp2p/go-libp2p/core/test"
)

const (
	protoPullSync  = protocol.ID("/swarm/pullsync/1.4.0/pullsync")
	protoRetrieval = protocol.ID("/swarm/retrieval/1.4.0/retrieval")
)

var (
	testPeersMu sync.Mutex
	testPeers   = map[int]libp2ppeer.ID{}
)

// testPeer returns a valid peer ID, the same one for each index i.
func testPeer(t *testing.T, i int) libp2ppeer.ID {
	t.Helper()
	testPeersMu.Lock()
	defer testPeersMu.Unlock()
	if p, ok := testPeers[i]; ok {
		return p
	}
	p, err := libp2ptest.RandPeerID()
	if err != nil {
		t.Fatal(err)
	}
	testPeers[i] = p
	return p
}

// openNegotiated opens an inbound stream for p and negotiates proto.
func openNegotiated(rm network.ResourceManager, p libp2ppeer.ID, proto protocol.ID) (network.StreamManagementScope, error) {
	s, err := rm.OpenStream(p, network.DirInbound)
	if err != nil {
		return nil, err
	}
	if err := s.SetProtocol(proto); err != nil {
		s.Done()
		return nil, err
	}
	return s, nil
}

func newStack(t *testing.T, o libp2p.StreamLimitOptions, streamCap int, blocked ...protocol.ID) *libp2p.StreamLimitStack {
	t.Helper()
	o.Enabled = true
	s, err := libp2p.NewStreamLimitStack(o, streamCap, blocked...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.RM.Close() })
	return s
}

// assertReleased checks that every count is back at zero.
func assertReleased(t *testing.T, s *libp2p.StreamLimitStack) {
	t.Helper()
	st := s.State.Stat()
	if st.System.NumStreamsInbound != 0 || st.Transient.NumStreamsInbound != 0 {
		t.Fatalf("system %d, transient %d inbound streams held, want 0", st.System.NumStreamsInbound, st.Transient.NumStreamsInbound)
	}
	for p, ps := range st.Protocols {
		if ps.NumStreamsInbound != 0 {
			t.Fatalf("protocol %s holds %d inbound streams, want 0", p, ps.NumStreamsInbound)
		}
	}
	pull, other, unneg := s.Counts()
	if pull != 0 || other != 0 || unneg != 0 {
		t.Fatalf("groups pull %d other %d, un-negotiated peers %d, want 0", pull, other, unneg)
	}
}

func TestBuildStreamLimits(t *testing.T) {
	t.Parallel()

	t.Run("defaults", func(t *testing.T) {
		c, err := libp2p.BuildStreamLimits(libp2p.StreamLimitOptions{Enabled: true}, libp2p.IncomingStreamCountLimit)
		if err != nil {
			t.Fatal(err)
		}
		want := libp2p.StreamLimitsConfig{Enabled: true, PerPeer: 256, Transient: 1024, Unnegotiated: 64, PullSyncMax: 2976, OtherMax: 1976}
		if c != want {
			t.Fatalf("got %+v, want %+v", c, want)
		}
	})
	t.Run("off by default", func(t *testing.T) {
		c, err := libp2p.BuildStreamLimits(libp2p.StreamLimitOptions{}, libp2p.IncomingStreamCountLimit)
		if err != nil {
			t.Fatal(err)
		}
		if c.Enabled {
			t.Fatal("limits on without the switch")
		}
	})
	t.Run("each limit off", func(t *testing.T) {
		c, err := libp2p.BuildStreamLimits(libp2p.StreamLimitOptions{Enabled: true, PerPeer: -1, ReservePullSync: -1, ReserveOther: -1, Transient: -1, UnnegotiatedPerPeer: -1}, libp2p.IncomingStreamCountLimit)
		if err != nil {
			t.Fatal(err)
		}
		if c != (libp2p.StreamLimitsConfig{Enabled: true}) {
			t.Fatalf("got %+v, want every limit off", c)
		}
	})
	for name, o := range map[string]libp2p.StreamLimitOptions{
		"negative per peer":                {PerPeer: -2},
		"negative reserve":                 {ReservePullSync: -5},
		"negative transient":               {Transient: -3},
		"negative un-negotiated":           {UnnegotiatedPerPeer: -9},
		"negative while off":               {Enabled: false, ReserveOther: -2},
		"reserves and transient over cap":  {Enabled: true, ReservePullSync: 3000, ReserveOther: 1500, Transient: 500},
		"reserve on with transient off":    {Enabled: true, Transient: -1},
		"pull reserve on, transient off":   {Enabled: true, ReserveOther: -1, Transient: -1},
		"reserves equal to cap with trans": {Enabled: true, ReservePullSync: 2000, ReserveOther: 1976, Transient: 1024},
	} {
		t.Run("refused: "+name, func(t *testing.T) {
			if _, err := libp2p.BuildStreamLimits(o, libp2p.IncomingStreamCountLimit); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

// TestStreamLimitsOffBuildsTodaysLimits checks that with the switch off,
// or with every limit -1, the node builds the same limits as without
// this change.
func TestStreamLimitsOffBuildsTodaysLimits(t *testing.T) {
	t.Parallel()

	base, err := libp2p.BuiltLimits(libp2p.StreamLimitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	allOff, err := libp2p.BuiltLimits(libp2p.StreamLimitOptions{Enabled: true, PerPeer: -1, ReservePullSync: -1, ReserveOther: -1, Transient: -1, UnnegotiatedPerPeer: -1})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(base.ToPartialLimitConfig(), allOff.ToPartialLimitConfig()) {
		t.Fatal("limits with every stream limit off differ from today's")
	}
	on, err := libp2p.BuiltLimits(libp2p.StreamLimitOptions{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	p := on.ToPartialLimitConfig()
	if p.PeerDefault.StreamsInbound != 256 || p.Transient.StreamsInbound != 1024 {
		t.Fatalf("peer %v, transient %v inbound, want 256 and 1024", p.PeerDefault.StreamsInbound, p.Transient.StreamsInbound)
	}
}

func TestStreamLimitPerPeer(t *testing.T) {
	t.Parallel()

	s := newStack(t, libp2p.StreamLimitOptions{PerPeer: 4, ReservePullSync: -1, ReserveOther: -1, Transient: -1}, 100)
	a, b := testPeer(t, 0), testPeer(t, 1)
	held := make([]network.StreamManagementScope, 0, 300)
	for range 4 {
		sc, err := openNegotiated(s.RM, a, protoRetrieval)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, sc)
	}
	if _, err := s.RM.OpenStream(a, network.DirInbound); !errors.Is(err, network.ErrResourceLimitExceeded) {
		t.Fatalf("fifth stream of one peer: got %v, want a resource limit error", err)
	}
	if sc, err := openNegotiated(s.RM, b, protoRetrieval); err != nil {
		t.Fatalf("another peer refused: %v", err)
	} else {
		sc.Done()
	}
	held[0].Done()
	sc, err := openNegotiated(s.RM, a, protoRetrieval)
	if err != nil {
		t.Fatalf("stream after one closed: %v", err)
	}
	sc.Done()
	for _, h := range held[1:] {
		h.Done()
	}
	if got := s.TakeAttribution()[a]; got != libp2p.StreamReasonPeerLimit {
		t.Fatalf("attribution reason %q, want %q", got, libp2p.StreamReasonPeerLimit)
	}
	assertReleased(t, s)
}

// TestStreamLimitGroups fills one group to its limit and checks that the
// other group still gets its reserve, both ways round. cap 100, reserves
// 40 and 20, transient 10: other may hold 50, pull-sync 70.
func TestStreamLimitGroups(t *testing.T) {
	t.Parallel()

	o := libp2p.StreamLimitOptions{PerPeer: -1, ReservePullSync: 40, ReserveOther: 20, Transient: 10, UnnegotiatedPerPeer: -1}
	for _, tc := range []struct {
		name               string
		fill, protect      protocol.ID
		fillMax, reserve   int
		fillLimit, protLim string
	}{
		{"other full, pull-sync keeps its reserve", protoRetrieval, protoPullSync, 50, 40, libp2p.StreamLimitGroupOther, libp2p.StreamLimitGroupPullSync},
		{"pull-sync full, others keep their reserve", protoPullSync, protoRetrieval, 70, 20, libp2p.StreamLimitGroupPullSync, libp2p.StreamLimitGroupOther},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newStack(t, o, 100)
			held := make([]network.StreamManagementScope, 0, 300)
			for i := range tc.fillMax {
				sc, err := openNegotiated(s.RM, testPeer(t, i), tc.fill)
				if err != nil {
					t.Fatalf("stream %d of the filling group: %v", i, err)
				}
				held = append(held, sc)
			}
			if _, err := openNegotiated(s.RM, testPeer(t, 200), tc.fill); !errors.Is(err, libp2p.ErrStreamGroupFull) {
				t.Fatalf("stream over the group limit: got %v, want %v", err, libp2p.ErrStreamGroupFull)
			}
			if got := s.Refused(tc.fillLimit); got != 1 {
				t.Fatalf("refusals at %s: %v, want 1", tc.fillLimit, got)
			}
			for i := range tc.reserve {
				sc, err := openNegotiated(s.RM, testPeer(t, 300+i), tc.protect)
				if err != nil {
					t.Fatalf("protected stream %d of %d: %v", i, tc.reserve, err)
				}
				held = append(held, sc)
			}
			if got := s.Refused(tc.protLim); got != 0 {
				t.Fatalf("refusals at %s: %v, want 0", tc.protLim, got)
			}
			for _, h := range held {
				h.Done()
			}
			assertReleased(t, s)
		})
	}
}

// TestStreamLimitUnnegotiated checks the per-peer limit on silent
// streams: one peer is refused at it while another peer still opens and
// negotiates, and a slot is freed when a stream negotiates or closes.
func TestStreamLimitUnnegotiated(t *testing.T) {
	t.Parallel()

	s := newStack(t, libp2p.StreamLimitOptions{UnnegotiatedPerPeer: 3, Transient: 10, ReservePullSync: 20, ReserveOther: 20}, 100)
	a, b := testPeer(t, 0), testPeer(t, 1)
	silent := make([]network.StreamManagementScope, 0, 5)
	for range 3 {
		sc, err := s.RM.OpenStream(a, network.DirInbound)
		if err != nil {
			t.Fatal(err)
		}
		silent = append(silent, sc)
	}
	if _, err := s.RM.OpenStream(a, network.DirInbound); !errors.Is(err, libp2p.ErrUnnegotiatedPerPeer) {
		t.Fatalf("fourth silent stream: got %v, want %v", err, libp2p.ErrUnnegotiatedPerPeer)
	}
	if got := s.Refused(libp2p.StreamLimitPeerUnnegotiated); got != 1 {
		t.Fatalf("refusals: %v, want 1", got)
	}
	bs, err := openNegotiated(s.RM, b, protoPullSync)
	if err != nil {
		t.Fatalf("other peer's pull-sync refused: %v", err)
	}
	// Negotiating one silent stream frees its slot.
	if err := silent[0].SetProtocol(protoPullSync); err != nil {
		t.Fatal(err)
	}
	extra, err := s.RM.OpenStream(a, network.DirInbound)
	if err != nil {
		t.Fatalf("stream after a slot was freed by negotiation: %v", err)
	}
	// Closing a silent stream frees its slot too.
	silent[1].Done()
	extra2, err := s.RM.OpenStream(a, network.DirInbound)
	if err != nil {
		t.Fatalf("stream after a slot was freed by closing: %v", err)
	}
	if _, _, unneg := s.Scan(); unneg != 3 {
		t.Fatalf("un-negotiated high-water %v, want 3", unneg)
	}
	for _, sc := range []network.StreamManagementScope{silent[0], silent[2], extra, extra2, bs} {
		sc.Done()
	}
	assertReleased(t, s)
}

// TestStreamLimitTransient checks the node-wide transient limit: with
// enough peers holding silent streams, a new stream is refused at accept.
func TestStreamLimitTransient(t *testing.T) {
	t.Parallel()

	s := newStack(t, libp2p.StreamLimitOptions{UnnegotiatedPerPeer: 2, Transient: 5, ReservePullSync: 20, ReserveOther: 20}, 100)
	silent := make([]network.StreamManagementScope, 0, 5)
	for i := range 5 {
		sc, err := s.RM.OpenStream(testPeer(t, i/2), network.DirInbound)
		if err != nil {
			t.Fatalf("silent stream %d: %v", i, err)
		}
		silent = append(silent, sc)
	}
	if _, err := s.RM.OpenStream(testPeer(t, 9), network.DirInbound); !errors.Is(err, network.ErrResourceLimitExceeded) {
		t.Fatalf("stream over the transient limit: got %v, want a resource limit error", err)
	}
	if _, th, _ := s.Scan(); th != 5 {
		t.Fatalf("transient high-water %v, want 5", th)
	}
	silent[0].Done()
	sc, err := openNegotiated(s.RM, testPeer(t, 9), protoPullSync)
	if err != nil {
		t.Fatalf("pull-sync once transient room is free: %v", err)
	}
	sc.Done()
	for _, x := range silent[1:] {
		x.Done()
	}
	assertReleased(t, s)
}

// TestStreamLimitInnerSetProtocolRefused checks that a stream the inner
// resource manager refuses at negotiation gives its group place back.
func TestStreamLimitInnerSetProtocolRefused(t *testing.T) {
	t.Parallel()

	s := newStack(t, libp2p.StreamLimitOptions{}, 5000, protoRetrieval)
	if _, err := openNegotiated(s.RM, testPeer(t, 0), protoRetrieval); err == nil {
		t.Fatal("blocked protocol admitted")
	}
	assertReleased(t, s)
}

// TestStreamLimitOutboundNotCounted checks that outbound streams are
// neither counted nor refused by the wrapper.
func TestStreamLimitOutboundNotCounted(t *testing.T) {
	t.Parallel()

	s := newStack(t, libp2p.StreamLimitOptions{ReservePullSync: 40, ReserveOther: 20, Transient: 10, UnnegotiatedPerPeer: 1}, 100)
	held := make([]network.StreamManagementScope, 0, 300)
	for i := range 80 {
		sc, err := s.RM.OpenStream(testPeer(t, 0), network.DirOutbound)
		if err != nil {
			t.Fatalf("outbound stream %d: %v", i, err)
		}
		if err := sc.SetProtocol(protoPullSync); err != nil {
			t.Fatalf("outbound stream %d negotiation: %v", i, err)
		}
		held = append(held, sc)
	}
	if pull, other, unneg := s.Counts(); pull != 0 || other != 0 || unneg != 0 {
		t.Fatalf("outbound streams counted: pull %d other %d un-negotiated peers %d", pull, other, unneg)
	}
	for _, h := range held {
		h.Done()
	}
}

// TestStreamLimitDoneTwice checks that a second Done releases nothing.
func TestStreamLimitDoneTwice(t *testing.T) {
	t.Parallel()

	s := newStack(t, libp2p.StreamLimitOptions{}, 5000)
	a, err := openNegotiated(s.RM, testPeer(t, 0), protoPullSync)
	if err != nil {
		t.Fatal(err)
	}
	b, err := openNegotiated(s.RM, testPeer(t, 1), protoPullSync)
	if err != nil {
		t.Fatal(err)
	}
	a.Done()
	a.Done()
	if pull, _, _ := s.Counts(); pull != 1 {
		t.Fatalf("pull-sync group %d after a double Done, want 1", pull)
	}
	b.Done()
	assertReleased(t, s)
}

// TestStreamLimitParallel opens, negotiates and closes streams from many
// goroutines at once, checking the group counts never pass their limits
// and end at zero. Run with -race.
func TestStreamLimitParallel(t *testing.T) {
	t.Parallel()

	s := newStack(t, libp2p.StreamLimitOptions{PerPeer: 8, UnnegotiatedPerPeer: 4, Transient: 20, ReservePullSync: 60, ReserveOther: 40}, 200)
	// other may hold 200-60-20 = 120, pull-sync 200-40-20 = 140
	var wg sync.WaitGroup
	var mu sync.Mutex
	var over string
	for g := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			proto := protoRetrieval
			if g%2 == 0 {
				proto = protoPullSync
			}
			for i := range 200 {
				sc, err := s.RM.OpenStream(testPeer(t, g*7+i%7), network.DirInbound)
				if err != nil {
					continue
				}
				if err := sc.SetProtocol(proto); err == nil {
					pull, other, _ := s.Counts()
					if pull > 140 || other > 120 {
						mu.Lock()
						over = "group count over its limit"
						mu.Unlock()
					}
				}
				sc.Done()
			}
		}()
	}
	wg.Wait()
	if over != "" {
		t.Fatal(over)
	}
	assertReleased(t, s)
}

// TestStreamHighWater checks that the reporter keeps the highest per-peer
// count between scans, also while the limits are off, and notes a peer
// above the threshold for the attribution log.
func TestStreamHighWater(t *testing.T) {
	t.Parallel()

	s, err := libp2p.NewStreamLimitStack(libp2p.StreamLimitOptions{}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer s.RM.Close()
	p := testPeer(t, 3)
	held := make([]network.StreamManagementScope, 0, 300)
	for range 300 {
		sc, err := openNegotiated(s.RM, p, protoRetrieval)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, sc)
	}
	for _, h := range held {
		h.Done()
	}
	if ph, _, _ := s.Scan(); ph != 300 {
		t.Fatalf("per-peer high-water %v after the peer closed all, want 300", ph)
	}
	if ph, _, _ := s.Scan(); ph != 0 {
		t.Fatalf("per-peer high-water %v in the next period, want 0", ph)
	}
}

// TestStreamLimitRefusalReachesPeer runs the limits in a real libp2p host
// and checks that a peer over its limit sees the stream reset with the
// resource-limit code.
func TestStreamLimitRefusalReachesPeer(t *testing.T) {
	t.Parallel()

	stack := newStack(t, libp2p.StreamLimitOptions{PerPeer: 3, ReservePullSync: -1, ReserveOther: -1, Transient: -1}, 100)
	server, err := golibp2p.New(golibp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), golibp2p.ResourceManager(stack.RM))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	release := make(chan struct{})
	defer close(release)
	server.SetStreamHandler(protoRetrieval, func(s network.Stream) {
		<-release
		_ = s.Close()
	})

	client, err := golibp2p.New(golibp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Connect(ctx, libp2ppeer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}); err != nil {
		t.Fatal(err)
	}

	// The first three streams are held open by the handler.
	for i := range 3 {
		st, err := client.NewStream(ctx, server.ID(), protoRetrieval)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		if _, err := st.Write([]byte{1}); err != nil {
			t.Fatalf("stream %d write: %v", i, err)
		}
	}
	waitFor(t, func() bool { return stack.State.Stat().System.NumStreamsInbound == 3 })

	st, err := client.NewStream(ctx, server.ID(), protoRetrieval)
	if err == nil {
		_ = st.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err = st.Write([]byte{1}); err == nil {
			_, err = st.Read(make([]byte, 1))
		}
	}
	var se *network.StreamError
	if !errors.As(err, &se) || !se.Remote || se.ErrorCode != network.StreamResourceLimitExceeded {
		t.Fatalf("fourth stream: got %v, want a remote reset with the resource-limit code", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 5 s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
