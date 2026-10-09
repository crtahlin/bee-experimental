// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

// Built-in values of the inbound stream limits. They apply only while
// p2p-inbound-stream-limits is on, which it is not by default.
const (
	defaultStreamsPerPeer          = 256
	defaultStreamReservePullSync   = 2_000
	defaultStreamReserveOther      = 1_000
	defaultStreamsTransient        = 1_024
	defaultStreamsUnnegotiatedPeer = 64

	// streamLimitOff is the setting value that turns one limit off.
	streamLimitOff = -1
)

// pullSyncProtocolPrefix starts the protocol ID of every pull-sync
// stream (pullsync and cursors). It is built the way
// p2p.NewSwarmStreamName builds protocol IDs.
const pullSyncProtocolPrefix = "/swarm/pullsync/"

// Protocol groups and the labels of their metrics.
const (
	streamGroupNone = iota
	streamGroupPullSync
	streamGroupOther
)

// Limit labels of the stream refusal counter and the attribution log.
const (
	streamLimitPeerUnnegotiated = "peer_unnegotiated"
	streamLimitGroupPullSync    = "group_pullsync"
	streamLimitGroupOther       = "group_other"
)

var (
	errUnnegotiatedPerPeer = fmt.Errorf("too many un-negotiated inbound streams from one peer: %w", network.ErrResourceLimitExceeded)
	errStreamGroupFull     = fmt.Errorf("inbound stream group full: %w", network.ErrResourceLimitExceeded)
)

// StreamLimitOptions are the inbound stream limit settings as the
// operator gave them: zero uses the built-in value and -1 turns that
// limit off.
type StreamLimitOptions struct {
	Enabled             bool
	PerPeer             int
	ReservePullSync     int
	ReserveOther        int
	Transient           int
	UnnegotiatedPerPeer int
}

// streamLimitsConfig is the resolved configuration. A zero limit means
// that limit does not apply.
type streamLimitsConfig struct {
	enabled      bool
	perPeer      int
	transient    int
	unnegotiated int
	// pullSyncMax and otherMax are the most negotiated inbound streams
	// each group may hold.
	pullSyncMax int
	otherMax    int
}

// resolveStreamLimit applies the setting convention to one value.
func resolveStreamLimit(name string, v, def int) (int, error) {
	switch {
	case v == 0:
		return def, nil
	case v == streamLimitOff:
		return 0, nil
	case v < 0:
		return 0, fmt.Errorf("%s: %d is not allowed: use 0 for the default or -1 to turn the limit off", name, v)
	}
	return v, nil
}

// buildStreamLimits validates the settings and resolves them against the
// node-wide inbound stream cap. The values are validated even while the
// limits are off, so a bad value is found before it is switched on.
func buildStreamLimits(o StreamLimitOptions, streamCap int) (streamLimitsConfig, error) {
	perPeer, err := resolveStreamLimit("p2p-inbound-streams-per-peer", o.PerPeer, defaultStreamsPerPeer)
	if err != nil {
		return streamLimitsConfig{}, err
	}
	reservePull, err := resolveStreamLimit("p2p-inbound-stream-reserve-pullsync", o.ReservePullSync, defaultStreamReservePullSync)
	if err != nil {
		return streamLimitsConfig{}, err
	}
	reserveOther, err := resolveStreamLimit("p2p-inbound-stream-reserve-other", o.ReserveOther, defaultStreamReserveOther)
	if err != nil {
		return streamLimitsConfig{}, err
	}
	transient, err := resolveStreamLimit("p2p-inbound-streams-transient", o.Transient, defaultStreamsTransient)
	if err != nil {
		return streamLimitsConfig{}, err
	}
	unnegotiated, err := resolveStreamLimit("p2p-inbound-streams-unnegotiated-per-peer", o.UnnegotiatedPerPeer, defaultStreamsUnnegotiatedPeer)
	if err != nil {
		return streamLimitsConfig{}, err
	}
	if !o.Enabled {
		return streamLimitsConfig{}, nil
	}
	if (reservePull > 0 || reserveOther > 0) && transient == 0 {
		return streamLimitsConfig{}, errors.New("p2p-inbound-streams-transient: the transient limit cannot be off while a stream reserve is on, because the reserves then give no guarantee")
	}
	if reservePull+reserveOther+transient >= streamCap {
		return streamLimitsConfig{}, fmt.Errorf("inbound stream limits: the pull-sync reserve (%d), the other reserve (%d) and the transient limit (%d) must add up to less than the inbound stream cap (%d)", reservePull, reserveOther, transient, streamCap)
	}

	c := streamLimitsConfig{enabled: true, perPeer: perPeer, transient: transient, unnegotiated: unnegotiated}
	if reserveOther > 0 {
		c.pullSyncMax = streamCap - reserveOther - transient
	}
	if reservePull > 0 {
		c.otherMax = streamCap - reservePull - transient
	}
	return c, nil
}

// apply sets the resource-manager part of the limits: the per-peer and
// the transient inbound stream limits. Build fills every field left at
// zero from the infinite defaults.
func (c streamLimitsConfig) apply(cfg *rcmgr.PartialLimitConfig) {
	if !c.enabled {
		return
	}
	if c.perPeer > 0 {
		cfg.PeerDefault.StreamsInbound = rcmgr.LimitVal(c.perPeer)
	}
	if c.transient > 0 {
		cfg.Transient.StreamsInbound = rcmgr.LimitVal(c.transient)
	}
}

// wrapperNeeded reports whether the wrapper has a limit to enforce.
func (c streamLimitsConfig) wrapperNeeded() bool {
	return c.enabled && (c.unnegotiated > 0 || c.pullSyncMax > 0 || c.otherMax > 0)
}

// streamGroupOf returns the group of a negotiated protocol.
func streamGroupOf(proto protocol.ID) int {
	if strings.HasPrefix(string(proto), pullSyncProtocolPrefix) {
		return streamGroupPullSync
	}
	return streamGroupOther
}

// streamLimits holds the counts the wrapper enforces: un-negotiated
// inbound streams per peer, and negotiated inbound streams per group.
type streamLimits struct {
	cfg     streamLimitsConfig
	metrics metrics
	attr    *streamAttribution

	mu           sync.Mutex
	unnegotiated map[libp2ppeer.ID]int
	unnegMax     int
	group        [3]int
}

func newStreamLimits(cfg streamLimitsConfig, m metrics, attr *streamAttribution) *streamLimits {
	for _, l := range []string{streamLimitPeerUnnegotiated, streamLimitGroupPullSync, streamLimitGroupOther} {
		m.InboundStreamsRefused.WithLabelValues(l)
	}
	m.InboundStreamsGroup.WithLabelValues("pullsync").Set(0)
	m.InboundStreamsGroup.WithLabelValues("other").Set(0)
	return &streamLimits{
		cfg:          cfg,
		metrics:      m,
		attr:         attr,
		unnegotiated: make(map[libp2ppeer.ID]int),
	}
}

// refuse counts a refusal and notes the peer for the attribution log.
func (l *streamLimits) refuse(p libp2ppeer.ID, limit string) {
	l.metrics.InboundStreamsRefused.WithLabelValues(limit).Inc()
	l.attr.note(p, limit, 0)
}

// reserveUnnegotiated takes one un-negotiated slot of peer p. It reports
// whether the stream holds a slot and whether it is admitted.
func (l *streamLimits) reserveUnnegotiated(p libp2ppeer.ID) (holds, ok bool) {
	if l.cfg.unnegotiated == 0 {
		return false, true
	}
	l.mu.Lock()
	n := l.unnegotiated[p] + 1
	if n > l.cfg.unnegotiated {
		l.mu.Unlock()
		l.refuse(p, streamLimitPeerUnnegotiated)
		return false, false
	}
	l.unnegotiated[p] = n
	if n > l.unnegMax {
		l.unnegMax = n
	}
	l.mu.Unlock()
	return true, true
}

func (l *streamLimits) releaseUnnegotiated(p libp2ppeer.ID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := l.unnegotiated[p] - 1; n > 0 {
		l.unnegotiated[p] = n
	} else {
		delete(l.unnegotiated, p)
	}
}

// takeUnnegotiatedMax returns the largest un-negotiated count of one peer
// since the previous call and starts a new period at the current largest.
func (l *streamLimits) takeUnnegotiatedMax() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := l.unnegMax
	l.unnegMax = 0
	for _, n := range l.unnegotiated {
		l.unnegMax = max(l.unnegMax, n)
	}
	return m
}

func (l *streamLimits) groupMax(g int) int {
	if g == streamGroupPullSync {
		return l.cfg.pullSyncMax
	}
	return l.cfg.otherMax
}

// reserveGroup counts one negotiated stream in group g, unless that
// passes the group's limit.
func (l *streamLimits) reserveGroup(g int) bool {
	l.mu.Lock()
	n := l.group[g] + 1
	if limit := l.groupMax(g); limit > 0 && n > limit {
		l.mu.Unlock()
		return false
	}
	l.group[g] = n
	l.mu.Unlock()
	l.setGroupGauge(g, n)
	return true
}

func (l *streamLimits) releaseGroup(g int) {
	l.mu.Lock()
	l.group[g]--
	n := l.group[g]
	l.mu.Unlock()
	l.setGroupGauge(g, n)
}

func (l *streamLimits) setGroupGauge(g, n int) {
	label := "other"
	if g == streamGroupPullSync {
		label = "pullsync"
	}
	l.metrics.InboundStreamsGroup.WithLabelValues(label).Set(float64(n))
}

// limitedStreamScope wraps the scope of one inbound stream. It holds the
// peer's un-negotiated slot until the stream negotiates a protocol, then
// its place in that protocol's group until it closes.
type limitedStreamScope struct {
	network.StreamManagementScope
	limits *streamLimits
	peer   libp2ppeer.ID

	mu        sync.Mutex
	holdsSlot bool
	group     int
	done      bool
}

// SetProtocol counts the stream in its protocol group before the inner
// scope moves it there: a refused stream never reaches the protocol
// scope, and a stream the inner scope refuses gives its place back.
func (s *limitedStreamScope) SetProtocol(proto protocol.ID) error {
	g := streamGroupOf(proto)
	if !s.limits.reserveGroup(g) {
		limit := streamLimitGroupOther
		if g == streamGroupPullSync {
			limit = streamLimitGroupPullSync
		}
		s.limits.refuse(s.peer, limit)
		return errStreamGroupFull
	}
	if err := s.StreamManagementScope.SetProtocol(proto); err != nil {
		s.limits.releaseGroup(g)
		return err
	}

	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		s.limits.releaseGroup(g)
		return network.ErrResourceScopeClosed
	}
	s.group = g
	slot := s.holdsSlot
	s.holdsSlot = false
	s.mu.Unlock()
	if slot {
		s.limits.releaseUnnegotiated(s.peer)
	}
	return nil
}

// Done releases the inner scope, then the slot and the group place, each
// once.
func (s *limitedStreamScope) Done() {
	s.StreamManagementScope.Done()

	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	s.done = true
	slot, g := s.holdsSlot, s.group
	s.holdsSlot = false
	s.group = streamGroupNone
	s.mu.Unlock()

	if slot {
		s.limits.releaseUnnegotiated(s.peer)
	}
	if g != streamGroupNone {
		s.limits.releaseGroup(g)
	}
}

// OpenStream applies the per-peer limit on un-negotiated inbound streams
// before the inner resource manager admits the stream, and wraps its
// scope so the protocol groups apply at negotiation. Outbound streams,
// and every stream while the limits are off, pass through unchanged.
func (l *inboundLimiter) OpenStream(p libp2ppeer.ID, dir network.Direction) (network.StreamManagementScope, error) {
	lim := l.streams
	if lim == nil || dir != network.DirInbound {
		return l.ResourceManager.OpenStream(p, dir)
	}
	holds, ok := lim.reserveUnnegotiated(p)
	if !ok {
		return nil, errUnnegotiatedPerPeer
	}
	scope, err := l.ResourceManager.OpenStream(p, dir)
	if err != nil {
		if holds {
			lim.releaseUnnegotiated(p)
		}
		return nil, err
	}
	return &limitedStreamScope{StreamManagementScope: scope, limits: lim, peer: p, holdsSlot: holds}, nil
}

// streamNote is what the attribution log says about one peer: why it
// was noted, the highest inbound stream count seen meanwhile, and its
// inbound streams by protocol, read as soon as it was noted.
type streamNote struct {
	reason     string
	maxInbound int
	byProtocol map[string]int
}

// streamSnapshotQueue is how many noted peers can wait for their
// per-protocol snapshot. A peer noted while the queue is full is read at
// the next scan instead.
const streamSnapshotQueue = 64

// streamAttribution collects the peers to name in the attribution log:
// those that passed the per-peer threshold or were refused at a limit.
// Notes are taken on hot paths, including inside the resource manager's
// trace reporter, so note only updates a map and hands the peer to a
// worker without blocking; the worker reads the peer's streams by
// protocol at once, so a short burst is seen as it was. A peer named in
// the log is not noted again for streamAttributionInterval.
type streamAttribution struct {
	now func() time.Time

	mu        sync.Mutex
	peers     map[libp2ppeer.ID]*streamNote
	logged    map[libp2ppeer.ID]time.Time
	protocols func(libp2ppeer.ID) map[string]int

	snapshots chan libp2ppeer.ID
}

func newStreamAttribution(now func() time.Time) *streamAttribution {
	if now == nil {
		now = time.Now
	}
	return &streamAttribution{
		now:       now,
		peers:     make(map[libp2ppeer.ID]*streamNote),
		logged:    make(map[libp2ppeer.ID]time.Time),
		snapshots: make(chan libp2ppeer.ID, streamSnapshotQueue),
	}
}

// note records peer p with the reason and its inbound stream count, and
// queues a per-protocol snapshot the first time p is noted.
func (a *streamAttribution) note(p libp2ppeer.ID, reason string, inbound int) {
	if a == nil {
		return
	}
	a.mu.Lock()
	if last, ok := a.logged[p]; ok && a.now().Sub(last) < streamAttributionInterval {
		a.mu.Unlock()
		return
	}
	n, ok := a.peers[p]
	if !ok {
		n = &streamNote{reason: reason}
		a.peers[p] = n
	}
	n.maxInbound = max(n.maxInbound, inbound)
	a.mu.Unlock()
	if !ok {
		select {
		case a.snapshots <- p:
		default:
		}
	}
}

// setProtocols sets the function that reads a peer's inbound streams by
// protocol. It is set once the host exists, before run starts.
func (a *streamAttribution) setProtocols(f func(libp2ppeer.ID) map[string]int) {
	a.mu.Lock()
	a.protocols = f
	a.mu.Unlock()
}

// run takes the per-protocol snapshot of each queued peer until quit is
// closed.
func (a *streamAttribution) run(quit <-chan struct{}) {
	for {
		select {
		case <-quit:
			return
		case p := <-a.snapshots:
			a.snapshot(p)
		}
	}
}

// snapshot reads the inbound streams of p by protocol into its note, if
// it still has a note without one. The note is written only while it is
// still in the map: take may have handed it to the scan meanwhile.
func (a *streamAttribution) snapshot(p libp2ppeer.ID) {
	a.mu.Lock()
	f := a.protocols
	n, ok := a.peers[p]
	a.mu.Unlock()
	if f == nil || !ok {
		return
	}
	counts := f(p)
	a.mu.Lock()
	if cur, ok := a.peers[p]; ok && cur == n && n.byProtocol == nil {
		n.byProtocol = counts
	}
	a.mu.Unlock()
}

// take returns copies of the notes since the previous call and marks
// each peer as logged now, so it is not noted again for
// streamAttributionInterval.
func (a *streamAttribution) take() map[libp2ppeer.ID]streamNote {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	for p, last := range a.logged {
		if now.Sub(last) >= streamAttributionInterval {
			delete(a.logged, p)
		}
	}
	if len(a.peers) == 0 {
		return nil
	}
	notes := make(map[libp2ppeer.ID]streamNote, len(a.peers))
	for p, n := range a.peers {
		notes[p] = *n
		a.logged[p] = now
	}
	a.peers = make(map[libp2ppeer.ID]*streamNote)
	return notes
}

// Reasons in the attribution log besides the limit labels.
const (
	streamReasonAboveThreshold = "above_threshold"
	streamReasonPeerLimit      = "peer_limit"
)

// streamHighWater wraps the resource manager's trace reporter and keeps
// the highest inbound stream count of each peer and of the transient
// scope, from every change the resource manager reports. Every event is
// passed on unchanged, so the existing resource-manager metrics stay as
// they are. The reporter is called synchronously, so each event costs
// one map update under a mutex.
type streamHighWater struct {
	inner rcmgr.TraceReporter
	attr  *streamAttribution

	mu           sync.Mutex
	peerMax      int
	transientMax int
	transientCur int
}

func newStreamHighWater(inner rcmgr.TraceReporter, attr *streamAttribution) *streamHighWater {
	return &streamHighWater{inner: inner, attr: attr}
}

func (h *streamHighWater) ConsumeEvent(evt rcmgr.TraceEvt) {
	h.inner.ConsumeEvent(evt)

	switch evt.Type {
	case rcmgr.TraceAddStreamEvt, rcmgr.TraceRemoveStreamEvt, rcmgr.TraceBlockAddStreamEvt:
	default:
		return
	}
	if evt.DeltaIn == 0 {
		return
	}
	if rcmgr.IsTransientScope(evt.Name) {
		if evt.Type == rcmgr.TraceBlockAddStreamEvt {
			return
		}
		h.mu.Lock()
		h.transientCur = evt.StreamsIn
		h.transientMax = max(h.transientMax, evt.StreamsIn)
		h.mu.Unlock()
		return
	}
	ps := rcmgr.PeerStrInScopeName(evt.Name)
	if ps == "" {
		return
	}
	switch evt.Type {
	case rcmgr.TraceAddStreamEvt:
		h.mu.Lock()
		h.peerMax = max(h.peerMax, evt.StreamsIn)
		h.mu.Unlock()
		if evt.StreamsIn > streamLogThreshold {
			if p, err := libp2ppeer.Decode(ps); err == nil {
				h.attr.note(p, streamReasonAboveThreshold, evt.StreamsIn)
			}
		}
	case rcmgr.TraceBlockAddStreamEvt:
		if p, err := libp2ppeer.Decode(ps); err == nil {
			h.attr.note(p, streamReasonPeerLimit, evt.StreamsIn)
		}
	}
}

// take returns the highest per-peer and transient inbound counts since
// the previous call. The transient maximum starts the next period at the
// current count; the per-peer one at zero, since the 30 s scan reports
// the current per-peer counts itself.
func (h *streamHighWater) take() (peerMax, transientMax int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	peerMax, transientMax = h.peerMax, h.transientMax
	h.peerMax = 0
	h.transientMax = h.transientCur
	return peerMax, transientMax
}
