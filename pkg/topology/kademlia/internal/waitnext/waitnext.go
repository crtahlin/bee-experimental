// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package waitnext keeps, per peer address, when kademlia may dial the peer
// again and how its earlier connections and dials went.
package waitnext

import (
	"sync"
	"time"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type next struct {
	tryAfter       time.Time
	failedAttempts int
	// connectedAt is when kademlia counted the current connection, zero
	// when not connected.
	connectedAt time.Time
	// shortLived counts the connections in a row that ended within the
	// stable-connection threshold.
	shortLived int
	// expiresAt is zero for a normal entry and set only on the reduced
	// entry kept after the peer is pruned.
	expiresAt time.Time
}

type WaitNext struct {
	next             map[string]*next
	stableConnection time.Duration
	maxBackoff       time.Duration
	prunedEntryTTL   time.Duration
	sync.Mutex
}

// New returns a WaitNext. A connection that ends before stableConnection
// counts as short-lived; the wait after short-lived connections doubles up
// to maxShortLivedBackoff; the reduced entry kept after pruning expires
// prunedEntryTTL after its wait ends.
func New(stableConnection, maxShortLivedBackoff, prunedEntryTTL time.Duration) *WaitNext {
	return &WaitNext{
		next:             make(map[string]*next),
		stableConnection: stableConnection,
		maxBackoff:       maxShortLivedBackoff,
		prunedEntryTTL:   prunedEntryTTL,
	}
}

// backoff is the wait after n short-lived connections in a row: base when n
// is 0, otherwise base * 2^(n-1) capped at the maximum backoff.
func (r *WaitNext) backoff(n int, base time.Duration) time.Duration {
	if n <= 0 {
		return base
	}
	d := base
	for i := 1; i < n; i++ {
		d *= 2
		if d >= r.maxBackoff {
			return r.maxBackoff
		}
	}
	return min(d, r.maxBackoff)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// entry returns the live entry for addr, creating one when there is none
// or when the existing one expired at or before now, so an expired reduced
// entry starts again from nothing. Any event on an entry makes it a normal
// entry again, so expiresAt is cleared. The caller holds the lock.
func (r *WaitNext) entry(addr swarm.Address, now time.Time) *next {
	info, ok := r.lookup(addr, now)
	if !ok {
		info = &next{}
		r.next[addr.ByteString()] = info
	}
	info.expiresAt = time.Time{}
	return info
}

// lookup returns the entry for addr, deleting it first when it has expired.
// The caller holds the lock.
func (r *WaitNext) lookup(addr swarm.Address, now time.Time) (*next, bool) {
	info, ok := r.next[addr.ByteString()]
	if !ok {
		return nil, false
	}
	if !info.expiresAt.IsZero() && !now.Before(info.expiresAt) {
		delete(r.next, addr.ByteString())
		return nil, false
	}
	return info, true
}

// Connected records that kademlia counted a connection to addr at now. It
// always overwrites the start of an earlier connection, and leaves the
// counts and the next dial time as they are: a connected peer is not dialled.
func (r *WaitNext) Connected(addr swarm.Address, now time.Time) {
	r.Lock()
	defer r.Unlock()

	r.entry(addr, now).connectedAt = now
}

// Disconnected records the end of a connection to addr at now and sets when
// the peer may be dialled again. A connection that lasted at least the
// stable-connection threshold resets both counts and the wait to now plus
// base. A shorter one keeps the failure count, increases the short-lived
// count and waits the doubled backoff. It reports whether the connection
// was short-lived and how long it lasted. A disconnect with no counted
// connection, for example a second call for the same connection, counts
// nothing and only keeps the later of the current wait and now plus base.
// InConnection reports whether the start of a connection to addr is
// recorded and not yet ended. It never creates an entry.
func (r *WaitNext) InConnection(addr swarm.Address, now time.Time) bool {
	r.Lock()
	defer r.Unlock()

	info, ok := r.lookup(addr, now)
	return ok && !info.connectedAt.IsZero()
}

func (r *WaitNext) Disconnected(addr swarm.Address, now time.Time, base time.Duration) (shortLived bool, lasted time.Duration) {
	r.Lock()
	defer r.Unlock()

	info := r.entry(addr, now)
	if info.connectedAt.IsZero() {
		info.tryAfter = later(info.tryAfter, now.Add(base))
		return false, 0
	}

	lasted = now.Sub(info.connectedAt)
	info.connectedAt = time.Time{}

	if lasted >= r.stableConnection {
		info.failedAttempts = 0
		info.shortLived = 0
		info.tryAfter = now.Add(base)
		return false, lasted
	}

	info.shortLived++
	info.tryAfter = later(info.tryAfter, now.Add(r.backoff(info.shortLived, base)))
	return true, lasted
}

// Failed records a failed dial to addr: the failure count becomes attempts,
// and the next dial waits for the latest of the current wait, retryAt, and
// now plus the backoff for the short-lived connections so far.
func (r *WaitNext) Failed(addr swarm.Address, now, retryAt time.Time, attempts int, base time.Duration) {
	r.Lock()
	defer r.Unlock()

	info := r.entry(addr, now)
	info.failedAttempts = attempts
	info.tryAfter = later(later(info.tryAfter, retryAt), now.Add(r.backoff(info.shortLived, base)))
}

// Pruned records that addr was pruned from the address book. A peer with no
// short-lived connections loses its entry, as before. A peer with some keeps
// a reduced entry: the short-lived count, a fresh wait of the backoff from
// now, and an expiry prunedEntryTTL after that wait, so that the wait holds
// when the peer is announced again soon after.
func (r *WaitNext) Pruned(addr swarm.Address, now time.Time, base time.Duration) {
	r.Lock()
	defer r.Unlock()

	info, ok := r.lookup(addr, now)
	if !ok || info.shortLived == 0 {
		delete(r.next, addr.ByteString())
		return
	}

	info.failedAttempts = 0
	info.connectedAt = time.Time{}
	info.tryAfter = later(info.tryAfter, now.Add(r.backoff(info.shortLived, base)))
	info.expiresAt = info.tryAfter.Add(r.prunedEntryTTL)
}

// Sweep deletes the reduced entries that expired at or before now.
func (r *WaitNext) Sweep(now time.Time) {
	r.Lock()
	defer r.Unlock()

	for k, info := range r.next {
		if !info.expiresAt.IsZero() && !now.Before(info.expiresAt) {
			delete(r.next, k)
		}
	}
}

func (r *WaitNext) Waiting(addr swarm.Address) bool {
	r.Lock()
	defer r.Unlock()

	now := time.Now()
	info, ok := r.lookup(addr, now)
	return ok && now.Before(info.tryAfter)
}

func (r *WaitNext) Attempts(addr swarm.Address) int {
	r.Lock()
	defer r.Unlock()

	if info, ok := r.lookup(addr, time.Now()); ok {
		return info.failedAttempts
	}

	return 0
}

// TryAfter returns when addr may be dialled again, and whether it has an
// entry at all.
func (r *WaitNext) TryAfter(addr swarm.Address) (time.Time, bool) {
	r.Lock()
	defer r.Unlock()

	info, ok := r.next[addr.ByteString()]
	if !ok {
		return time.Time{}, false
	}
	return info.tryAfter, true
}

// ShortLived returns how many connections to addr in a row were short-lived.
func (r *WaitNext) ShortLived(addr swarm.Address) int {
	r.Lock()
	defer r.Unlock()

	if info, ok := r.next[addr.ByteString()]; ok {
		return info.shortLived
	}
	return 0
}

// Len returns the number of entries, expired ones included until a sweep or
// a lookup deletes them.
func (r *WaitNext) Len() int {
	r.Lock()
	defer r.Unlock()

	return len(r.next)
}

func (r *WaitNext) Remove(addr swarm.Address) {
	r.Lock()
	defer r.Unlock()

	delete(r.next, addr.ByteString())
}
