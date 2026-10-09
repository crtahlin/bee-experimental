// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync

import (
	"context"
	"errors"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// errRequestReplaced is the cancel cause of a waiting request that the same
// peer replaced by asking again for the same bin (#640).
var errRequestReplaced = errors.New("pullsync: request replaced by a newer request from the same peer")

// maxWaitingPerBin is how many requests one peer may have waiting for their
// first chunk in one bin. A correct requester has at most two: its live and
// its historical worker, with different starts (#640).
const maxWaitingPerBin = 2

const (
	reasonSameStart = "same_start"
	reasonOverCap   = "over_cap"
)

type waitingKey struct {
	peer string
	bin  uint8
}

type waitingEntry struct {
	key    waitingKey
	start  uint64
	cancel context.CancelCauseFunc
	done   chan struct{} // closed when the entry's handler unregisters it
}

// waitingRequests tracks inbound requests that are still building their
// offer, per peer and bin. Registering, ending older entries and
// unregistering share one mutex.
type waitingRequests struct {
	mu sync.Mutex
	m  map[waitingKey][]*waitingEntry // oldest first
	n  int
	// gauge, if set, receives the count after every change, under the mutex.
	gauge func(float64)
}

func (w *waitingRequests) changed() {
	if w.gauge != nil {
		w.gauge(float64(w.n))
	}
}

// register adds a waiting request and ends the older requests it replaces:
// every one with the same start, then the oldest until at most
// maxWaitingPerBin remain, counting the new one. It returns the new entry, the
// entries it ended and how many were ended for each reason.
func (w *waitingRequests) register(peer swarm.Address, bin uint8, start uint64, cancel context.CancelCauseFunc) (e *waitingEntry, ended []*waitingEntry, sameStart, overCap int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.m == nil {
		w.m = make(map[waitingKey][]*waitingEntry)
	}
	key := waitingKey{peer: peer.ByteString(), bin: bin}
	list := w.m[key]

	kept := list[:0]
	for _, old := range list {
		if old.start == start {
			old.cancel(errRequestReplaced)
			ended = append(ended, old)
			sameStart++
			continue
		}
		kept = append(kept, old)
	}
	for len(kept) >= maxWaitingPerBin {
		kept[0].cancel(errRequestReplaced)
		ended = append(ended, kept[0])
		overCap++
		kept = kept[1:]
	}

	e = &waitingEntry{key: key, start: start, cancel: cancel, done: make(chan struct{})}
	w.m[key] = append(kept, e)
	w.n += 1 - sameStart - overCap
	w.changed()
	return e, ended, sameStart, overCap
}

// unregister removes the entry if it is still tracked; an entry that a newer
// request already ended is gone and nothing else is removed. It must be called
// exactly once per entry, by the entry's own handler.
func (w *waitingRequests) unregister(e *waitingEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer close(e.done)

	list := w.m[e.key]
	for i, x := range list {
		if x == e {
			list = append(list[:i], list[i+1:]...)
			w.n--
			w.changed()
			break
		}
	}
	if len(list) == 0 {
		delete(w.m, e.key)
		return
	}
	w.m[e.key] = list
}

// count returns the number of tracked waiting requests.
func (w *waitingRequests) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}
