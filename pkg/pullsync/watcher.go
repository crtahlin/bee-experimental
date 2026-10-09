// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/libp2p/go-libp2p/core/network"
)

var (
	// errRequesterGone is the cancel cause of a waiting request whose
	// requester reset its stream, closed its write side, or lost the
	// connection (#641).
	errRequesterGone = errors.New("pullsync: requester gone")
	// errUnexpectedData is the cancel cause of a waiting request whose
	// requester sent data before the offer, which a correct requester never
	// does (#641).
	errUnexpectedData = errors.New("pullsync: data from the requester before the offer")
)

const (
	reasonReset          = "reset"
	reasonDisconnect     = "disconnect"
	reasonEOF            = "eof"
	reasonError          = "error"
	reasonUnexpectedData = "unexpected_data"
)

// readDeadliner is the part of a stream the watcher needs to be stopped
// without consuming data. Streams from libp2p have it; p2p.Stream does not
// declare it.
type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

// watcher reads from a request's stream while the request waits for its
// first chunk, and ends the request when the read returns (#641).
type watcher struct {
	dl      readDeadliner
	stopped atomic.Bool
	done    chan struct{}
	reason  string // set before done is closed
}

// startWatcher starts a watcher on the stream, or returns nil if the stream
// has no read deadline. cancel ends the request with a cause.
func startWatcher(stream p2p.Stream, cancel context.CancelCauseFunc) *watcher {
	dl, ok := stream.(readDeadliner)
	if !ok || dl.SetReadDeadline(time.Time{}) != nil {
		return nil
	}
	w := &watcher{dl: dl, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		var b [1]byte
		for {
			n, err := stream.Read(b[:])
			if n > 0 {
				w.reason = reasonUnexpectedData
				cancel(errUnexpectedData)
				return
			}
			if w.stopped.Load() {
				return
			}
			if err != nil {
				w.reason = requesterGoneReason(err)
				cancel(errRequesterGone)
				return
			}
		}
	}()
	return w
}

// stop ends the watcher before the offer is written: the stop flag is set
// first, so any read that returns without data after it is ignored whatever
// its error; then the read deadline wakes the read, and is cleared once the
// watcher has returned so later reads are not affected. A nil watcher is a
// no-op.
func (w *watcher) stop() {
	if w == nil {
		return
	}
	w.stopped.Store(true)
	_ = w.dl.SetReadDeadline(time.Now())
	<-w.done
	_ = w.dl.SetReadDeadline(time.Time{})
}

// requesterGoneReason classifies the error of a read that returned no data.
// The order matters: at the end of a connection a stream's error can wrap
// both a reset and io.EOF.
func requesterGoneReason(err error) string {
	var se *network.StreamError
	if errors.As(err, &se) && se.Remote {
		return reasonReset
	}
	var ce *network.ConnError
	if errors.As(err, &ce) || errors.Is(err, network.ErrReset) {
		return reasonDisconnect
	}
	if errors.Is(err, io.EOF) {
		return reasonEOF
	}
	return reasonError
}
