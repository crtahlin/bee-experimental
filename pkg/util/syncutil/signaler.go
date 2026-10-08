// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package syncutil

import (
	"sync"
	"sync/atomic"
)

// Signaler allows for multiple writers to safely signal an event
// so that reader on the channel C would get unblocked
type Signaler struct {
	C    chan struct{}
	once sync.Once
	err  atomic.Pointer[error]
}

// NewSignaler initializes a new obj
func NewSignaler() *Signaler {
	return &Signaler{C: make(chan struct{})}
}

// Signal safely closes the blocking channel
func (s *Signaler) Signal() {
	s.once.Do(func() {
		close(s.C)
	})
}

// SignalWithError records why the event happened, if nothing recorded a cause
// before, and then signals. The cause is stored before the channel closes, so
// a reader woken by C sees it through Err.
func (s *Signaler) SignalWithError(err error) {
	if err != nil {
		s.err.CompareAndSwap(nil, &err)
	}
	s.Signal()
}

// Err returns the first cause recorded by SignalWithError, or nil.
func (s *Signaler) Err() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}
