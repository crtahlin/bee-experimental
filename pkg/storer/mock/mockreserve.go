// Copyright 2023 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mockstorer

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

type chunksResponse struct {
	chunks []*storer.BinC
	err    error
}

// WithSubscribeResp mocks a desired response when calling IntervalChunks method.
// Different possible responses for subsequent responses in multi-call scenarios
// are possible (i.e. first call yields a,b,c, second call yields d,e,f).
// Mock maintains state of current call using chunksCalls counter.
func WithSubscribeResp(chunks []*storer.BinC, err error) Option {
	return optionFunc(func(p *ReserveStore) {
		p.subResponses = append(p.subResponses, chunksResponse{chunks: chunks, err: err})
	})
}

// WithChunks mocks the set of chunks that the store is aware of (used in Get and Has calls).
func WithChunks(chs ...swarm.Chunk) Option {
	return optionFunc(func(p *ReserveStore) {
		for _, c := range chs {
			if c.Stamp() != nil {
				stampHash, _ := c.Stamp().Hash()
				p.chunks[c.Address().String()+string(c.Stamp().BatchID())+string(stampHash)] = c
			} else {
				p.chunks[c.Address().String()] = c
			}
		}
	})
}

// WithEvilChunk allows to inject a malicious chunk (request a certain address
// of a chunk, but get another), in order to mock unsolicited chunk delivery.
func WithEvilChunk(addr swarm.Address, ch swarm.Chunk) Option {
	return optionFunc(func(p *ReserveStore) {
		p.evilAddr = addr
		p.evilChunk = ch
	})
}

func WithCursors(c []uint64, e uint64) Option {
	return optionFunc(func(p *ReserveStore) {
		p.cursors = c
		p.epoch = e
	})
}

func WithCursorsErr(e error) Option {
	return optionFunc(func(p *ReserveStore) {
		p.cursorsErr = e
	})
}

func WithRadius(r uint8) Option {
	return optionFunc(func(p *ReserveStore) {
		p.radius = r
	})
}

func WithReserveSize(s int) Option {
	return optionFunc(func(p *ReserveStore) {
		p.reservesize = s
	})
}

func WithCapacityDoubling(s int) Option {
	return optionFunc(func(p *ReserveStore) {
		p.capacityDoubling = s
	})
}

func WithPutHook(f func(swarm.Chunk) error) Option {
	return optionFunc(func(p *ReserveStore) {
		p.putHook = f
	})
}

func WithSample(s storer.Sample) Option {
	return optionFunc(func(p *ReserveStore) {
		p.sample = s
	})
}

// WithEvictingFor sets what EvictingFor returns (#649).
func WithEvictingFor(d time.Duration) Option {
	return optionFunc(func(p *ReserveStore) {
		p.evictingFor.Store(int64(d))
	})
}

// WithSampleHook sets a function ReserveSample calls before it returns, so a
// test can change the store while a sample runs (#649).
func WithSampleHook(f func()) Option {
	return optionFunc(func(p *ReserveStore) {
		p.sampleHook = f
	})
}

func WithWindowedSample(s storer.Sample) Option {
	return optionFunc(func(p *ReserveStore) {
		p.windowedSample = s
		p.windowedSampleSet = true
	})
}

var _ storer.ReserveStore = (*ReserveStore)(nil)

type ReserveStore struct {
	mtx         sync.Mutex
	chunksCalls int
	subCalls    int
	putCalls    int
	setCalls    int

	chunks    map[string]swarm.Chunk
	evilAddr  swarm.Address
	evilChunk swarm.Chunk

	cursors    []uint64
	cursorsErr error
	epoch      uint64

	radius           uint8
	radiusIncreases  uint64
	reservesize      int
	capacityDoubling int
	sampling         atomic.Bool
	evictingFor      atomic.Int64

	subResponses []chunksResponse
	subs         map[*subscription]struct{}
	putHook      func(swarm.Chunk) error

	sample            storer.Sample
	windowedSample    storer.Sample
	windowedSampleSet bool
	sampleHook        func()
}

// NewReserve returns a new Reserve mock.
func NewReserve(opts ...Option) *ReserveStore {
	s := &ReserveStore{
		chunks: make(map[string]swarm.Chunk),
	}
	for _, v := range opts {
		v.apply(s)
	}
	return s
}

func (s *ReserveStore) EvictBatch(ctx context.Context, batchID []byte) error { return nil }
func (s *ReserveStore) IsWithinStorageRadius(addr swarm.Address) bool        { return true }
func (s *ReserveStore) IsSampling() bool                                     { return s.sampling.Load() }

// SetSampling toggles the sampling-in-progress signal so a test can assert the
// puller pauses while it is set. See issue #23.
func (s *ReserveStore) SetSampling(v bool)  { s.sampling.Store(v) }
func (s *ReserveStore) IsFullySynced() bool { return true }

// EvictingFor returns the duration SetEvictingFor set, 0 by default (#649).
func (s *ReserveStore) EvictingFor() time.Duration { return time.Duration(s.evictingFor.Load()) }

// SetEvictingFor sets what EvictingFor returns, so a test can check that the
// storage incentives agent sits out a round while the node evicts.
func (s *ReserveStore) SetEvictingFor(d time.Duration) { s.evictingFor.Store(int64(d)) }

func (s *ReserveStore) StorageRadius() uint8 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.radius
}

func (s *ReserveStore) SetStorageRadius(r uint8) {
	s.mtx.Lock()
	if r > s.radius {
		s.radiusIncreases++
	}
	s.radius = r
	s.mtx.Unlock()
}

// RadiusState returns the radius and how many times SetStorageRadius raised
// it (#658).
func (s *ReserveStore) RadiusState() (uint8, uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.radius, s.radiusIncreases
}

func (s *ReserveStore) CommittedDepth() uint8 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.radius + uint8(s.capacityDoubling)
}

func (s *ReserveStore) CapacityDoubling() uint8 {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return uint8(s.capacityDoubling)
}

// subscription is an open SubscribeBin call without a prepared response,
// which PublishBin can deliver to.
type subscription struct {
	ctx   context.Context
	bin   uint8
	start uint64
	out   chan *storer.BinC
}

// SubscribeBin returns the next prepared response (WithSubscribeResp). When no
// prepared response is left it returns a subscription that stays open and
// empty until PublishBin delivers to it or its context ends.
func (s *ReserveStore) SubscribeBin(ctx context.Context, bin uint8, start uint64) (<-chan *storer.BinC, func(), <-chan error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.subCalls++

	out := make(chan *storer.BinC)
	errC := make(chan error, 1)

	if s.chunksCalls >= len(s.subResponses) {
		sub := &subscription{ctx: ctx, bin: bin, start: start, out: out}
		if s.subs == nil {
			s.subs = make(map[*subscription]struct{})
		}
		s.subs[sub] = struct{}{}
		return out, func() { s.dropSub(sub) }, errC
	}

	r := s.subResponses[s.chunksCalls]
	s.chunksCalls++

	go func() {
		for _, c := range r.chunks {
			select {
			case out <- &storer.BinC{Address: c.Address, BatchID: c.BatchID, BinID: c.BinID, StampHash: c.StampHash}:
			case <-ctx.Done():
				select {
				case errC <- ctx.Err():
				default:
				}
			}
		}
	}()

	return out, func() {}, errC
}

func (s *ReserveStore) dropSub(sub *subscription) {
	s.mtx.Lock()
	delete(s.subs, sub)
	s.mtx.Unlock()
}

// PublishBin delivers c to every open subscription without a prepared
// response for bin whose start is at or below c.BinID. A subscription whose
// context has ended is dropped instead.
func (s *ReserveStore) PublishBin(bin uint8, c *storer.BinC) {
	s.mtx.Lock()
	var targets []*subscription
	for sub := range s.subs {
		if sub.bin == bin && sub.start <= c.BinID {
			targets = append(targets, sub)
		}
	}
	s.mtx.Unlock()

	for _, sub := range targets {
		select {
		case sub.out <- c:
		case <-sub.ctx.Done():
			s.dropSub(sub)
		}
	}
}

// SubscribeBinCalls returns how many times SubscribeBin was called, whether
// or not a prepared response was left. For tests.
func (s *ReserveStore) SubscribeBinCalls() int {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.subCalls
}

// OpenSubscriptions returns the number of open subscriptions without a
// prepared response.
func (s *ReserveStore) OpenSubscriptions() int {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return len(s.subs)
}

func (s *ReserveStore) ReserveSize() int {
	return s.reservesize
}

func (s *ReserveStore) ReserveLastBinIDs() (curs []uint64, epoch uint64, err error) {
	return s.cursors, s.epoch, s.cursorsErr
}

// PutCalls returns the amount of times Put was called.
func (s *ReserveStore) PutCalls() int {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.putCalls
}

// SetCalls returns the amount of times Set was called.
func (s *ReserveStore) SetCalls() int {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.setCalls
}

// Get chunks.
func (s *ReserveStore) ReserveGet(ctx context.Context, addr swarm.Address, batchID []byte, stampHash []byte) (swarm.Chunk, error) {
	if s.evilAddr.Equal(addr) {
		// inject the malicious chunk instead
		return s.evilChunk, nil
	}

	if v, ok := s.chunks[addr.String()+string(batchID)+string(stampHash)]; ok {
		return v, nil
	}

	return nil, storage.ErrNotFound
}

// Put chunks.
func (s *ReserveStore) ReservePutter() storage.Putter {
	return storage.PutterFunc(
		func(ctx context.Context, c swarm.Chunk) error {
			s.mtx.Lock()
			s.putCalls++
			s.mtx.Unlock()
			return s.put(ctx, c)
		},
	)
}

// Put chunks.
func (s *ReserveStore) put(_ context.Context, chs ...swarm.Chunk) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, c := range chs {
		if s.putHook != nil {
			if err := s.putHook(c); err != nil {
				return err
			}
		}
		stampHash, err := c.Stamp().Hash()
		if err != nil {
			return err
		}
		s.chunks[c.Address().String()+string(c.Stamp().BatchID())+string(stampHash)] = c
	}
	return nil
}

// Has chunks.
func (s *ReserveStore) ReserveHas(addr swarm.Address, batchID []byte, stampHash []byte) (bool, error) {
	if _, ok := s.chunks[addr.String()+string(batchID)+string(stampHash)]; !ok {
		return false, nil
	}
	return true, nil
}

func (s *ReserveStore) ReserveSample(context.Context, []byte, uint8, uint64, *big.Int) (storer.Sample, error) {
	if s.sampleHook != nil {
		s.sampleHook()
	}
	return s.sample, nil
}

func (s *ReserveStore) WindowedSample(context.Context, []byte, uint8, uint64, *big.Int) (storer.Sample, error) {
	if s.windowedSampleSet {
		return s.windowedSample, nil
	}
	return s.sample, nil
}

type Option interface {
	apply(*ReserveStore)
}
type optionFunc func(*ReserveStore)

func (f optionFunc) apply(r *ReserveStore) { f(r) }
