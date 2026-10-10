// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer_test

import (
	"context"
	"encoding/binary"
	"math/big"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/postage"
	realbatchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore"
	batchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	postagetesting "github.com/ethersphere/bee/v2/pkg/postage/testing"
	pullerMock "github.com/ethersphere/bee/v2/pkg/puller/mock"
	"github.com/ethersphere/bee/v2/pkg/statestore/leveldb"
	chunk "github.com/ethersphere/bee/v2/pkg/storage/testing"
	"github.com/ethersphere/bee/v2/pkg/storer"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

// signedBatch is a batch with a real owner key, saved in a real batch
// store, whose stamps pass postage.ValidStamp.
type signedBatch struct {
	batch  *postage.Batch
	signer crypto.Signer
	next   uint32
}

func newSignedBatch(t *testing.T, value int64) *signedBatch {
	t.Helper()
	key, err := crypto.GenerateSecp256k1Key()
	if err != nil {
		t.Fatal(err)
	}
	signer := crypto.NewDefaultSigner(key)
	owner, err := crypto.NewEthereumAddress(key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	b := postagetesting.MustNewBatch(postagetesting.WithOwner(owner), postagetesting.WithValue(value), postagetesting.WithDepth(20))
	b.BucketDepth = 16
	return &signedBatch{batch: b, signer: signer}
}

// stamp returns a valid stamp for addr with the given timestamp.
func (s *signedBatch) stamp(addr swarm.Address, ts uint64) *postage.Stamp {
	bucket := binary.BigEndian.Uint32(addr.Bytes()[:4]) >> (32 - s.batch.BucketDepth)
	index := make([]byte, postage.IndexSize)
	binary.BigEndian.PutUint32(index, bucket)
	binary.BigEndian.PutUint32(index[4:], s.next)
	s.next++
	tsb := make([]byte, 8)
	binary.BigEndian.PutUint64(tsb, ts)
	sig := postagetesting.MustNewValidSignature(s.signer, addr, s.batch.ID, index, tsb)
	return postage.NewStamp(s.batch.ID, index, tsb, sig)
}

// TestExpiredBatchSampleSameEvictedOrNot checks the claim behind #663:
// once a batch has expired, every node's sample leaves its chunks out
// whether or not the reserve worker has evicted them yet, so an expiry
// eviction cannot make neighbours' samples differ. It uses the real batch
// store, with its eviction callback wired to the storer as pkg/node does,
// and the default stamp validation.
func TestExpiredBatchSampleSameEvictedOrNot(t *testing.T) {
	t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)

	// An ordered state store: cleanup walks batches by value and stops at
	// the first one above the payout, which the map-based mock does not
	// keep in order.
	state, err := leveldb.NewInMemoryStateStore(log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	var st *storer.DB
	bs, err := realbatchstore.New(state, func(id []byte) error { return st.EvictBatch(context.Background(), id) }, 1_000_000, log.Noop)
	if err != nil {
		t.Fatal(err)
	}
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 10_000, bs, nil, time.Minute)
	opts.ValidStamp = nil // the default, postage.ValidStamp over the real batch store
	st, err = memStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}

	valid := newSignedBatch(t, 1_000)  // A
	expiring := newSignedBatch(t, 100) // E
	notYet := newSignedBatch(t, 600)   // above the payout after expiry, below the minimum balance
	for _, b := range []*signedBatch{valid, expiring, notYet} {
		if err := bs.Save(b.batch); err != nil {
			t.Fatal(err)
		}
	}

	// With the anchor equal to the overlay every chunk at proximity >= the
	// depth is in range. Seven chunks, fewer than SampleSize, so each valid
	// one is in the sample whatever its transformed address.
	const depth = 2
	consensus := uint64(time.Now().UnixNano())
	put := func(b *signedBatch, n int) {
		for range n {
			ch := chunk.GenerateValidRandomChunkAt(t, baseAddr, depth+1)
			ch = ch.WithStamp(b.stamp(ch.Address(), consensus-1))
			if err := st.ReservePutForTest(context.Background(), ch); err != nil {
				t.Fatal(err)
			}
		}
	}
	put(valid, 3)
	put(expiring, 2)
	put(notYet, 2)

	// S1 to S3 are taken with no minimum balance, so only expiry and the
	// stamp check decide; sampleMin is taken with a minimum balance above
	// the not-yet-expired batch's value, as the agent's minBatchBalance is.
	minBalance := big.NewInt(700)
	sampleWith := func(minimum *big.Int) storer.Sample {
		t.Helper()
		s, err := st.ReserveSample(context.Background(), baseAddr.Bytes(), depth, consensus, minimum)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	sample := func() storer.Sample { t.Helper(); return sampleWith(nil) }
	count := func(s storer.Sample, b *signedBatch) int {
		n := 0
		for _, it := range s.Items {
			if string(it.Stamp.BatchID()) == string(b.batch.ID) {
				n++
			}
		}
		return n
	}

	s1 := sample()
	m1 := sampleWith(minBalance)
	if count(s1, valid) != 3 {
		t.Fatalf("S1 holds %d items of the valid batch, want 3", count(s1, valid))
	}
	if count(s1, expiring) != 2 {
		t.Fatalf("S1 holds %d items of the batch that will expire, want 2 (the test must exercise the case)", count(s1, expiring))
	}
	if count(s1, notYet) != 2 {
		t.Fatalf("S1 holds %d items of the not-yet-expired batch, want 2", count(s1, notYet))
	}

	// Expire E the way the chain does: the total payout passes its value.
	// cleanup records it for eviction and deletes it from the batch store;
	// the reserve worker is not running, so nothing is deleted yet.
	if err := bs.PutChainState(&postage.ChainState{Block: 10, TotalAmount: big.NewInt(500), CurrentPrice: big.NewInt(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.Get(expiring.batch.ID); err == nil {
		t.Fatal("expired batch still in the batch store")
	}
	if got := st.ReserveSize(); got != 7 {
		t.Fatalf("reserve size %d before the worker runs, want 7 (nothing evicted yet)", got)
	}
	s2 := sample()

	ready := make(chan struct{})
	st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(st.StorageRadius()), ready)
	<-ready
	waitUntil(t, "the expired batch evicted", func() bool { return st.ReserveSize() == 5 })
	if _, err := bs.Get(notYet.batch.ID); err != nil {
		t.Fatalf("the not-yet-expired batch left the batch store: %v", err)
	}
	s3 := sample()
	m3 := sampleWith(minBalance)

	if len(s2.Items) != len(s3.Items) {
		t.Fatalf("sample differs with the expired chunks present (%d items) and evicted (%d items)", len(s2.Items), len(s3.Items))
	}
	for i := range s2.Items {
		if !s2.Items[i].TransformedAddress.Equal(s3.Items[i].TransformedAddress) || !s2.Items[i].ChunkAddress.Equal(s3.Items[i].ChunkAddress) {
			t.Fatalf("item %d differs: %s and %s", i, s2.Items[i].ChunkAddress, s3.Items[i].ChunkAddress)
		}
	}
	for name, s := range map[string]storer.Sample{"S2": s2, "S3": s3} {
		if count(s, expiring) != 0 {
			t.Fatalf("%s holds %d items of the expired batch, want 0", name, count(s, expiring))
		}
		if count(s, notYet) != 2 {
			t.Fatalf("%s holds %d items of the not-yet-expired batch, want 2", name, count(s, notYet))
		}
		if count(s, valid) != 3 {
			t.Fatalf("%s holds %d items of the valid batch, want 3", name, count(s, valid))
		}
	}
	// A batch above the payout but below the minimum balance is still in
	// the batch store and not evicted: the minimum balance leaves it out
	// before and after the expiry eviction of E.
	for name, s := range map[string]storer.Sample{"before": m1, "after": m3} {
		if count(s, notYet) != 0 || count(s, expiring) != 0 || count(s, valid) != 3 {
			t.Fatalf("sample with the minimum balance %s eviction: valid %d, expiring %d, not yet expired %d; want 3, 0, 0", name, count(s, valid), count(s, expiring), count(s, notYet))
		}
	}
}

// TestExpiryEvictionNotEvicting checks at the reserve-worker level that an
// expired-batch eviction never makes the node count as evicting, while its
// own active time is counted (#663).
func TestExpiryEvictionNotEvicting(t *testing.T) {
	t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)
	bs := batchstore.New()
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 10_000, bs, nil, time.Minute)
	opts.ReserveEvictionRate = 1_000
	st, err := memStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	putBin0(t, st, baseAddr, batch.ID, 2_100, 0)

	ready := make(chan struct{})
	st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), ready)
	<-ready
	if err := st.EvictBatch(context.Background(), batch.ID); err != nil {
		t.Fatal(err)
	}
	// About two seconds at 1,000 chunks/s.
	sawRun := false
	deadline := time.Now().Add(30 * time.Second)
	for st.ReserveSize() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the expiry eviction")
		}
		if got := st.EvictingFor(); got != 0 {
			t.Fatalf("EvictingFor %v during an expiry eviction, want 0", got)
		}
		if st.ExpiryRunningSecondsForTest() > 0 {
			sawRun = true
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawRun {
		t.Fatal("the expiry running gauge never rose above 0")
	}
	waitUntil(t, "the expiry run counted", func() bool { return st.EvictionExpirySecondsForTest() > 0.5 })
	if got := st.EvictingFor(); got != 0 {
		t.Fatalf("EvictingFor %v after the expiry eviction, want 0", got)
	}
}

// TestExpiryEvictionPausesForSample checks that an expired-batch eviction
// still waits while a reserve sample runs (#663).
func TestExpiryEvictionPausesForSample(t *testing.T) {
	t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)
	bs := batchstore.New()
	baseAddr := swarm.RandAddress(t)
	opts := dbTestOps(baseAddr, 10_000, bs, nil, time.Minute)
	st, err := memStorer(t, opts)()
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	putBin0(t, st, baseAddr, batch.ID, 1_500, 0)

	ready := make(chan struct{})
	st.StartReserveWorker(context.Background(), pullerMock.NewMockRateReporter(0), networkRadiusFunc(0), ready)
	<-ready
	st.SampleStartedForTest()
	if err := st.EvictBatch(context.Background(), batch.ID); err != nil {
		t.Fatal(err)
	}
	stays(t, "nothing evicted while a sample runs", 500*time.Millisecond, func() bool { return st.ReserveSize() == 1_500 })
	st.SampleDoneForTest()
	waitUntil(t, "the expired batch evicted after the sample", func() bool { return st.ReserveSize() == 0 })
	if got := st.EvictionPausedSecondsForTest(); got < 0.4 {
		t.Fatalf("paused seconds %v, want about the sample duration", got)
	}
}

// TestEpisodeWorkLeftIgnoresExpiredBatches checks that a recorded expired
// batch does not keep an eviction episode open; only being over capacity
// does (#663).
func TestEpisodeWorkLeftIgnoresExpiredBatches(t *testing.T) {
	t.Cleanup(storer.ResetReserveSizeWithinRadiusForTest)
	bs := batchstore.New()
	baseAddr := swarm.RandAddress(t)
	st, err := memStorer(t, dbTestOps(baseAddr, 100, bs, nil, time.Minute))()
	if err != nil {
		t.Fatal(err)
	}
	batch := postagetesting.MustNewBatch()
	if err := bs.Save(batch); err != nil {
		t.Fatal(err)
	}
	putBin0(t, st, baseAddr, batch.ID, 50, 0)
	// The worker is not started, so the expired batch stays recorded.
	if err := st.EvictBatch(context.Background(), batch.ID); err != nil {
		t.Fatal(err)
	}
	if st.EpisodeWorkLeftForTest() {
		t.Fatal("a recorded expired batch keeps the episode open, want only over capacity")
	}
	putBin0(t, st, baseAddr, batch.ID, 100, 0)
	if !st.EpisodeWorkLeftForTest() {
		t.Fatal("over capacity does not keep the episode open")
	}
}
