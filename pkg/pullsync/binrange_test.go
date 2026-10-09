// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync_test

import (
	"context"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/v2/pkg/p2p/streamtest"
	"github.com/ethersphere/bee/v2/pkg/pullsync"
	"github.com/ethersphere/bee/v2/pkg/pullsync/pb"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/prometheus/client_golang/prometheus"
)

// openRawBin opens a pull-sync stream and writes a Get with a raw int32 bin,
// so values a correct requester never sends can be tested.
func openRawBin(t *testing.T, recorder *streamtest.Recorder, bin int32, start uint64) p2p.Stream {
	t.Helper()
	stream, err := recorder.NewStream(context.Background(), swarm.ZeroAddress, nil, psProtocol, psVersion, psStream)
	if err != nil {
		t.Fatal(err)
	}
	if err := protobuf.NewWriter(stream).WriteMsg(&pb.Get{Bin: bin, Start: start}); err != nil {
		t.Fatal(err)
	}
	return stream
}

// requireRefused fails unless the server ends the stream before sending an
// offer: a read returns EOF or an error, never an Offer.
func requireRefused(t *testing.T, name string, stream p2p.Stream) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		var offer pb.Offer
		done <- protobuf.NewReader(stream).ReadMsg(&offer)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("%s: server sent an offer, want the request refused", name)
		}
	case <-time.After(waitDeadline):
		t.Fatalf("%s: no answer after %s, want the request refused at once", name, waitDeadline)
	}
}

func TestBinOutOfRangeRawValues(t *testing.T) {
	for _, bin := range []int32{32, 255, 278, -1, -234} {
		synctest.Test(t, func(t *testing.T) {
			server, store := newPullSync(t, nil, 5)
			recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))

			stream := openRawBin(t, recorder, bin, 7)
			synctest.Wait()
			requireRefused(t, "bin "+strconv.Itoa(int(bin)), stream)

			if got := server.WaitingTracked(); got != 0 {
				t.Fatalf("bin %d: waiting %d, want 0", bin, got)
			}
			if got := store.SubscribeBinCalls(); got != 0 {
				t.Fatalf("bin %d: %d SubscribeBin calls, want 0", bin, got)
			}
			if got := server.RequestsRefused(pullsync.ReasonBinOutOfRange); got != 1 {
				t.Fatalf("bin %d: refused counter %v, want 1", bin, got)
			}
		})
	}
}

func TestBinEdgesServed(t *testing.T) {
	for _, bin := range []int32{0, 31} {
		synctest.Test(t, func(t *testing.T) {
			server, store := newPullSync(t, nil, 5)
			recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))

			stream := openRawBin(t, recorder, bin, 7)
			synctest.Wait()
			if got := server.WaitingTracked(); got != 1 {
				t.Fatalf("bin %d: waiting %d, want 1 (served)", bin, got)
			}
			if got := store.SubscribeBinCalls(); got != 1 {
				t.Fatalf("bin %d: %d SubscribeBin calls, want 1", bin, got)
			}
			if got := server.RequestsRefused(pullsync.ReasonBinOutOfRange); got != 0 {
				t.Fatalf("bin %d: refused counter %v, want 0", bin, got)
			}
			_ = stream.Reset()
		})
	}
}

// TestBinRefusedBeforeRegistration: a value that wraps to bin 22 must not
// register as (peer, 22, start), which would end a valid waiting request for
// bin 22 with the same start as a duplicate (#640) and never unregister.
func TestBinRefusedBeforeRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server, _ := newPullSync(t, nil, 5)
		recorder := streamtest.New(streamtest.WithProtocols(server.Protocol()), streamtest.WithBaseAddr(peerA))

		valid := openRawBin(t, recorder, 22, 240)
		synctest.Wait()
		if got := server.WaitingTracked(); got != 1 {
			t.Fatalf("waiting %d, want 1 before the wrapped request", got)
		}

		wrapped := openRawBin(t, recorder, -234, 240)
		synctest.Wait()
		requireRefused(t, "bin -234", wrapped)

		if got := server.WaitingTracked(); got != 1 {
			t.Fatalf("waiting %d after the refusal, want 1 (only the valid request)", got)
		}
		if got := server.RequestsReplaced("same_start") + server.RequestsReplaced("over_cap"); got != 0 {
			t.Fatalf("replaced %v, want 0: the wrapped request ended the valid one", got)
		}

		// The valid request is still waiting: its stream gives no answer.
		done := make(chan error, 1)
		go func() {
			var offer pb.Offer
			done <- protobuf.NewReader(valid).ReadMsg(&offer)
		}()
		select {
		case err := <-done:
			t.Fatalf("valid request ended (%v), want it still waiting", err)
		case <-time.After(waitDeadline):
		}
		_ = valid.Reset()
		<-done
	})
}

func TestBinRefusedSeriesExportedAtZero(t *testing.T) {
	server, _ := newPullSync(t, nil, 5)
	reg := prometheus.NewRegistry()
	for _, c := range server.Metrics() {
		if err := reg.Register(c); err != nil {
			t.Fatal(err)
		}
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "bee_pullsync_requests_refused_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == pullsync.ReasonBinOutOfRange {
					if v := m.GetCounter().GetValue(); v != 0 {
						t.Fatalf("value %v before any refusal, want 0", v)
					}
					return
				}
			}
		}
	}
	t.Fatal("requests_refused_total{reason=bin_out_of_range} not exported before any refusal")
}
