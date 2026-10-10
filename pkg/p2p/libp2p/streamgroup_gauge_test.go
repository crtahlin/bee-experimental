// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func groupGaugeValue(t *testing.T, m metrics, label string) float64 {
	t.Helper()
	var d dto.Metric
	if err := m.InboundStreamsGroup.WithLabelValues(label).Write(&d); err != nil {
		t.Fatal(err)
	}
	return d.GetGauge().GetValue()
}

// TestStreamGroupGaugeTracksCount checks that each group's gauge equals its
// count through reserves, refusals at the limit and releases (#687).
func TestStreamGroupGaugeTracksCount(t *testing.T) {
	t.Parallel()
	m := newMetrics()
	l := newStreamLimits(streamLimitsConfig{enabled: true, pullSyncMax: 2, otherMax: 1}, m, nil)

	check := func(step string) {
		t.Helper()
		if g, n := groupGaugeValue(t, m, "pullsync"), float64(l.group[streamGroupPullSync]); g != n {
			t.Fatalf("%s: pullsync gauge %v, count %v", step, g, n)
		}
		if g, n := groupGaugeValue(t, m, "other"), float64(l.group[streamGroupOther]); g != n {
			t.Fatalf("%s: other gauge %v, count %v", step, g, n)
		}
	}

	for i := 0; i < 3; i++ {
		l.reserveGroup(streamGroupPullSync) // the third is refused
	}
	check("pull-sync reserves")
	if got := groupGaugeValue(t, m, "pullsync"); got != 2 {
		t.Fatalf("pullsync gauge %v after a refusal at the limit, want 2", got)
	}
	l.reserveGroup(streamGroupOther)
	l.reserveGroup(streamGroupOther) // refused
	check("other reserves")
	if got := groupGaugeValue(t, m, "other"); got != 1 {
		t.Fatalf("other gauge %v after a refusal at the limit, want 1", got)
	}
	l.releaseGroup(streamGroupPullSync)
	l.releaseGroup(streamGroupOther)
	check("releases")
	if got := groupGaugeValue(t, m, "pullsync"); got != 1 {
		t.Fatalf("pullsync gauge %v after one release, want 1", got)
	}
}

// TestStreamGroupGaugeConcurrent ends at 0 after concurrent reserves and
// releases. It is a regression test for the Inc and Dec form: the earlier
// form, which set the gauge after unlocking, did not fail it in practice
// either (270 runs on a 15-core machine), so it guards the new form only.
func TestStreamGroupGaugeConcurrent(t *testing.T) {
	t.Parallel()
	m := newMetrics()
	l := newStreamLimits(streamLimitsConfig{}, m, nil)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				if l.reserveGroup(streamGroupPullSync) {
					l.releaseGroup(streamGroupPullSync)
				}
			}
		}()
	}
	wg.Wait()
	if g := groupGaugeValue(t, m, "pullsync"); g != 0 {
		t.Fatalf("pullsync gauge %v after every stream closed, want 0", g)
	}
}
