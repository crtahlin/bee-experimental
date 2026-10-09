// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pullsync

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// SetAfterMakeOffer replaces the hook that runs between building an offer and
// unregistering the waiting request, and returns a function that restores it.
func SetAfterMakeOffer(f func()) (restore func()) {
	prev := afterMakeOffer
	afterMakeOffer = f
	return func() { afterMakeOffer = prev }
}

// RequestsReplaced returns the replaced-request counter for a reason.
func (s *Syncer) RequestsReplaced(reason string) float64 {
	return metricValue(s.metrics.RequestsReplaced.WithLabelValues(reason))
}

// WaitingRequests returns the waiting-request gauge.
func (s *Syncer) WaitingRequests() float64 {
	return metricValue(s.metrics.WaitingRequests)
}

// WaitingTracked returns the number of tracked waiting requests.
func (s *Syncer) WaitingTracked() int {
	return s.waiting.count()
}

func metricValue(m prometheus.Metric) float64 {
	var d dto.Metric
	if err := m.Write(&d); err != nil {
		return -1
	}
	if c := d.GetCounter(); c != nil {
		return c.GetValue()
	}
	return d.GetGauge().GetValue()
}
