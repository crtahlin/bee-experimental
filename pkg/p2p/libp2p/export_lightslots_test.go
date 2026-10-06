// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	LightRefusalPicker     = lightRefusalPicker
	LightRefusalReserve    = lightRefusalReserve
	LightRefusalUltraLight = lightRefusalUltraLight
)

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// LightPeerRefusals returns how many light peers were refused for reason.
func (s *Service) LightPeerRefusals(reason string) float64 {
	return counterValue(s.metrics.LightPeerRefusals.WithLabelValues(reason))
}

// KickedOutPeers returns how many light peers were evicted.
func (s *Service) KickedOutPeers() float64 {
	return counterValue(s.metrics.KickedOutPeersCount)
}
