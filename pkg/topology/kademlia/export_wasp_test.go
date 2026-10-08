// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kademlia

import dto "github.com/prometheus/client_model/go"

// DepthRecalculations returns how many times the depth was recalculated.
func (k *Kad) DepthRecalculations() float64 {
	var m dto.Metric
	if err := k.metrics.DepthRecalculations.Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}
