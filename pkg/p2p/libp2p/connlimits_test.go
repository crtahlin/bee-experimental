// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p_test

import (
	"testing"

	"github.com/ethersphere/bee/v2/pkg/p2p/libp2p"
	libp2prate "github.com/libp2p/go-libp2p/x/rate"
)

func TestPerIPLimits(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                string
		maxConns, burst     int
		rate                float64
		wantConns, wantBurs int
		wantRate            float64
	}{
		{name: "defaults", wantConns: 200, wantRate: 10, wantBurs: 40},
		{name: "configured", maxConns: 500, rate: 25, burst: 100, wantConns: 500, wantRate: 25, wantBurs: 100},
		{name: "below zero uses defaults", maxConns: -1, rate: -1, burst: -1, wantConns: 200, wantRate: 10, wantBurs: 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ipv4, ipv6, limiter := libp2p.PerIPLimits(tc.maxConns, tc.rate, tc.burst)

			if len(ipv4) != 1 || ipv4[0].PrefixLength != 32 || ipv4[0].ConnCount != tc.wantConns {
				t.Errorf("got IPv4 connection limits %+v, want %d per /32", ipv4, tc.wantConns)
			}
			if len(ipv6) != 1 || ipv6[0].PrefixLength != 56 || ipv6[0].ConnCount != tc.wantConns {
				t.Errorf("got IPv6 connection limits %+v, want %d per /56", ipv6, tc.wantConns)
			}

			want := libp2prate.Limit{RPS: tc.wantRate, Burst: tc.wantBurs}
			v4 := limiter.SubnetRateLimiter.IPv4SubnetLimits
			if len(v4) != 1 || v4[0].PrefixLength != 32 || v4[0].Limit != want {
				t.Errorf("got IPv4 rate limits %+v, want %+v per /32", v4, want)
			}
			v6 := limiter.SubnetRateLimiter.IPv6SubnetLimits
			if len(v6) != 1 || v6[0].PrefixLength != 56 || v6[0].Limit != want {
				t.Errorf("got IPv6 rate limits %+v, want %+v per /56", v6, want)
			}

			// Localhost stays exempt from the rate limit.
			exempt := map[string]bool{}
			for _, p := range limiter.NetworkPrefixLimits {
				if p.Limit == (libp2prate.Limit{}) {
					exempt[p.Prefix.String()] = true
				}
			}
			if !exempt["127.0.0.0/8"] || !exempt["::1/128"] {
				t.Errorf("localhost not exempt from the rate limit: %+v", limiter.NetworkPrefixLimits)
			}
		})
	}
}
