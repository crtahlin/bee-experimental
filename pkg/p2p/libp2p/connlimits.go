// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package libp2p

import (
	"net/netip"
	"time"

	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	libp2prate "github.com/libp2p/go-libp2p/x/rate"
)

// Defaults of the per-IP connection limits. An IPv6 address is limited
// per /56 subnet, so that one host cannot avoid the limit by using many
// addresses from its own block.
const (
	defaultMaxConnectionsPerIP  = 200
	defaultConnectionRatePerIP  = 10.0
	defaultConnectionBurstPerIP = 40
)

// perIPLimits holds the resource-manager limits for one IP address: the
// number of open connections, and the rate of new ones.
type perIPLimits struct {
	ipv4Conns []rcmgr.ConnLimitPerSubnet
	ipv6Conns []rcmgr.ConnLimitPerSubnet
	rate      *libp2prate.Limiter
}

// buildPerIPLimits returns the per-IP limits for the configured values. A
// value of zero or below uses the default.
//
// Localhost stays exempt from both limits. The rate limiter exempts it
// explicitly below. The connection count leaves it to the resource
// manager's default network-prefix limits, which allow unlimited
// connections from localhost, because only the per-subnet limits are
// replaced here.
func buildPerIPLimits(maxConns int, rate float64, burst int) perIPLimits {
	if maxConns <= 0 {
		maxConns = defaultMaxConnectionsPerIP
	}
	if rate <= 0 {
		rate = defaultConnectionRatePerIP
	}
	if burst <= 0 {
		burst = defaultConnectionBurstPerIP
	}

	limit := libp2prate.Limit{RPS: rate, Burst: burst}
	return perIPLimits{
		ipv4Conns: []rcmgr.ConnLimitPerSubnet{{PrefixLength: 32, ConnCount: maxConns}},
		ipv6Conns: []rcmgr.ConnLimitPerSubnet{{PrefixLength: 56, ConnCount: maxConns}},
		rate: &libp2prate.Limiter{
			NetworkPrefixLimits: []libp2prate.PrefixLimit{
				{Prefix: netip.MustParsePrefix("127.0.0.0/8"), Limit: libp2prate.Limit{}},
				{Prefix: netip.MustParsePrefix("::1/128"), Limit: libp2prate.Limit{}},
			},
			GlobalLimit: libp2prate.Limit{}, // no global limit
			SubnetRateLimiter: libp2prate.SubnetLimiter{
				IPv4SubnetLimits: []libp2prate.SubnetLimit{{PrefixLength: 32, Limit: limit}},
				IPv6SubnetLimits: []libp2prate.SubnetLimit{{PrefixLength: 56, Limit: limit}},
				// How long the state of an address is kept after it
				// becomes inactive.
				GracePeriod: 10 * time.Second,
			},
		},
	}
}

// options returns the resource-manager options that apply the limits.
func (l perIPLimits) options() []rcmgr.Option {
	return []rcmgr.Option{
		rcmgr.WithLimitPerSubnet(l.ipv4Conns, l.ipv6Conns),
		rcmgr.WithConnRateLimiters(l.rate),
	}
}
