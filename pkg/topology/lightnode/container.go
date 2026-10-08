// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lightnode

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/p2p"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology"
	"github.com/ethersphere/bee/v2/pkg/topology/pslice"
)

// Token identifies one slot reservation made by TryReserve. The zero
// value is never handed out.
type Token uint64

var (
	// ErrLightNodeLimit is returned by TryReserve when every light-node
	// slot is either used or reserved.
	ErrLightNodeLimit = errors.New("lightnode: light node limit reached")
	// ErrUltraLightNodeLimit is returned by TryReserve when every slot for
	// ultra-light peers is either used or reserved.
	ErrUltraLightNodeLimit = errors.New("lightnode: ultra-light node limit reached")
)

type Container struct {
	base swarm.Address
	// peerMu guards connectedPeers, disconnectedPeers and the slot ledger
	// below, so that a reservation always sees used and reserved slots
	// that belong together.
	peerMu            sync.Mutex
	connectedPeers    *pslice.PSlice
	disconnectedPeers *pslice.PSlice

	// The slot ledger. The number of used light slots is the number of
	// connected peers. ultraLight maps each connected overlay to whether
	// it was counted as ultra-light, so a disconnect frees the right
	// count. reservations maps each open token to the same flag.
	limit           int
	ultraLightLimit int
	ultraLight      map[string]bool
	usedUltraLight  int
	reservations    map[Token]bool
	reservedUltra   int
	nextToken       Token

	metrics metrics
}

func NewContainer(base swarm.Address) *Container {
	return &Container{
		base:              base,
		connectedPeers:    pslice.New(1, base),
		disconnectedPeers: pslice.New(1, base),
		ultraLight:        make(map[string]bool),
		reservations:      make(map[Token]bool),
		metrics:           newMetrics(),
	}
}

// SetLimits sets the number of light-peer slots and, within them, the
// number of slots ultra-light peers may take. Zero means no limit for
// either value. The ultra-light limit only applies when it is below the
// light limit.
func (c *Container) SetLimits(lightLimit, ultraLightLimit int) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	c.limit = lightLimit
	c.ultraLightLimit = ultraLightLimit
}

// AtLimit reports whether used and reserved light slots together have
// reached the light limit. It reserves nothing, so it is only a cheap
// first filter: TryReserve makes the binding decision.
func (c *Container) AtLimit() bool {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	return c.limit > 0 && c.connectedPeers.Length()+len(c.reservations) >= c.limit
}

// TryReserve reserves a light slot, and for an ultra-light peer also an
// ultra-light slot. It returns ErrLightNodeLimit or
// ErrUltraLightNodeLimit when no such slot is free. A reservation must be
// passed to Commit or Release exactly once.
func (c *Container) TryReserve(ultraLight bool) (Token, error) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()

	if c.limit > 0 && c.connectedPeers.Length()+len(c.reservations) >= c.limit {
		return 0, ErrLightNodeLimit
	}
	if ultraLight && c.ultraLightLimit > 0 && c.usedUltraLight+c.reservedUltra >= c.ultraLightLimit {
		return 0, ErrUltraLightNodeLimit
	}

	c.nextToken++
	token := c.nextToken
	c.reservations[token] = ultraLight
	if ultraLight {
		c.reservedUltra++
	}
	return token, nil
}

// Commit adds the overlay as a connected light peer and turns the
// reservation into a used slot. Both happen under the same lock, so a
// concurrent reservation never sees a total that is too low. It returns
// false, and adds nothing, when the token is not an open reservation.
func (c *Container) Commit(token Token, overlay swarm.Address) bool {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()

	ultraLight, ok := c.reservations[token]
	if !ok {
		return false
	}
	c.addLocked(overlay, ultraLight)
	c.releaseLocked(token)
	return true
}

// Release drops a reservation that was not committed. Releasing a token
// that is not open does nothing, so a deferred Release after a Commit is
// safe.
func (c *Container) Release(token Token) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	c.releaseLocked(token)
}

func (c *Container) releaseLocked(token Token) {
	ultraLight, ok := c.reservations[token]
	if !ok {
		return
	}
	delete(c.reservations, token)
	if ultraLight {
		c.reservedUltra--
	}
}

// Connected adds a light peer without a reservation and without checking
// any limit. Bootnode mode uses it, because a bootnode accepts every
// light peer and evicts another one when it is over its limit.
func (c *Container) Connected(ctx context.Context, peer p2p.Peer) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	c.addLocked(peer.Address, false)
}

func (c *Container) addLocked(addr swarm.Address, ultraLight bool) {
	key := addr.ByteString()
	if was, ok := c.ultraLight[key]; ok {
		// The overlay is already counted, so it does not take a second
		// slot. Only its ultra-light flag can change.
		if was {
			c.usedUltraLight--
		}
	} else {
		c.metrics.Connects.Inc()
	}
	c.ultraLight[key] = ultraLight
	if ultraLight {
		c.usedUltraLight++
	}

	c.connectedPeers.Add(addr)
	c.disconnectedPeers.Remove(addr)

	c.metrics.CurrentlyConnectedPeers.Set(float64(c.connectedPeers.Length()))
	c.metrics.CurrentlyDisconnectedPeers.Set(float64(c.disconnectedPeers.Length()))
}

// Disconnected frees the slot of a connected light peer. An overlay the
// container does not hold frees nothing. It does not look at
// peer.FullNode, which can be wrong when the remote side closed the
// connection.
func (c *Container) Disconnected(peer p2p.Peer) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()

	addr := peer.Address
	if found := c.connectedPeers.Exists(addr); found {
		c.connectedPeers.Remove(addr)
		c.disconnectedPeers.Add(addr)
		key := addr.ByteString()
		if c.ultraLight[key] {
			c.usedUltraLight--
		}
		delete(c.ultraLight, key)
		c.metrics.Disconnects.Inc()
	}

	c.metrics.CurrentlyConnectedPeers.Set(float64(c.connectedPeers.Length()))
	c.metrics.CurrentlyDisconnectedPeers.Set(float64(c.disconnectedPeers.Length()))
}

// Slots returns the used and reserved light slots, and the used and
// reserved ultra-light slots.
func (c *Container) Slots() (used, reserved, usedUltraLight, reservedUltraLight int) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	return c.connectedPeers.Length(), len(c.reservations), c.usedUltraLight, c.reservedUltra
}

func (c *Container) Count() int {
	return c.connectedPeers.Length()
}

func (c *Container) RandomPeer(not swarm.Address) (swarm.Address, error) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	var (
		cnt   = big.NewInt(int64(c.Count()))
		addr  = swarm.ZeroAddress
		count = int64(0)
	)

PICKPEER:
	i, e := rand.Int(rand.Reader, cnt)
	if e != nil {
		return swarm.ZeroAddress, e
	}
	i64 := i.Int64()

	count = 0
	_ = c.connectedPeers.EachBinRev(func(peer swarm.Address, _ uint8) (bool, bool, error) {
		if count == i64 {
			addr = peer
			return true, false, nil
		}
		count++
		return false, false, nil
	})

	if addr.Equal(not) {
		goto PICKPEER
	}

	return addr, nil
}

func (c *Container) EachPeer(pf topology.EachPeerFunc) error {
	return c.connectedPeers.EachBin(pf)
}

func (c *Container) PeerInfo() topology.BinInfo {
	return topology.BinInfo{
		BinPopulation:     uint(c.connectedPeers.Length()),
		BinConnected:      uint(c.connectedPeers.Length()),
		DisconnectedPeers: peersInfo(c.disconnectedPeers),
		ConnectedPeers:    peersInfo(c.connectedPeers),
	}
}

func peersInfo(s *pslice.PSlice) []*topology.PeerInfo {
	if s.Length() == 0 {
		return nil
	}
	peers := make([]*topology.PeerInfo, 0, s.Length())
	_ = s.EachBin(func(addr swarm.Address, po uint8) (bool, bool, error) {
		peers = append(peers, &topology.PeerInfo{Address: addr})
		return false, false, nil
	})
	return peers
}
