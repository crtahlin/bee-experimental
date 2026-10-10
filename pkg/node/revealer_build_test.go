// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package node

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/storageincentives"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
)

const testBuildBlockTime = 5 * time.Second

// unsyncedChain is a chain backend whose block number follows the
// (synctest) clock and whose latest header is an hour old, so the early
// reveal keeps waiting for it to sync.
type unsyncedChain struct {
	start time.Time
	base  uint64
	reads atomic.Int64
}

func (c *unsyncedChain) block() uint64 {
	return c.base + uint64(time.Since(c.start)/testBuildBlockTime)
}

func (c *unsyncedChain) BlockNumber(context.Context) (uint64, error) {
	c.reads.Add(1)
	return c.block(), nil
}

func (c *unsyncedChain) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	c.reads.Add(1)
	return &types.Header{Number: new(big.Int).SetUint64(c.block()), Time: uint64(time.Now().Add(-time.Hour).Unix())}, nil
}

// A build error after the Revealer is started: the early reveal is
// registered in the node, so the build's error shutdown closes it, and it
// reads the chain no more afterwards (#737).
func TestBuildErrorClosesEarlyReveal(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const round = 1
		commitPhase := round * storageincentives.DefaultBlocksPerRound
		state, err := storageincentives.NewRedistributionState(log.Noop, common.Address{}, mock.NewStateStore(), nil, transactionmock.New())
		if err != nil {
			t.Fatal(err)
		}
		if err := state.SetCommitKey(round, []byte("key")); err != nil {
			t.Fatal(err)
		}
		state.SetCurrentBlock(uint64(commitPhase + 2))

		chain := &unsyncedChain{start: time.Now(), base: uint64(commitPhase + 2)}
		revealer := storageincentives.NewRevealer(state, nil, transactionmock.New(), chain, func() time.Duration { return testBuildBlockTime }, storageincentives.DefaultBlocksPerRound, storageincentives.DefaultBlocksPerPhase, log.Noop)

		b := &Bee{logger: log.Noop, ctxCancel: func() {}}
		// no stop wait, so the shutdown reaches the Revealer's close
		// while the early reveal still polls
		b.startRevealer(revealer, false)
		if b.revealGuard != revealer {
			t.Fatal("Revealer not registered in the node")
		}

		time.Sleep(3 * testBuildBlockTime)
		synctest.Wait() // the early reveal polls the unsynced backend

		b.shutdownAfterBuildError(errors.New("api listener: address in use"))

		// the early reveal would poll until the round's reveal phase ends
		// by its estimate, far beyond these blocks
		if chain.block() >= uint64(commitPhase+storageincentives.DefaultBlocksPerPhase) {
			t.Fatal("the test ran into the reveal phase; it needs an early reveal still polling")
		}
		reads := chain.reads.Load()
		time.Sleep(5 * testBuildBlockTime)
		synctest.Wait()
		if got := chain.reads.Load(); got != reads {
			t.Fatalf("the early reveal still reads the chain after the build's shutdown (%d reads, was %d)", got, reads)
		}
	})
}

// The early reveal starts right after the chain is initialised and before
// the postage catch-up (#737). A source-order check: a node build needs a
// chain endpoint and is not run in unit tests.
func TestRevealerStartsBeforePostageCatchUp(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "node.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	first := map[string]token.Pos{}
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if ok && fn.Name.Name != "NewBee" {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var name string
		switch f := call.Fun.(type) {
		case *ast.Ident:
			name = f.Name
		case *ast.SelectorExpr:
			if x, ok := f.X.(*ast.Ident); ok {
				name = x.Name + "." + f.Sel.Name
			}
		}
		if _, seen := first[name]; name != "" && !seen {
			first[name] = call.Pos()
		}
		return true
	})

	order := []string{"InitChain", "b.startRevealer", "batchSvc.Start"}
	for i, name := range order {
		if first[name] == token.NoPos {
			t.Fatalf("%s not called in NewBee", name)
		}
		if i > 0 && first[order[i-1]] >= first[name] {
			t.Fatalf("%s at %s is not before %s at %s", order[i-1], fset.Position(first[order[i-1]]), name, fset.Position(first[name]))
		}
	}
}
