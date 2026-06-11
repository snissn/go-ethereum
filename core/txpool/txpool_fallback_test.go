// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package txpool

import (
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
)

type missingStateChain struct {
	head    *types.Header
	genesis *types.Header
}

func (c *missingStateChain) Config() *params.ChainConfig { return params.TestChainConfig }

func (c *missingStateChain) CurrentBlock() *types.Header { return c.head }

func (c *missingStateChain) Genesis() *types.Block {
	return types.NewBlockWithHeader(c.genesis)
}

func (c *missingStateChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return event.NewSubscription(func(quit <-chan struct{}) error {
		<-quit
		return nil
	})
}

func (c *missingStateChain) StateAt(header *types.Header) (*state.StateDB, error) {
	return nil, errors.New("state unavailable")
}

type staticStateChain struct {
	missingStateChain
	states map[int64]*state.StateDB
}

func (c *staticStateChain) StateAt(header *types.Header) (*state.StateDB, error) {
	if statedb := c.states[header.Number.Int64()]; statedb != nil {
		return statedb, nil
	}
	return nil, errors.New("state unavailable")
}

func TestStateAtOrEmptyUsesHeadState(t *testing.T) {
	head := &types.Header{Number: big.NewInt(1)}
	genesis := &types.Header{Number: big.NewInt(0)}
	headState, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	if err != nil {
		t.Fatalf("failed to create head state: %v", err)
	}
	chain := &staticStateChain{
		missingStateChain: missingStateChain{head: head, genesis: genesis},
		states:            map[int64]*state.StateDB{head.Number.Int64(): headState},
	}
	statedb, fallback, err := StateAtOrEmpty(chain, head)
	if err != nil {
		t.Fatalf("StateAtOrEmpty returned error: %v", err)
	}
	if statedb != headState {
		t.Fatalf("expected head state")
	}
	if fallback {
		t.Fatalf("did not expect fallback to be reported")
	}
}

func TestStateAtOrEmptyFallsBackToGenesis(t *testing.T) {
	head := &types.Header{Number: big.NewInt(1)}
	genesis := &types.Header{Number: big.NewInt(0)}
	genesisState, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	if err != nil {
		t.Fatalf("failed to create genesis state: %v", err)
	}
	chain := &staticStateChain{
		missingStateChain: missingStateChain{head: head, genesis: genesis},
		states:            map[int64]*state.StateDB{genesis.Number.Int64(): genesisState},
	}
	statedb, fallback, err := StateAtOrEmpty(chain, head)
	if err != nil {
		t.Fatalf("StateAtOrEmpty returned error: %v", err)
	}
	if statedb != genesisState {
		t.Fatalf("expected genesis state")
	}
	if !fallback {
		t.Fatalf("expected fallback to be reported")
	}
}

func TestStateAtOrEmptyFallsBackWhenHeadAndGenesisMissing(t *testing.T) {
	chain := &missingStateChain{
		head:    &types.Header{Number: big.NewInt(1)},
		genesis: &types.Header{Number: big.NewInt(0)},
	}
	statedb, fallback, err := StateAtOrEmpty(chain, chain.head)
	if err != nil {
		t.Fatalf("StateAtOrEmpty returned error: %v", err)
	}
	if statedb == nil {
		t.Fatalf("expected empty fallback state")
	}
	if !fallback {
		t.Fatalf("expected fallback to be reported")
	}
}

type delayedHeadStateChain struct {
	missingStateChain
	headState *state.StateDB
	headCalls atomic.Int32
}

func (c *delayedHeadStateChain) StateAt(header *types.Header) (*state.StateDB, error) {
	if header == c.head && c.headCalls.Add(1) > 1 {
		return c.headState, nil
	}
	return nil, errors.New("state unavailable")
}

func TestTxPoolRetriesHeadStateAfterStartupFallback(t *testing.T) {
	oldInterval := headStateRetryInterval
	headStateRetryInterval = 10 * time.Millisecond
	defer func() { headStateRetryInterval = oldInterval }()

	headState, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
	if err != nil {
		t.Fatalf("failed to create head state: %v", err)
	}
	chain := &delayedHeadStateChain{
		missingStateChain: missingStateChain{
			head:    &types.Header{Number: big.NewInt(1)},
			genesis: &types.Header{Number: big.NewInt(0)},
		},
		headState: headState,
	}
	pool, err := New(0, chain, nil)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer pool.Close()

	deadline := time.After(time.Second)
	for {
		pool.stateLock.RLock()
		loaded := pool.state == headState
		pool.stateLock.RUnlock()
		if loaded {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("txpool did not retry and load head state")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
