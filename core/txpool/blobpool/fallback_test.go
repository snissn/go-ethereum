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

package blobpool

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/billy"
	"github.com/holiman/uint256"
)

type recordingStore struct {
	deleted []uint64
}

func (s *recordingStore) Close() error                        { return nil }
func (s *recordingStore) Put(data []byte) (uint64, error)     { return 0, nil }
func (s *recordingStore) Get(key uint64) ([]byte, error)      { return nil, nil }
func (s *recordingStore) Delete(key uint64) error             { s.deleted = append(s.deleted, key); return nil }
func (s *recordingStore) Size(key uint64) uint32              { return 0 }
func (s *recordingStore) Limits() (uint32, uint32)            { return 0, 0 }
func (s *recordingStore) Infos() *billy.Infos                 { return nil }
func (s *recordingStore) Iterate(onData billy.OnDataFn) error { return nil }

func TestInitEvictionMetadataAllowsFallbackHeap(t *testing.T) {
	p := &BlobPool{
		index: map[common.Address][]*blobTxMeta{
			{0x01}: {
				{nonce: 2, execTipCap: uint256.NewInt(5), basefeeJumps: 7, blobfeeJumps: 9},
				{nonce: 3, execTipCap: uint256.NewInt(3), basefeeJumps: 6, blobfeeJumps: 8},
			},
			{0x02}: {
				{nonce: 7, execTipCap: uint256.NewInt(4), basefeeJumps: 5, blobfeeJumps: 6},
			},
		},
	}
	for addr := range p.index {
		p.initEvictionMetadata(addr)
	}
	heap := newPriceHeap(uint256.NewInt(1), uint256.NewInt(1), p.index)
	if heap.Len() != len(p.index) {
		t.Fatalf("heap length mismatch: have %d want %d", heap.Len(), len(p.index))
	}
	for addr, txs := range p.index {
		for _, tx := range txs {
			if tx.evictionExecTip == nil {
				t.Fatalf("missing eviction metadata for %v nonce %d", addr, tx.nonce)
			}
		}
	}
}

func TestInitEvictionMetadataDropsInternalNonceGap(t *testing.T) {
	addr := common.Address{0x01}
	store := new(recordingStore)
	p := &BlobPool{
		store:  store,
		lookup: newLookup(),
		index: map[common.Address][]*blobTxMeta{
			addr: {
				{id: 1, nonce: 2, costCap: uint256.NewInt(0), execTipCap: uint256.NewInt(5), basefeeJumps: 7, blobfeeJumps: 9},
				{id: 2, nonce: 4, costCap: uint256.NewInt(0), execTipCap: uint256.NewInt(3), basefeeJumps: 6, blobfeeJumps: 8},
			},
		},
		spent: map[common.Address]*uint256.Int{addr: uint256.NewInt(0)},
	}
	p.initEvictionMetadata(addr)
	if len(p.index[addr]) != 1 {
		t.Fatalf("gapped tail retained: have %d txs", len(p.index[addr]))
	}
	if len(store.deleted) != 1 || store.deleted[0] != 2 {
		t.Fatalf("deleted ids mismatch: have %v", store.deleted)
	}
	heap := newPriceHeap(uint256.NewInt(1), uint256.NewInt(1), p.index)
	if heap.Len() != 1 {
		t.Fatalf("heap length mismatch: have %d want 1", heap.Len())
	}
}
