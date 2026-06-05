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

//go:build !wasm
// +build !wasm

// Package treedb wraps the external TreeDB geth ethdb adapter behind a small
// geth-local seam. The first TreeDB engine POC is intentionally a direct engine
// case; this package keeps the external adapter dependency and platform stubs in
// one place so a future registry/hook can reuse the same opener.
package treedb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	gomaptreedb "github.com/snissn/gomap/TreeDB"
	gethethdb "github.com/snissn/gomap/TreeDB/integration/gethethdb"
)

var (
	// ErrClosed is the TreeDB adapter's closed-database sentinel.
	ErrClosed = gethethdb.ErrClosed
	// ErrNotFound is the TreeDB adapter's geth-facing not-found sentinel.
	ErrNotFound = gethethdb.ErrNotFound
	// ErrRecoveryRequired indicates TreeDB must be opened read-write to recover
	// before read-only/offline probe flows can continue.
	ErrRecoveryRequired = gomaptreedb.ErrRecoveryRequired
)

// New opens a TreeDB-backed ethdb key/value store. TreeDB intentionally ignores
// cache/handles/namespace because the adapter owns TreeDB's command-WAL durable
// profile defaults; they remain in the signature to match other geth backends.
func New(file string, cache int, handles int, namespace string, readonly bool) (ethdb.KeyValueStore, error) {
	opts := gethethdb.DefaultOpenOptions()
	opts.ReadOnly = readonly
	return gethethdb.Open(file, &opts)
}

// IsRecoveryRequired reports whether err means a read-only/offline TreeDB probe
// encountered state that must be recovered by a subsequent writable open.
func IsRecoveryRequired(err error) bool {
	return errors.Is(err, ErrRecoveryRequired)
}
