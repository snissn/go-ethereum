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

//go:build wasm
// +build wasm

package treedb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
)

var (
	ErrClosed           = errors.New("treedb is unavailable on wasm: closed")
	ErrNotFound         = errors.New("treedb is unavailable on wasm: key not found")
	ErrRecoveryRequired = errors.New("treedb is unavailable on wasm: recovery required")
)

// New returns an unsupported-platform error. Persistent TreeDB is not available
// in wasm builds.
func New(file string, cache int, handles int, namespace string, readonly bool) (ethdb.KeyValueStore, error) {
	return nil, errors.New("treedb is unavailable on wasm")
}

func IsRecoveryRequired(err error) bool { return false }
