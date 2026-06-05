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

package treedb

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReadOnlyOpenMissingDirectoryDoesNotCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-treedb")
	if _, err := New(path, 0, 0, "", true); err == nil {
		t.Fatal("expected read-only open of missing TreeDB path to fail")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("read-only open mutated filesystem: stat error = %v, want IsNotExist", err)
	}
}

func TestReadOnlyOpenExistingTreeDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "treedb")
	db, err := New(path, 0, 0, "", false)
	if err != nil {
		t.Fatalf("open writable TreeDB: %v", err)
	}
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.SyncKeyValue(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close writable: %v", err)
	}

	ro, err := New(path, 0, 0, "", true)
	if err != nil {
		t.Fatalf("open read-only TreeDB: %v", err)
	}
	defer ro.Close()
	got, err := ro.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get read-only: %v", err)
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("get read-only = %q, want value", got)
	}
}
