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

package rawdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreexistingDatabaseDetectsTreeDBLayouts(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "maindb"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "maindb", "index.db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := PreexistingDatabase(root); got != DBTreedb {
		t.Fatalf("PreexistingDatabase(root) = %q, want %q", got, DBTreedb)
	}

	flat := t.TempDir()
	if err := os.WriteFile(filepath.Join(flat, "index.db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := PreexistingDatabase(flat); got != DBTreedb {
		t.Fatalf("PreexistingDatabase(flat) = %q, want %q", got, DBTreedb)
	}
}

func TestPreexistingDatabaseKeepsPebbleAndLeveldbDetection(t *testing.T) {
	leveldb := t.TempDir()
	if err := os.WriteFile(filepath.Join(leveldb, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := PreexistingDatabase(leveldb); got != DBLeveldb {
		t.Fatalf("PreexistingDatabase(leveldb) = %q, want %q", got, DBLeveldb)
	}

	pebble := t.TempDir()
	if err := os.WriteFile(filepath.Join(pebble, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pebble, "OPTIONS-000001"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := PreexistingDatabase(pebble); got != DBPebble {
		t.Fatalf("PreexistingDatabase(pebble) = %q, want %q", got, DBPebble)
	}
}

func TestPreexistingDatabaseIgnoresFreshTreeDBRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "maindb"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := PreexistingDatabase(root); got != "" {
		t.Fatalf("PreexistingDatabase(fresh root) = %q, want no existing database", got)
	}
}
