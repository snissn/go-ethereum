package treedb

import (
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
)

func BenchmarkTreeDB(b *testing.B) {
	dbtest.BenchDatabaseSuite(b, func() ethdb.KeyValueStore {
		dir, err := os.MkdirTemp("", "treedb-bench")
		if err != nil {
			b.Fatal(err)
		}
		db, err := New(dir, 0, 0, "", false)
		if err != nil {
			b.Fatal(err)
		}
		return db
	})
}
