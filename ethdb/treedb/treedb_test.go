package treedb

import (
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
)

func TestTreeDB(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			dir, err := os.MkdirTemp("", "treedb-test")
			if err != nil {
				t.Fatal(err)
			}
			db, err := New(dir, 0, 0, "", false)
			if err != nil {
				t.Fatal(err)
			}
			return db
		})
	})
}
