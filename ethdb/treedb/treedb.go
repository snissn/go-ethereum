package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbbatch "github.com/snissn/gomap/TreeDB/batch"
)

var (
	errKeyEmpty    = errors.New("treedb: key empty")
	errBatchClosed = errors.New("treedb: batch closed")
)

func concatBytes(a, b []byte) []byte {
	if len(a) == 0 {
		if len(b) == 0 {
			return nil
		}
		return append([]byte(nil), b...)
	}
	if len(b) == 0 {
		return append([]byte(nil), a...)
	}
	out := make([]byte, len(a)+len(b))
	copy(out, a)
	copy(out[len(a):], b)
	return out
}

// Database implements ethdb.KeyValueStore.
type Database struct {
	fn  string
	db  *treedb.DB
	log log.Logger

	closeOnce sync.Once
	closeErr  error
}

// New returns a wrapped TreeDB object.
//
// Note: arguments cache/handles/namespace/readonly exist for compatibility with
// other geth DB backends. TreeDB currently ignores them.
func New(file string, cache int, handles int, namespace string, readonly bool) (*Database, error) {
	openOpts := treedb.Options{
		Dir:  file,
		Mode: treedb.ModeCached,
	}

	tdb, err := treedb.Open(openOpts)
	if err != nil {
		return nil, err
	}

	return &Database{
		fn:  file,
		db:  tdb,
		log: log.New("database", file),
	}, nil
}

func (db *Database) Close() error {
	if db == nil {
		return nil
	}
	db.closeOnce.Do(func() {
		if db.db != nil {
			db.closeErr = db.db.Close()
		}
		db.db = nil
	})
	return db.closeErr
}

func (db *Database) Has(key []byte) (bool, error) {
	if db == nil || db.db == nil {
		return false, treedb.ErrClosed
	}
	if key == nil {
		return false, errKeyEmpty
	}
	return db.db.Has(key)
}

func (db *Database) Get(key []byte) ([]byte, error) {
	if db == nil || db.db == nil {
		return nil, treedb.ErrClosed
	}
	if key == nil {
		return nil, errKeyEmpty
	}
	return db.db.Get(key)
}

func (db *Database) Put(key []byte, value []byte) error {
	if db == nil || db.db == nil {
		return treedb.ErrClosed
	}
	if key == nil {
		return errKeyEmpty
	}
	if value == nil {
		value = []byte{}
	}
	return db.db.Set(key, value)
}

func (db *Database) Delete(key []byte) error {
	if db == nil || db.db == nil {
		return treedb.ErrClosed
	}
	if key == nil {
		return errKeyEmpty
	}
	return db.db.Delete(key)
}

func (db *Database) DeleteRange(start, end []byte) error {
	if db == nil || db.db == nil {
		return treedb.ErrClosed
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}

	it, err := db.db.Iterator(start, end)
	if err != nil {
		return err
	}
	defer it.Close()

	var (
		tb       = db.db.NewBatch()
		byteSize int
	)
	if tb == nil {
		return errBatchClosed
	}
	defer func() { _ = tb.Close() }()

	for it.Valid() {
		k := it.Key() // TreeDB iterator returns a copy; safe to pass to TreeDB batch.
		if err := tb.Delete(k); err != nil {
			return err
		}
		byteSize += len(k)

		if byteSize > ethdb.IdealBatchSize {
			if err := tb.Write(); err != nil {
				return err
			}
			_ = tb.Close()
			tb = db.db.NewBatch()
			if tb == nil {
				return errBatchClosed
			}
			byteSize = 0
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return tb.Write()
}

func (db *Database) NewBatch() ethdb.Batch {
	return newBatch(db, 0)
}

func (db *Database) NewBatchWithSize(size int) ethdb.Batch {
	return newBatch(db, size)
}

func (db *Database) Stat() (string, error) {
	if db == nil || db.db == nil {
		return "", treedb.ErrClosed
	}
	return fmt.Sprintf("%v", db.db.Stats()), nil
}

func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

func (db *Database) Path() string { return db.fn }

func (db *Database) SyncKeyValue() error { return nil }

// Batch implementation.

type batch struct {
	db *Database

	tb treedb.Batch
}

func newBatch(db *Database, sizeHint int) *batch {
	b := &batch{db: db}
	if db != nil && db.db != nil {
		if sizeHint > 0 {
			b.tb = db.db.NewBatchWithSize(sizeHint)
		} else {
			b.tb = db.db.NewBatch()
		}
	}
	return b
}

func (b *batch) Put(key, value []byte) error {
	if key == nil {
		return errKeyEmpty
	}
	if value == nil {
		value = []byte{}
	}
	if b.tb != nil {
		return b.tb.Set(key, value)
	}
	return nil
}

func (b *batch) Delete(key []byte) error {
	if key == nil {
		return errKeyEmpty
	}
	if b.tb != nil {
		return b.tb.Delete(key)
	}
	return nil
}

func (b *batch) DeleteRange(start, end []byte) error {
	it := b.db.NewIterator(nil, start)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		if end != nil && bytes.Compare(k, end) >= 0 {
			break
		}
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return it.Error()
}

func (b *batch) ValueSize() int {
	if b.tb == nil {
		return 0
	}
	s, _ := b.tb.GetByteSize()
	return s
}

func (b *batch) Write() error {
	if b.db == nil || b.db.db == nil {
		return errBatchClosed
	}
	if b.tb == nil {
		return errBatchClosed
	}

	err := b.tb.Write()
	if err == nil {
		b.Reset()
	}
	return err
}

func (b *batch) Reset() {
	if b.tb != nil {
		_ = b.tb.Close()
	}
	if b.db != nil && b.db.db != nil {
		b.tb = b.db.db.NewBatch()
	} else {
		b.tb = nil
	}
}

func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	if b.tb == nil {
		return nil
	}
	return b.tb.Replay(func(e treedbbatch.Entry) error {
		if e.Type == treedbbatch.OpDelete {
			return w.Delete(e.Key)
		}
		return w.Put(e.Key, e.Value)
	})
}

// Iterator implementation.

type iterator struct {
	iter treedb.Iterator
	err  error

	first bool
	valid bool

	keyLoaded bool
	valLoaded bool
	key       []byte
	val       []byte
}

func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	if db == nil || db.db == nil {
		return &iterator{err: treedb.ErrClosed}
	}

	fullStart := concatBytes(prefix, start)
	fullEnd := upperBound(prefix)

	itInt, err := db.db.Iterator(fullStart, fullEnd)
	if err != nil {
		return &iterator{err: err}
	}

	return &iterator{
		iter:  itInt,
		first: true,
	}
}

func (it *iterator) Next() bool {
	if it.err != nil || it.iter == nil {
		return false
	}
	if it.first {
		it.first = false
		it.valid = it.iter.Valid()
	} else {
		it.iter.Next()
		it.valid = it.iter.Valid()
	}
	it.keyLoaded = false
	it.valLoaded = false
	it.key = nil
	it.val = nil
	return it.valid
}

func (it *iterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.iter != nil {
		return it.iter.Error()
	}
	return nil
}

func (it *iterator) Key() []byte {
	if !it.valid || it.iter == nil {
		return nil
	}
	if !it.keyLoaded {
		it.key = it.iter.Key()
		it.keyLoaded = true
	}
	return it.key
}

func (it *iterator) Value() []byte {
	if !it.valid || it.iter == nil {
		return nil
	}
	if !it.valLoaded {
		it.val = it.iter.Value()
		it.valLoaded = true
	}
	return it.val
}

func (it *iterator) Release() {
	if it.iter != nil {
		_ = it.iter.Close()
		it.iter = nil
	}
}

func upperBound(prefix []byte) (limit []byte) {
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c == 0xff {
			continue
		}
		limit = make([]byte, i+1)
		copy(limit, prefix)
		limit[i] = c + 1
		break
	}
	return limit
}
