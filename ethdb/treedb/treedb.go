package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	treedb "github.com/snissn/gomap/TreeDB"
)

var (
	errKeyEmpty    = errors.New("treedb: key empty")
	errValueNil    = errors.New("treedb: value nil")
	errBatchClosed = errors.New("treedb: batch closed")
)

// Defaults for TreeDB performance tuning
const (
	defaultFlushThreshold = 32 * 1024 * 1024 // 32MB write buffer
)

// Database implements ethdb.KeyValueStore
type Database struct {
	fn       string
	db       *treedb.DB
	log      log.Logger
	quitLock sync.Mutex
	closed   bool
}

// New returns a wrapped TreeDB object.
func New(file string, cache int, handles int, namespace string, readonly bool) (*Database, error) {
	openOpts := treedb.Options{
		Dir:            file,
		FlushThreshold: int64(defaultFlushThreshold),
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
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true
	return db.db.Close()
}

func (db *Database) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, nil
	}
	return db.db.Has(key)
}

func (db *Database) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, nil
	}
	value, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return common.CopyBytes(value), nil
}

func (db *Database) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		value = []byte{}
	}
	return db.db.Set(key, value)
}

func (db *Database) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	return db.db.Delete(key)
}

func (db *Database) DeleteRange(start, end []byte) error {
	batch := db.NewBatch()
	it := db.NewIterator(nil, start)
	defer it.Release()

	for it.Next() {
		if end != nil && bytes.Compare(it.Key(), end) >= 0 {
			break
		}
		if err := batch.Delete(it.Key()); err != nil {
			return err
		}
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	return batch.Write()
}

func (db *Database) NewBatch() ethdb.Batch {
	return newBatch(db)
}

func (db *Database) NewBatchWithSize(size int) ethdb.Batch {
	return newBatch(db)
}

func (db *Database) Stat() (string, error) {
	stats := db.db.Stats()
	return fmt.Sprintf("%v", stats), nil
}

func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

func (db *Database) Path() string {
	return db.fn
}

func (db *Database) SyncKeyValue() error {
	return nil
}

// Batch implementation

type batch struct {
	db       *Database
	ops      []batchOp
	byteSize int
	lock     sync.Mutex
}

type batchOp struct {
	isDelete bool
	key      []byte
	value    []byte
}

func newBatch(db *Database) *batch {
	return &batch{db: db}
}

func (b *batch) Put(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		value = []byte{}
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.ops = append(b.ops, batchOp{false, common.CopyBytes(key), common.CopyBytes(value)})
	b.byteSize += len(key) + len(value)
	return nil
}

func (b *batch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.ops = append(b.ops, batchOp{true, common.CopyBytes(key), nil})
	b.byteSize += len(key)
	return nil
}

func (b *batch) DeleteRange(start, end []byte) error {
	it := b.db.NewIterator(nil, start)
	defer it.Release()
	for it.Next() {
		if end != nil && bytes.Compare(it.Key(), end) >= 0 {
			break
		}
		b.Delete(it.Key())
	}
	return nil
}

func (b *batch) ValueSize() int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.byteSize
}

func (b *batch) Write() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.db.closed {
		return errBatchClosed
	}

	tb := b.db.db.NewBatch()
	if tb == nil {
		return errBatchClosed
	}
	defer func() { _ = tb.Close() }()

	for _, op := range b.ops {
		if op.isDelete {
			if err := tb.Delete(op.key); err != nil {
				return err
			}
		} else {
			if err := tb.Set(op.key, op.value); err != nil {
				return err
			}
		}
	}

	return tb.Write()
}

func (b *batch) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.ops = nil
	b.byteSize = 0
}

func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	for _, op := range b.ops {
		if op.isDelete {
			if err := w.Delete(op.key); err != nil {
				return err
			}
		} else {
			if err := w.Put(op.key, op.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// Iterator implementation

type iterator struct {
	iter  treedb.Iterator
	start []byte
	end   []byte
	first bool
	valid bool
	err   error
}

func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	fullStart := append(prefix, start...)
	if len(fullStart) == 0 {
		fullStart = nil
	}
	fullEnd := upperBound(prefix)
	if len(fullEnd) == 0 {
		fullEnd = nil
	}

	itInt, err := db.db.Iterator(fullStart, fullEnd)
	if err != nil {
		return &iterator{err: err}
	}

	return &iterator{
		iter:  itInt,
		start: fullStart,
		end:   fullEnd,
		first: true,
	}
}

func (it *iterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.iter == nil {
		return false
	}

	if it.first {
		it.first = false
		it.valid = it.iter.Valid()
	} else {
		it.iter.Next()
		it.valid = it.iter.Valid()
	}
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
	return common.CopyBytes(it.iter.Key())
}

func (it *iterator) Value() []byte {
	if !it.valid || it.iter == nil {
		return nil
	}
	return common.CopyBytes(it.iter.Value())
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
