package archivedb

import (
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/tidwall/bfile"
)

var (
	ErrCRC32    = errors.New("checksumIEEE error")
	ErrNotFound = errors.New("Not Found")
)

type DB struct {
	mu        sync.RWMutex
	index     *index
	storage   *bfile.Pager
	endOffset int64
}

func Open(filePath string) (*DB, error) {
	file, err := os.OpenFile(filePath, fsMode, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrap(err, "opening storage")
	}
	idxOpt := &indexOptions{
		filePath: filePath + ".idx",
	}
	idx, err := openIndex(idxOpt)
	if err != nil {
		return nil, errors.Wrap(err, "opening index")
	}
	db := &DB{
		index:   idx,
		storage: bfile.NewPager(file),
	}
	ep, err := db.getEndOffset()
	if err != nil {
		return nil, errors.Wrap(err, "get end offset")
	}
	db.endOffset = ep
	return db, nil
}

func (db *DB) Set(key, value []byte) error {
	h := db.hash(key)
	db.mu.Lock()
	defer db.mu.Unlock()
	n, err := db.storage.WriteAt(value, db.endOffset)
	if err != nil {
		return err
	}
	err = db.index.PutRecord(newIndexRecord(h, uint64(db.endOffset), uint32(n), NewCRC(value).Value()))
	if err != nil {
		return err
	}
	db.endOffset += int64(n)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	h := db.hash(key)
	db.mu.RLock()
	defer db.mu.RUnlock()
	id, found := db.index.hashmapGet(h)
	if !found {
		return nil, ErrNotFound
	}
	rec, err := db.index.GetRecord(id)
	if err != nil {
		return nil, err
	}
	b := make([]byte, rec.dataSize)
	n, err := db.storage.ReadAt(b, int64(rec.dataOffset))
	if err != nil {
		return nil, err
	}
	if n != int(rec.dataSize) {
		return nil, errors.New("size error")
	}
	return b, nil
}

func (db *DB) Has(key []byte) bool {
	h := db.hash(key)
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, found := db.index.hashmapGet(h)
	return found
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.index.Close(); err != nil {
		return err
	}
	if err := db.storage.Flush(); err != nil {
		return err
	}
	return nil
}

// hash = fnv32a
func (db *DB) hash(data []byte) uint32 {
	var hash uint32 = 0x811c9dc5
	for i := 0; i < len(data); i++ {
		hash ^= uint32(data[i])
		hash *= 0x01000193
	}
	return hash
}

func (db *DB) getEndOffset() (int64, error) {
	tk := db.index.totalKeys
	if tk == 0 {
		return 0, nil
	}
	ir, err := db.index.GetRecord(tk - 1)
	if err != nil {
		return 0, err
	}
	return int64(ir.dataOffset) + int64(ir.dataSize), nil
}
