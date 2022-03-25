package archivedb

import (
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/tidwall/bfile"
)

/*
index record
+---------------+----------------+
|Hash key (8B)  |Entry seek (8B) |
+---------------+----------------+
*/
const (
	idxRecordSize = 16
	fsMode        = os.O_RDWR | os.O_CREATE
)

var intconv = binary.BigEndian

type item struct {
	id  int64
	off int64
}

func (it *item) Offset() int64 {
	return it.off
}

func (it *item) ID() int64 {
	return it.id
}

type index struct {
	file        *os.File
	pager       *bfile.Pager
	hashmap     map[uint64]*item
	hashmapLock sync.RWMutex
	total       int64
}

func openIndex(filePath string) (*index, error) {
	file, err := os.OpenFile(filePath, fsMode, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrap(err, "open index file")
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat index file")
	}
	idx := &index{
		file:  file,
		pager: bfile.NewPager(file),
	}
	if stat.Size() == 0 {
		if err := idx.writeMeta(); err != nil {
			return nil, errors.Wrap(err, "write index file")
		}
	}
	if err := idx.readMeta(); err != nil {
		return nil, errors.Wrap(err, "read index file")
	}

	return idx, idx.load(idx.Length())
}

func (idx *index) readMeta() error {
	var b [idxRecordSize]byte
	n, err := idx.pager.ReadAt(b[:], 0)
	if err != nil {
		return err
	}
	if n != idxRecordSize {
		return errors.New("index record size error")
	}
	atomic.StoreInt64(&idx.total, int64(intconv.Uint64(b[0:8])))
	return nil
}

func (idx *index) writeMeta() error {
	var b [idxRecordSize]byte
	intconv.PutUint64(b[0:8], uint64(idx.Length()))
	intconv.PutUint64(b[8:16], 0)
	n, err := idx.pager.WriteAt(b[:], 0)
	if err != nil {
		return err
	}
	if n != idxRecordSize {
		return errors.New("index record size error")
	}
	return nil
}

func (idx *index) load(total int64) error {
	idx.hashmapLock.Lock()
	defer idx.hashmapLock.Unlock()
	var b [idxRecordSize]byte
	idx.hashmap = make(map[uint64]*item, total)
	for i := int64(1); i <= total; i++ {
		n, err := idx.pager.ReadAt(b[:], i*idxRecordSize)
		if err != nil {
			return err
		}
		if n != idxRecordSize {
			return errors.New("index record size error")
		}
		keyHash := intconv.Uint64(b[0:8])
		dataOffset := intconv.Uint64(b[8:16])
		idx.hashmap[keyHash] = &item{
			id:  i,
			off: int64(dataOffset),
		}

	}
	atomic.StoreInt64(&idx.total, total)
	return nil
}

func (idx *index) Length() int64 {
	return atomic.LoadInt64(&idx.total)
}

func (idx *index) get(k uint64) (*item, bool) {
	if item, ok := idx.hashmap[k]; ok {
		return item, ok
	}
	return nil, false
}

func (idx *index) Set(k uint64, off int64) error {
	idx.hashmapLock.Lock()
	defer idx.hashmapLock.Unlock()
	it, ok := idx.get(k)
	if ok {
		it.off = off
	} else {
		it = &item{
			id:  atomic.AddInt64(&idx.total, 1),
			off: off,
		}
		idx.hashmap[k] = it
	}
	var b [idxRecordSize]byte
	intconv.PutUint64(b[0:8], k)
	intconv.PutUint64(b[8:16], uint64(it.off))

	n, err := idx.pager.WriteAt(b[:], it.ID()*idxRecordSize)
	if err != nil {
		return err
	}
	if n != idxRecordSize {
		return errors.New("index record size error")
	}
	return idx.writeMeta()
}

func (idx *index) Get(k uint64) (*item, bool) {
	idx.hashmapLock.RLock()
	defer idx.hashmapLock.RUnlock()
	return idx.get(k)
}

func (idx *index) Sync() error {
	return idx.pager.Flush()
}

func (idx *index) Close() error {
	if err := idx.pager.Flush(); err != nil {
		return err
	}
	return idx.file.Close()
}
