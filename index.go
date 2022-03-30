package archivedb

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

/*
 +-------------+-------------+-------------+
 | Hash(8B)    | segment(2B) | Offset(4B)  |
 +-------------+-------------+-------------+
*/
const (
	bucketsCount  = 512
	indexBlock    = 14 << 16
	indexItemSize = 14
	indexMagic    = "ArIdX"
)

var intconv = binary.BigEndian

type item struct {
	id  uint16
	off uint32
}

func (it item) Offset() uint32 {
	return it.off
}

func (it item) ID() uint16 {
	return it.id
}

type bucket struct {
	items map[uint64]item
	mu    sync.RWMutex
}

func (b *bucket) Init() {
	b.items = make(map[uint64]item)
	b.Reset()
}

func (b *bucket) Reset() {
	b.mu.Lock()
	bm := b.items
	for k := range bm {
		delete(bm, k)
	}
	b.mu.Unlock()
}

func (b *bucket) Set(k uint64, item item) error {
	b.mu.Lock()
	b.items[k] = item
	b.mu.Unlock()
	return nil
}

func (b *bucket) Get(k uint64) (item, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if it, ok := b.items[k]; ok {
		return it, ok
	}
	return item{}, false
}

type index struct {
	path    string
	file    *os.File
	data    mmap.MMap
	buckets [bucketsCount]bucket
	total   int64
	size    int
}

func openIndex(filePath string) (*index, error) {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open index file")
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "failed to stat index file")
	}
	if fi.Size() == 0 {
		if err := f.Truncate(indexBlock); err != nil {
			return nil, errors.Wrap(err, "failed to truncate index file")
		}
	}
	idx := &index{
		path: filePath,
		file: f,
	}
	if idx.data, err = mmap.Map(f, mmap.RDWR, 0); err != nil {
		return nil, errors.Wrap(err, "failed to mmap index file")
	}
	atomic.StoreInt64(&idx.total, 0)

	for i := range idx.buckets[:] {
		idx.buckets[i].Init()
	}

	return idx, idx.load()
}

func (idx *index) load() error {
	if idx.data == nil {
		return nil
	}
	for idx.size = 0; idx.size < len(idx.data); {
		b := idx.data[idx.size : idx.size+indexItemSize]
		key := intconv.Uint64(b[0:8])
		id := intconv.Uint16(b[8:10])
		offset := intconv.Uint32(b[10:14])
		if key == 0 {
			break
		}
		if key == 65538 {
			_ = key
		}
		if err := idx.Insert(key, id, offset); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) Length() int64 {
	return atomic.LoadInt64(&idx.total)
}

func (idx *index) get(k uint64) (item, bool) {
	bid := k % bucketsCount
	return idx.buckets[bid].Get(k)
}

func (idx *index) Insert(k uint64, segmentID uint16, off uint32) error {
	var err error
	if k == 65538 {
		_ = k
	}
	bid := k % bucketsCount
	var b [indexItemSize]byte
	intconv.PutUint64(b[0:8], k)
	intconv.PutUint16(b[8:10], segmentID)
	intconv.PutUint32(b[10:14], off)
	n := copy(idx.data[idx.size:], b[:])
	if n != indexItemSize {
		return io.ErrShortWrite
	} else if err = idx.buckets[bid].Set(k, item{segmentID, off}); err != nil {
		return err
	}
	idx.size += indexItemSize
	atomic.AddInt64(&idx.total, 1)

	// auto remap
	if atomic.LoadInt64(&idx.total)%(indexBlock/indexItemSize-100) == 0 {
		if idx.data != nil {
			if err := idx.data.Flush(); err != nil {
				return err
			} else if err := idx.data.Unmap(); err != nil {
				return err
			}
		}
		fi, err := idx.file.Stat()
		if err != nil {
			return err
		}
		if err = idx.file.Truncate(int64(fi.Size() + indexBlock)); err != nil {
			return err
		}
		if idx.data, err = mmap.Map(idx.file, mmap.RDWR, 0); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) Get(k uint64) (item, bool) {
	return idx.get(k)
}

func (idx *index) Flush() error {
	if idx.data != nil {
		if err := idx.data.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) Close() error {
	if idx.data != nil {
		if err := idx.data.Flush(); err != nil {
			return err
		} else if err := idx.data.Unmap(); err != nil {
			return err
		}
	}
	if idx.file != nil {
		if err := idx.file.Sync(); err != nil {
			return err
		} else if err := idx.file.Close(); err != nil {
			return err
		}
	}
	return nil
}
