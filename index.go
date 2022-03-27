package archivedb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/millken/archivedb/internal/mmap"
)

/*
 +-------------+-------------+-------------+
 | Hash(8B)    | segment(2B) | Offset(4B)  |
 +-------------+-------------+-------------+
*/
const (
	bucketsCount = 512

	indexItemSize = 14
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
}
func (b *bucket) Set(k uint64, item item) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items[k] = item
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
	data    []byte
	buckets [bucketsCount]bucket
	w       *bufio.Writer
	total   int64
}

func openIndex(filePath string) (*index, error) {
	// Open file handler for writing & seek to end of data.
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	} else if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	idx := &index{
		path: filePath,
		file: file,
		w:    bufio.NewWriterSize(file, 32*1024),
	}
	atomic.StoreInt64(&idx.total, fi.Size()/indexItemSize)
	// Memory map file data.
	if idx.data, err = mmap.Map(idx.path, fi.Size()); err != nil {
		return nil, err
	}
	for i := range idx.buckets[:] {
		idx.buckets[i].Init()
	}

	return idx, idx.load()
}

func (idx *index) load() error {
	for i := int64(0); i < atomic.LoadInt64(&idx.total); i++ {
		b := idx.data[i*indexItemSize : (i+1)*indexItemSize]
		keyHash := intconv.Uint64(b[0:8])
		id := intconv.Uint16(b[8:10])
		offset := intconv.Uint32(b[10:14])
		idx.Insert(keyHash, id, offset)
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
	bid := k % bucketsCount
	var b [indexItemSize]byte
	intconv.PutUint64(b[0:8], k)
	intconv.PutUint16(b[8:10], segmentID)
	intconv.PutUint32(b[10:14], off)
	n, err := idx.w.Write(b[:])
	if err != nil {
		return err
	} else if n != indexItemSize {
		return io.ErrShortWrite
	} else if err = idx.buckets[bid].Set(k, item{segmentID, off}); err != nil {
		return err
	}
	atomic.AddInt64(&idx.total, 1)
	return nil
}

func (idx *index) Get(k uint64) (item, bool) {
	return idx.get(k)
}

func (idx *index) Flush() error {
	if idx.w != nil {
		if err := idx.w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) Close() error {
	if err := idx.Flush(); err != nil {
		return err
	}
	return idx.file.Close()
}
