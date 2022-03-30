package archivedb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/millken/archivedb/internal/mmap"
	"github.com/pkg/errors"
)

/*
 +-------------+-------------+-------------+
 | Hash(8B)    | segment(2B) | Offset(4B)  |
 +-------------+-------------+-------------+
*/
const (
	bucketsCount    = 512
	indexBlock      = 14 << 16
	indexItemSize   = 14
	IndexVersion    = 1
	IndexMagic      = "ArIdX"
	IndexHeaderSize = 6
)

var (
	ErrInvalidIndex        = errors.New("invalid index")
	ErrInvalidIndexVersion = errors.New("invalid index version")
	ErrIndexNotWritable    = errors.New("index not writable")
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

type indexHeader struct {
	Version uint8
}

func newIndexHeader() indexHeader {
	return indexHeader{Version: SegmentVersion}
}

// WriteTo writes the header to w.
func (hdr *indexHeader) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	buf.WriteString(IndexMagic)
	binary.Write(&buf, binary.BigEndian, hdr.Version)
	return buf.WriteTo(w)
}

func decodeIndexHeader(b []byte) (hdr indexHeader, err error) {
	if len(b) < len(IndexMagic) {
		return hdr, errors.Wrap(ErrInvalidIndex, "invalid index header")
	}
	magic := b[0:len(IndexMagic)]
	if !bytes.Equal(magic, []byte(IndexMagic)) {
		return hdr, errors.Wrap(ErrInvalidIndexVersion, "invalid magic")
	}
	hdr.Version = b[len(IndexMagic)]
	return hdr, nil
}

type index struct {
	path    string
	mapFile *mmap.MapFile
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
		// Write header to file and close.
		hdr := newIndexHeader()
		if _, err := hdr.WriteTo(f); err != nil {
			return nil, err
		} else if err := f.Sync(); err != nil {
			return nil, err
		} else if err := f.Close(); err != nil {
			return nil, err
		}
	}
	mapFile, err := mmap.Open(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to mmap index file")
	}
	idx := &index{
		path:    filePath,
		mapFile: mapFile,
	}
	atomic.StoreInt64(&idx.total, 0)

	for i := range idx.buckets[:] {
		idx.buckets[i].Init()
	}

	return idx, idx.load()
}

func (idx *index) load() error {
	b := make([]byte, indexItemSize)
	for i := IndexHeaderSize; i < int(idx.mapFile.Size()); i += indexItemSize {
		if n, err := idx.mapFile.ReadAt(b, int64(i)); err != nil {
			return errors.Wrap(err, "failed to read index")
		} else if n != indexItemSize {
			return errors.Wrap(ErrInvalidIndex, "invalid index")
		}
		key := intconv.Uint64(b[0:8])
		id := intconv.Uint16(b[8:10])
		offset := intconv.Uint32(b[10:14])

		if err := idx.set(key, id, offset); err != nil {
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

func (idx *index) set(k uint64, segmentID uint16, off uint32) error {
	bid := k % bucketsCount
	return idx.buckets[bid].Set(k, item{segmentID, off})
}

func (idx *index) Insert(k uint64, segmentID uint16, off uint32) error {
	b := make([]byte, indexItemSize)
	intconv.PutUint64(b[0:8], k)
	intconv.PutUint16(b[8:10], segmentID)
	intconv.PutUint32(b[10:14], off)
	if n, err := idx.mapFile.Write(b[:]); err != nil {
		return errors.Wrap(err, "failed to write index")
	} else if n != indexItemSize {
		return errors.Wrap(ErrInvalidIndex, "invalid index")
	} else if err = idx.set(k, segmentID, off); err != nil {
		return err
	}
	idx.size += indexItemSize
	atomic.AddInt64(&idx.total, 1)
	return nil
}

func (idx *index) Get(k uint64) (item, bool) {
	return idx.get(k)
}

func (idx *index) Flush() error {
	return idx.mapFile.Flush()
}

func (idx *index) Close() error {
	return idx.mapFile.Close()
}
