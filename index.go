package archivedb

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/tidwall/bfile"
)

/*
index record
+---------------+----------------+---------------+----------+
|Hash key (4B)  |Data seek (8B)  |Data len (4B)  |CRC32 (4B)|
+---------------+----------------+---------------+----------+
*/
const (
	idxMetaLength = 16
	idxRecordSize = 20
	fsMode        = os.O_RDWR | os.O_CREATE
)

var intconv = binary.BigEndian

type indexOptions struct {
	filePath string
}
type indexRecord struct {
	keyHash    uint32
	dataOffset uint64
	dataSize   uint32
	dataCRC32  uint32
}

func (r *indexRecord) Bytes() []byte {
	b := acquireByte20()
	defer releaseByte20(b)
	intconv.PutUint32(b[0:4], r.keyHash)
	intconv.PutUint64(b[4:12], r.dataOffset)
	intconv.PutUint32(b[12:16], r.dataSize)
	intconv.PutUint32(b[16:20], r.dataCRC32)
	return b
}

func newIndexRecord(keyHash uint32, dataOffset uint64, dataSize uint32, dataCRC32 uint32) *indexRecord {
	ir := acquireIndexRecord()
	defer releaseIndexRecord(ir)
	ir.keyHash = keyHash
	ir.dataOffset = dataOffset
	ir.dataSize = dataSize
	ir.dataCRC32 = dataCRC32
	return ir
}

func parseIndexRecord(b []byte) (*indexRecord, error) {
	if cap(b) != blockMetaSize {
		return nil, errors.New("parse block meta length error")
	}
	ir := acquireIndexRecord()
	defer releaseIndexRecord(ir)
	ir.keyHash = intconv.Uint32(b[0:4])
	ir.dataOffset = intconv.Uint64(b[4:12])
	ir.dataSize = intconv.Uint32(b[12:16])
	ir.dataCRC32 = intconv.Uint32(b[16:20])
	return ir, nil
}

type index struct {
	totalKeys   uint32
	activeKeys  uint32
	file        *os.File
	pager       *bfile.Pager
	hashmap     map[uint32]uint32
	hashmapLock sync.RWMutex
}

func openIndex(opt *indexOptions) (*index, error) {
	file, err := os.OpenFile(opt.filePath, fsMode, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrap(err, "open index file")
	}
	pager := bfile.NewPager(file)
	idx := &index{
		file:    file,
		pager:   pager,
		hashmap: make(map[uint32]uint32),
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat index file")
	}
	if stat.Size() == 0 {
		return idx, nil
	}
	idx.totalKeys = uint32(stat.Size()) / idxRecordSize
	err = idx.readRecords()
	if err != nil {
		return nil, errors.Wrap(err, "read index records")
	}
	return idx, nil
}

func (idx *index) readRecords() error {
	var b [idxRecordSize]byte
	for i := 0; i < int(idx.totalKeys); i++ {
		n, err := idx.file.ReadAt(b[:], int64(i*idxRecordSize))
		if err != nil {
			return err
		}
		if n != idxRecordSize {
			return errors.New("index record size error")
		}
		keyHash := intconv.Uint32(b[0:4])
		// dataOffset := intconv.Uint64(b[4:12])
		dataSize := intconv.Uint32(b[12:16])
		dataCRC32 := intconv.Uint32(b[16:20])
		if dataSize != 0 && dataCRC32 != 0 {
			idx.activeKeys++
		}
		idx.hashmapSet(keyHash, uint32(i))
	}

	return nil
}

func (idx *index) PutRecord(ir *indexRecord) error {
	var offset int64
	recordID, ok := idx.hashmapGet(ir.keyHash)
	if ok {
		offset = int64(recordID * idxRecordSize)
	} else {
		if ir.dataSize != 0 && ir.dataCRC32 != 0 {
			idx.activeKeys++
		}
		offset = int64(idx.totalKeys * idxRecordSize)
		idx.totalKeys++
	}
	n, err := idx.file.WriteAt(ir.Bytes(), offset)
	if err != nil {
		return err
	}
	if n != idxRecordSize {
		return errors.New("write record size error != 20")
	}
	return nil
}

func (idx *index) hashmapSet(k, v uint32) {
	idx.hashmapLock.Lock()
	idx.hashmap[k] = v
	idx.hashmapLock.Unlock()
}

func (idx *index) hashmapGet(k uint32) (uint32, bool) {
	idx.hashmapLock.RLock()
	v, found := idx.hashmap[k]
	idx.hashmapLock.RUnlock()
	return v, found
}

func (idx *index) GetRecord(recordID uint32) (*indexRecord, error) {
	if recordID > idx.totalKeys {
		return nil, errors.New("error recordID")
	}
	buf := acquireByte20()
	defer releaseByte20(buf)
	offset := recordID * idxRecordSize
	_, err := idx.pager.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	return parseIndexRecord(buf)
}

func (idx *index) Close() error {
	return idx.file.Close()
}
