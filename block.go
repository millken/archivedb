package archivedb

import (
	"sync"

	"github.com/pkg/errors"
)

const (
	blockSize      = 4 << 20 //4MB
	blockNum       = 1024
	blockMetaSize  = 20
	blockEntrySize = 24
)

/*
Block 4M
+--------------+---+--------------+
|Entry 1 (24B) |...|Entry N (24B) |
+--------------+---+--------------+

blockEntry
+---------------+---------------+----------------+---------------+----------+
|Hash key (4B)  |Hash index (4B)|Data seek (8B)  |Data len (4B)  |CRC32 (4B)|
+---------------+---------------+----------------+---------------+----------+
*/

type entryKey uint32

type block struct {
	lock    sync.RWMutex
	hashmap map[entryKey]uint32
}
type blockMeta struct {
	blockID uint16
	keys    uint32
	t1      uint64
	t2      uint32
	t3      uint32
}

func getBlockID(key uint32) uint16 {
	return uint16(key%uint32(blockNum) - 1)
}

func (r *blockMeta) Bytes() []byte {
	var b [blockMetaSize]byte
	intconv.PutUint32(b[0:4], r.keys)
	intconv.PutUint64(b[4:12], 1)
	intconv.PutUint32(b[12:16], 2)
	intconv.PutUint32(b[16:20], 3)
	return b[:]
}

func (r *blockMeta) Parse(b []byte) error {
	if cap(b) != blockMetaSize {
		return errors.New("parse block meta length error")
	}
	r.keys = intconv.Uint32(b[0:4])
	r.t1 = intconv.Uint64(b[4:12])
	r.t2 = intconv.Uint32(b[12:16])
	r.t3 = intconv.Uint32(b[16:20])
	return nil
}

type blockEntry struct {
	keyHash    uint32
	keyIndex   uint32
	dataOffset uint64
	dataLen    uint32
	dataCRC32  uint32
}

func (r *blockEntry) Bytes() []byte {
	var b [blockEntrySize]byte
	intconv.PutUint32(b[0:4], r.keyHash)
	intconv.PutUint32(b[4:8], r.keyIndex)
	intconv.PutUint64(b[8:16], r.dataOffset)
	intconv.PutUint32(b[16:20], r.dataLen)
	intconv.PutUint32(b[20:24], r.dataCRC32)
	return b[:]
}

func (r *blockEntry) Parse(b []byte) error {
	if cap(b) != blockEntrySize {
		return errors.New("block entry length error")
	}
	r.keyHash = intconv.Uint32(b[0:4])
	r.keyIndex = intconv.Uint32(b[4:8])
	r.dataOffset = intconv.Uint64(b[8:16])
	r.dataLen = intconv.Uint32(b[16:20])
	r.dataCRC32 = intconv.Uint32(b[20:24])
	return nil
}
