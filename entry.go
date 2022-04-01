package archivedb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/pkg/errors"
)

const (
	EntryMaxVersion       = math.MaxUint8
	EntryHeaderSize       = 16
	EntryFlagSize         = 1
	EntryInsertFlag uint8 = 1
	EntryDeleteFlag uint8 = 2
)

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

/*
*
+----------+---------------+---------------+---------------+
| Flag(1B) |  keySize (1B) | ValueSize (4B)|  Checksum (4B)|
+----------+---------------+---------------+---------------+
*
*/
type EntryHeader struct {
	ValueSize uint32
	Checksum  uint32
	KeySize   uint8
	Flag      uint8
	_         [6]byte // padding
}

type entry struct {
	key   []byte
	value []byte
	hdr   EntryHeader
}

func (hdr EntryHeader) EntrySize() uint32 {
	return EntryHeaderSize + uint32(hdr.KeySize) + hdr.ValueSize
}
func (e *EntryHeader) Encode() []byte {
	var b [EntryHeaderSize]byte
	intconv.PutUint32(b[0:4], e.ValueSize)
	intconv.PutUint32(b[4:8], e.Checksum)
	b[9] = byte(e.KeySize)
	b[10] = byte(e.Flag)

	return b[:]
}

func (e *EntryHeader) String() string {
	return fmt.Sprintf("Flag: %d, KeySize: %d, ValueSize: %d, Checksum: %d",
		e.Flag, e.KeySize, e.ValueSize, e.Checksum)
}

func (e *entry) Size() uint32 {
	return EntryHeaderSize + uint32(e.hdr.KeySize) + e.hdr.ValueSize
}

func readEntryHeader(b []byte) (EntryHeader, error) {
	if len(b) < EntryHeaderSize {
		return EntryHeader{}, errors.Wrapf(ErrInvalidEntryHeader, "read entry header length %d", len(b))
	}
	fmt.Printf("%d %v", len(b), b)
	return EntryHeader{
		ValueSize: intconv.Uint32(b[0:4]),
		Checksum:  intconv.Uint32(b[4:8]),
		KeySize:   uint8(b[9]),
		Flag:      uint8(b[10]),
	}, nil
}

func createEntry(flag uint8, key, value []byte) entry {
	return entry{
		key:   key,
		value: value,
		hdr: EntryHeader{
			ValueSize: uint32(len(value)),
			Checksum:  crc32.Checksum(value, CastagnoliCrcTable),
			KeySize:   uint8(len(key)),
			Flag:      flag,
		},
	}
}

func (e *entry) verify(key []byte) error {
	if e.hdr.KeySize != uint8(len(e.key)) || e.hdr.ValueSize != uint32(len(e.value)) {
		return ErrLengthMismatch
	}
	if !bytes.Equal(e.key, key) {
		return errors.Wrap(ErrKeyMismatch, "verify entry key")
	}
	if e.hdr.Checksum != crc32.Checksum(e.value, CastagnoliCrcTable) {
		return ErrChecksumFailed
	}
	return nil
}

func (e *entry) String() string {
	return fmt.Sprintf("Key: %s, Value: %s, Header: %s",
		string(e.key), string(e.value), e.hdr.String())
}

// isValidEntryFlag returns true if flag is valid.
func isValidEntryFlag(flag uint8) bool {
	switch flag {
	case EntryInsertFlag, EntryDeleteFlag:
		return true
	default:
		return false
	}
}
