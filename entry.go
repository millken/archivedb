package archivedb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/pkg/errors"
)

type EntryFlag uint8

const (
	EntryMaxVersion           = math.MaxUint8
	EntryHeaderSize           = 16
	EntryFlagSize             = 1
	EntryInsertFlag EntryFlag = 1
	EntryDeleteFlag EntryFlag = 2
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
	Flag      EntryFlag
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
	b[0] = byte(e.Flag)
	b[1] = byte(e.KeySize)
	intconv.PutUint32(b[2:6], e.ValueSize)
	intconv.PutUint32(b[6:10], e.Checksum)

	return b[:]
}

func (e *EntryHeader) String() string {
	return fmt.Sprintf("Flag: %d, KeySize: %d, ValueSize: %d, Checksum: %d",
		e.Flag, e.KeySize, e.ValueSize, e.Checksum)
}

func (e *entry) Size() uint32 {
	return EntryHeaderSize + uint32(e.hdr.KeySize) + e.hdr.ValueSize
}

func readEntryHeader(b []byte) (hdr EntryHeader, err error) {
	if len(b) < EntryHeaderSize {
		return hdr, ErrInvalidEntryHeader
	}
	hdr.Flag = EntryFlag(b[0])
	hdr.KeySize = uint8(b[1])
	hdr.ValueSize = intconv.Uint32(b[2:6])
	hdr.Checksum = intconv.Uint32(b[6:10])
	return hdr, nil
}

func createEntry(flag EntryFlag, key, value []byte) entry {
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

func (e *entry) WriteTo(w io.Writer) error {
	n, err := w.Write(e.hdr.Encode())
	if err != nil {
		return errors.Wrap(err, "write entry header")
	}
	if n != EntryHeaderSize {
		return errors.Wrapf(ErrInvalidEntryHeader, "write entry header length %d", n)
	}

	n, err = w.Write(e.key)
	if err != nil {
		return err
	}
	if n != int(e.hdr.KeySize) {
		return errors.Wrapf(ErrInvalidEntryHeader, "write key length %d", n)
	}
	n, err = w.Write(e.value)
	if err != nil {
		return err
	}
	if n != int(e.hdr.ValueSize) {
		return errors.Wrapf(ErrInvalidEntryHeader, "write value length %d", n)
	}
	return nil
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
func isValidEntryFlag(flag EntryFlag) bool {
	switch flag {
	case EntryInsertFlag, EntryDeleteFlag:
		return true
	default:
		return false
	}
}
