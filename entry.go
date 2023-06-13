package archivedb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/pkg/errors"
)

const (
	EntryMaxVersion = math.MaxUint8
)

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

/*
*
+----------+---------------+---------------+---------------+
| Flag(1B) |  keySize (1B) | ValueSize (4B)|  Checksum (4B)|
+----------+---------------+---------------+---------------+
*
*/

type entry struct {
	key   []byte
	value []byte
	hdr   *hdr
}

func createEntry(flag flag, key, value []byte) *entry {
	h := hdr{}
	return &entry{
		key:   key,
		value: value,
		hdr: h.setFlag(flag).
			setKeySize(uint8(len(key))).
			setValueSize(uint32(len(value))).
			setChecksum(crc32.Checksum(value, CastagnoliCrcTable)),
	}
}

func (e *entry) verify(key []byte) error {
	if e.hdr.getKeySize() != uint8(len(e.key)) || e.hdr.getValueSize() != uint32(len(e.value)) {
		return ErrLengthMismatch
	}
	if !bytes.Equal(e.key, key) {
		return errors.Wrap(ErrKeyMismatch, "verify entry key")
	}
	if e.hdr.getChecksum() != crc32.Checksum(e.value, CastagnoliCrcTable) {
		return ErrChecksumFailed
	}
	return nil
}

func (e *entry) Size() uint32 {
	return e.hdr.entrySize()
}

func (e *entry) String() string {
	return fmt.Sprintf("Key: %s, Value: %s, Header: %s",
		string(e.key), string(e.value), e.hdr.String())
}
