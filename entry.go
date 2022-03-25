package archivedb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var (
	currentTime        = time.Now
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

	ErrInvalidEntryHeader  = errors.New("invalid entry header")
	ErrInvalidEntryVersion = errors.New("invalid entry version")
)

type entryMeta byte

const (
	entryMaxVersion           = math.MaxUint8
	entryHeaderSize           = 20
	bitDelete       entryMeta = 1 << 0 // Set if the key has been deleted.
)

var entryPool = &sync.Pool{
	New: func() interface{} {
		return NewEntry(nil, nil)
	},
}

func acquireEntry() *Entry {
	v := entryPool.Get().(*Entry)
	return v
}

func releaseEntry(v *Entry) {
	entryPool.Put(v)
}

type entryHeader struct {
	ExpiresAt uint64
	ValueLen  uint32
	ValueCRC  uint32
	KeyLen    uint16
	meta      uint8
	ver       uint8
}
type Entry struct {
	Key    []byte
	Value  []byte
	Header *entryHeader
}

func (e *entryHeader) Encode() []byte {
	var b [entryHeaderSize]byte
	intconv.PutUint16(b[0:2], e.KeyLen)
	intconv.PutUint32(b[2:6], e.ValueLen)
	intconv.PutUint32(b[6:10], e.ValueCRC)
	intconv.PutUint64(b[10:18], e.ExpiresAt)
	b[18] = byte(e.meta)
	b[19] = byte(e.ver)
	return b[:]
}

func (e *entryHeader) Decode(b []byte) error {
	if len(b) != entryHeaderSize {
		return errors.Wrapf(ErrInvalidEntryHeader, "decode entry header length %d", len(b))
	}
	e.KeyLen = intconv.Uint16(b[0:2])
	e.ValueLen = intconv.Uint32(b[2:6])
	e.ValueCRC = intconv.Uint32(b[6:10])
	e.ExpiresAt = intconv.Uint64(b[10:18])
	e.meta = b[18]
	e.ver = b[19]
	return nil
}

func (e entryHeader) String() string {
	return fmt.Sprintf("KeyLen: %d ValueLen: %d ValueCRC: %x ExpiresAt: %d meta: %d",
		e.KeyLen, e.ValueLen, e.ValueCRC, e.ExpiresAt, e.meta)
}

/*
the entry  format:
+---------------+---------------+---------------+---------------+----------+----------+-----+-----+
|  keyLen (2B)  | DataLen (4B)  |  CRC32 (4B)   | Timestamp(4B) | Meta(2B) | Ver(1B)  | Key |Data |
+---------------+---------------+---------------+---------------+----------+----------+-----+-----+
*/
func NewEntry(key, value []byte) *Entry {
	e := &Entry{
		Key:   key,
		Value: value,
		Header: &entryHeader{
			KeyLen:    uint16(len(key)),
			ValueLen:  uint32(len(value)),
			ValueCRC:  crc32.Checksum(value, CastagnoliCrcTable),
			ExpiresAt: 0,
			meta:      0,
			ver:       0,
		},
	}
	return e
}

// Size returns the size of the entry.
func (e *Entry) Size() uint64 {
	return uint64(e.Header.KeyLen) + uint64(e.Header.ValueLen) + entryHeaderSize
}

//addMeta adds meta to Entry e.
func (e *Entry) set(key, value []byte) *Entry {
	e.Key = key
	e.Value = value
	e.Header.KeyLen = uint16(len(key))
	e.Header.ValueLen = uint32(len(value))
	e.Header.ValueCRC = crc32.Checksum(value, CastagnoliCrcTable)
	return e
}

// WithTTL adds time to live duration to Entry e. Entry stored with a TTL would automatically expire
// after the time has elapsed, and will be eligible for garbage collection.
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.Header.ExpiresAt = uint64(currentTime().Add(dur).Unix())
	return e
}

//addMeta adds meta to Entry e.
func (e *Entry) addMeta(meta entryMeta) *Entry {
	e.Header.meta |= uint8(meta)
	return e
}

//deleteMeta deletes meta from Entry e.
func (e *Entry) deleteMeta(meta entryMeta) *Entry {
	e.Header.meta &= ^uint8(meta)
	return e
}

func (e *Entry) addVersion() (*Entry, error) {
	v := e.Header.ver + 1
	if v > entryMaxVersion {
		return nil, ErrInvalidEntryVersion
	}
	return e, nil
}

//IsDeleted returns true if Entry e has been deleted.
func (e *Entry) IsDeleted() bool {
	return e.Header.meta&uint8(bitDelete) != 0
}

func (e *Entry) HasExpired() bool {
	return e.Header.ExpiresAt > 0 && e.Header.ExpiresAt < uint64(currentTime().Unix())
}

func (e *Entry) verify(key []byte) error {
	if e.Header.KeyLen != uint16(len(e.Key)) || e.Header.ValueLen != uint32(len(e.Value)) {
		return ErrLengthMismatch
	}
	if !bytes.Equal(e.Key, key) {
		return ErrKeyMismatch
	}
	if e.Header.ValueCRC != crc32.Checksum(e.Value, CastagnoliCrcTable) {
		return ErrChecksumFailed
	}
	return nil
}

func (e *Entry) String() string {
	return fmt.Sprintf("Key: %s, Value: %s, Header: %s",
		string(e.Key), string(e.Value), e.Header.String())
}
