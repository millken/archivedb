package archivedb

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

const (
	MaxKeySize          = math.MaxUint16
	MaxValueSize uint32 = SegmentSize - SegmentHeaderSize
)

var (
	ErrSegmentNotFound    = errors.New("segment not found")
	ErrKeyNotFound        = errors.New("key not found")
	ErrKeyDeleted         = errors.New("key deleted")
	ErrEmptyKey           = errors.New("empty key")
	ErrKeyTooLarge        = errors.New("key size is too large")
	ErrKeyExpired         = errors.New("key expired")
	ErrKeyMismatch        = errors.New("key mismatch")
	ErrChecksumFailed     = errors.New("checksum failed")
	ErrValueTooLarge      = errors.New("value size is too large")
	ErrLengthMismatch     = errors.New("length mismatch")
	ErrInvalidEntryHeader = errors.New("invalid entry header")
	ErrInvalidOffset      = errors.New("invalid offset")
)

type DB struct {
	path     string
	opts     *option
	index    *index
	segments []*segment
	mu       sync.RWMutex
}

func Open(path string, options ...Option) (*DB, error) {
	opts := &option{
		fsync:    false,
		hashFunc: DefaultHashFunc,
	}
	// Create path if it doesn't exist.
	if err := os.MkdirAll(filepath.Join(path), 0777); err != nil {
		return nil, err
	}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return nil, errors.Wrap(err, "Invalid option")
		}
	}

	db := &DB{
		path: path,
		opts: opts,
	}
	// Open components.
	if err := func() (err error) {
		if err := db.openSegments(); err != nil {
			return err
		}

		// Init last segment for writes.
		if err := db.activeSegment().InitForWrite(); err != nil {
			return err
		}

		db.index, err = openIndex(db.IndexPath())
		if err != nil {
			return errors.Wrap(err, "opening index")
		}
		//  else if err := db.index.Recover(db.segments); err != nil {
		// 	return err
		// }

		return nil
	}(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) openSegments() error {
	var err error
	fis, err := ioutil.ReadDir(db.path)
	if err != nil {
		return err
	}
	for _, fi := range fis {
		segmentID, err := parseSegmentFilename(fi.Name())
		if err != nil {
			continue
		}

		segment := newSegment(segmentID, filepath.Join(db.path, fi.Name()))
		if err := segment.Open(); err != nil {
			return err
		}
		db.segments = append(db.segments, segment)
	}
	// Create initial segment if none exist.
	if len(db.segments) == 0 {
		segment, err := createSegment(0, filepath.Join(db.path, "0000"))
		if err != nil {
			return err
		}
		db.segments = append(db.segments, segment)
	}
	return nil
}

// activeSegment returns the last segment.
func (db *DB) activeSegment() *segment {
	if len(db.segments) == 0 {
		return nil
	}
	return db.segments[len(db.segments)-1]
}

func (db *DB) createSegment() (*segment, error) {
	// Close writer for active segment, if one exists.
	if segment := db.activeSegment(); segment != nil {
		if err := segment.CloseForWrite(); err != nil {
			return nil, err
		}
	}

	// Generate a new sequential segment identifier.
	var id uint16
	if len(db.segments) > 0 {
		id = db.segments[len(db.segments)-1].ID() + 1
	}
	filename := fmt.Sprintf("%04x", id)

	// Generate new empty segment.
	segment, err := createSegment(id, filepath.Join(db.path, filename))
	if err != nil {
		return nil, err
	}
	db.segments = append(db.segments, segment)

	// Allow segment to write.
	if err := segment.InitForWrite(); err != nil {
		return nil, err
	}

	return segment, nil
}

// IndexPath returns the path to the series index.
func (db *DB) IndexPath() string { return filepath.Join(db.path, "index") }

//Put put the value of the key to the db
func (db *DB) Put(key, value []byte) error {
	var err error
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := validateKey(key); err != nil {
		return err
	}
	if len(value) > int(MaxValueSize) {
		return ErrValueTooLarge
	}
	entry := createEntry(EntryInsertFlag, key, value)
	segment := db.activeSegment()
	if segment == nil || !segment.CanWrite(entry) {
		if segment, err = db.createSegment(); err != nil {
			return err
		}
	}
	if err = segment.WriteEntry(entry); err != nil {
		return err
	}
	hashKey := db.opts.hashFunc(key)
	offset := segment.Size() - entry.Size()
	if err = db.index.Insert(hashKey, segment.ID(), offset); err != nil {
		return err
	}
	if db.opts.fsync {
		if err := segment.Flush(); err != nil {
			return err
		} else if err := db.index.Flush(); err != nil {
			return err
		}
	}
	return nil
}

//Get gets the value of the key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if err := validateKey(key); err != nil {
		return nil, err
	}
	hashKey := db.opts.hashFunc(key)
	item, ok := db.index.Get(hashKey)
	if !ok {
		return nil, ErrKeyNotFound
	}
	segment := db.segments[item.ID()]
	if segment == nil {
		return nil, ErrSegmentNotFound
	}
	entry, err := segment.ReadEntry(item.Offset())
	if err != nil {
		return nil, err
	}

	if err := entry.verify(key); err != nil {
		return nil, err
	}

	return entry.value, nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := validateKey(key); err != nil {
		return err
	}

	return nil
}

// Close closes the DB
func (db *DB) Close() error {
	if err := db.activeSegment().Close(); err != nil {
		return errors.Wrap(err, "closing storage")
	}
	if err := db.index.Close(); err != nil {
		return errors.Wrap(err, "closing index")
	}
	return nil
}

func validateKey(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}
