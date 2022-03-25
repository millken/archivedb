package archivedb

import (
	"math"
	"sync"

	"github.com/pkg/errors"
)

const (
	MaxKeySize   = math.MaxUint16
	MaxValueSize = math.MaxUint32
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrKeyDeleted     = errors.New("key deleted")
	ErrEmptyKey       = errors.New("empty key")
	ErrKeyTooLarge    = errors.New("key size is too large")
	ErrKeyExpired     = errors.New("key expired")
	ErrKeyMismatch    = errors.New("key mismatch")
	ErrChecksumFailed = errors.New("checksum failed")
	ErrValueTooLarge  = errors.New("value size is too large")
	ErrLengthMismatch = errors.New("length mismatch")
)

type DB struct {
	opts    *option
	index   *index
	storage *storage
	mu      sync.RWMutex
}

func Open(filePath string, options ...Option) (*DB, error) {
	opts := &option{
		fsync:    false,
		hashFunc: DefaultHashFunc,
	}
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return nil, errors.Wrap(err, "Invalid option")
		}
	}
	storage, err := openStorage(filePath)
	if err != nil {
		return nil, err
	}

	index, err := openIndex(filePath + ".idx")
	if err != nil {
		return nil, errors.Wrap(err, "opening index")
	}
	db := &DB{
		opts:    opts,
		index:   index,
		storage: storage,
	}
	return db, nil
}

//Put put the value of the key to the db
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := validateKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	entry := acquireEntry()
	defer releaseEntry(entry)
	entry.set(key, value)
	hashKey := db.opts.hashFunc(key)
	off := db.storage.getEndOffset()
	if err := db.storage.writeEntry(entry); err != nil {
		return err
	}
	if db.opts.fsync {
		if err := db.storage.Sync(); err != nil {
			return err
		}
	}
	if err := db.index.Set(hashKey, off); err != nil {
		return err
	}
	if db.opts.fsync {
		if err := db.index.Sync(); err != nil {
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
	entry, err := db.storage.readEntry(uint64(item.Offset()))
	if err != nil {
		return nil, err
	}
	if err := entry.verify(key); err != nil {
		return nil, err
	}
	if entry.IsDeleted() {
		return nil, ErrKeyDeleted
	}
	return entry.Value, nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := validateKey(key); err != nil {
		return err
	}
	hashKey := db.opts.hashFunc(key)
	item, ok := db.index.Get(hashKey)
	if !ok {
		return nil
	}
	entry, err := db.storage.readEntry(uint64(item.Offset()))
	if err != nil {
		return err
	}
	if err := entry.verify(key); err != nil {
		return err
	}
	entry.addMeta(bitDelete)
	if err := db.storage.updateEntryHeader(item.Offset(), entry.Header); err != nil {
		return err
	}
	if db.opts.fsync {
		if err := db.storage.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the DB
func (db *DB) Close() error {
	if err := db.storage.Close(); err != nil {
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
