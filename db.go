package archivedb

import (
	"math"

	"github.com/pkg/errors"
)

const (
	MaxKeySize = math.MaxUint16
)

var (
	ErrNotFound   = errors.New("Not Found")
	ErrInvalidKey = errors.New("Invalid Key")
	ErrKeySize    = errors.New("Key size is too large")
)

type DB struct {
	opts    *option
	index   *index
	storage *storage
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

//Set sets the value of the key
func (db *DB) Set(key, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	entry := acquireEntry()
	defer releaseEntry(entry)
	entry.set(key, value)
	hashKey := db.opts.hashFunc(key)
	off := db.storage.getEndOffset()
	if err := db.storage.writeEntry(entry); err != nil {
		return err
	}
	if err := db.index.Set(hashKey, off); err != nil {
		return err
	}
	return nil
}

//Get gets the value of the key
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	hashKey := db.opts.hashFunc(key)
	item, ok := db.index.Get(hashKey)
	if !ok {
		return nil, ErrNotFound
	}
	entry, err := db.storage.readEntry(uint64(item.Offset()))
	if err != nil {
		return nil, err
	}
	if !entry.verify(key) {
		return nil, ErrInvalidEntry
	}
	return entry.Value, nil
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
		return ErrInvalidKey
	}
	if len(key) > MaxKeySize {
		return ErrKeySize
	}
	return nil
}
