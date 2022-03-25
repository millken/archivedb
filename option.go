package archivedb

import "github.com/cespare/xxhash/v2"

// Option sets parameters for archiveDB construction parameter
type Option func(*option) error

// HashFunc defines a function to generate the hash which will be used as key in db
type HashFunc func([]byte) uint64

// DefaultHashFunc implements a default hash function
func DefaultHashFunc(b []byte) uint64 {
	return xxhash.Sum64(b)
}

type option struct {
	// hashFunc is used to generate the hash which will be used as key in db
	hashFunc HashFunc
	// fsync is used to sync the data to disk
	fsync bool

	// writeVersion is used to write the version to the entry
	writeVersion bool
}

// HashFuncOption sets the hash func for the database
func HashFuncOption(h HashFunc) Option {
	return func(db *option) error {
		db.hashFunc = h
		return nil
	}
}

func FsyncOption(fsync bool) Option {
	return func(db *option) error {
		db.fsync = fsync
		return nil
	}
}
func WriteVersionOption(writeVersion bool) Option {
	return func(db *option) error {
		db.writeVersion = writeVersion
		return nil
	}
}
