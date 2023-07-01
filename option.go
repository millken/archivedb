package archivedb

// Option sets parameters for archiveDB construction parameter
type Option func(*option) error

type option struct {
	// fsync is used to sync the data to disk
	fsync bool
}

func FsyncOption(fsync bool) Option {
	return func(db *option) error {
		db.fsync = fsync
		return nil
	}
}
