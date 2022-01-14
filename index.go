package archivedb

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/pkg/errors"
)

/*
index
+----------+--------------+---+--------------+
|Meta (16B)|Record 1 (16B)|...|Record N (16B)|
+----------+--------------+---+--------------+
indexMeta
+---------------+----------------+
|Total keys (8B)|Active keys (8B)|
+---------------+----------------+
indexRecord
+---------------+----------------+
|Hash key (8B)  |Data seek (8B)  |
+---------------+----------------+
*/
const (
	idxMetaLength = 16
	fsMode        = os.O_RDWR | os.O_CREATE
)

var intconv = binary.BigEndian

type indexOptions struct {
	fileName string
}

type indexMeta struct {
	totalKeys  uint64
	activeKeys uint64
}

var indexMetaPool = &sync.Pool{
	New: func() interface{} {
		return new(indexMeta)
	},
}

func acquireIndexMeta() *indexMeta {
	v := indexMetaPool.Get().(*indexMeta)
	return v
}

func releaseIndexMeta(v *indexMeta) {
	indexMetaPool.Put(v)
}

// Parse is a helper function to parses given bytes into new IndexMeta struct
func (p *indexMeta) Parse(b []byte) error {
	if len(b) < idxMetaLength {
		return errors.New("index meta length error")
	}
	p.totalKeys = intconv.Uint64(b[0:8])
	p.activeKeys = intconv.Uint64(b[8:16])
	return nil
}

type indexRecord struct {
	hash uint64
	seek uint64
}
type index struct {
	meta *indexMeta
	file *os.File
}

func openIndex(opt *indexOptions) (*index, error) {
	file, err := os.OpenFile(opt.fileName, fsMode, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrap(err, "opening index")
	}
	idx := &index{
		meta: new(indexMeta),
		file: file,
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat index")
	}
	if stat.Size() == 0 {
		if err = idx.writeMeta(); err != nil {
			return nil, errors.Wrap(err, "write index meta")
		}
	}
	err = idx.readMeta()
	if err != nil {
		return nil, errors.Wrap(err, "read index meta")
	}
	return idx, nil
}

func (idx *index) readMeta() error {
	b := make([]byte, idxMetaLength)
	n, err := idx.file.ReadAt(b, 0)
	if err != nil {
		return err
	}
	if n != idxMetaLength {
		return errors.New("index meta length error")
	}
	idx.meta.totalKeys = intconv.Uint64(b[0:8])
	idx.meta.activeKeys = intconv.Uint64(b[8:16])
	return nil
}

func (idx *index) writeMeta() error {
	b := make([]byte, idxMetaLength)
	intconv.PutUint64(b, idx.meta.totalKeys)
	intconv.PutUint64(b[8:16], idx.meta.activeKeys)
	n, err := idx.file.WriteAt(b, 0)
	if err != nil {
		return err
	}
	if n != idxMetaLength {
		return errors.New("write length error")
	}
	return nil
}

func (idx *index) Write(hash, seek uint64) error {
}
