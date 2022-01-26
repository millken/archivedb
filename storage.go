package archivedb

import (
	"os"
	"sync"

	"github.com/pkg/errors"
)

/*
keyHash|keySeek|keyLen|CRC32
uint32|uint64|uint32|uint32
+---------------+----------------+---------------+----------+
|Hash key (4B)  |Data seek (8B)  |Data len (4B)  |CRC32 (4B)|
+---------------+----------------+---------------+----------+
*/

type storage struct {
	lock    sync.RWMutex
	file    *os.File
	hashMap sync.Map
}

func initializeStorage(filePath string) (*storage, error) {
	file, err := os.OpenFile(filePath, fsMode, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrap(err, "opening storage")
	}
	s := &storage{
		file: file,
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat storage")
	}
	if stat.Size() > 0 {
		if err = s.loadData(); err != nil {
			return nil, errors.Wrap(err, "load data")
		}
	}
	return s, nil
}

func (s *storage) loadData() error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for i := 0; i < blockNum; i++ {
		meta, err := s.readBlockMeta(uint16(i))
		if err != nil {
			return err
		}
		if meta.keys == 0 {
			continue
		}
		for j := 0; j < int(meta.keys); j++ {
			var b [16]byte
			offset := int64(meta.blockID*blockNum) + int64(blockMetaSize*blockNum) + int64(j*blockEntrySize)
			n, err := s.file.ReadAt(b[:], offset)
			if err != nil {
				return err
			}
			if n != 16 {
				return errors.New("read buffer length error !=16")
			}
		}
	}
	return nil
}

// func (s *storage) writeMeta(m *meta) error {
// 	offset := int64(m.id) * blockMetaSize
// 	// ret, err := s.file.Seek(offset, io.SeekStart)
// 	// if err != nil {
// 	// 	return err
// 	// }
// 	// fmt.Printf("%v", ret)
// 	n, err := s.file.WriteAt(m.Bytes(), offset)
// 	if err != nil {
// 		return err
// 	}
// 	if n != blockMetaSize {
// 		return errors.New("write meta error")
// 	}
// 	return nil
// }

func (s *storage) readBlockMeta(blockID uint16) (*blockMeta, error) {

	b := acquireByte20()
	defer releaseByte20(b)
	offset := blockID * blockMetaSize
	n, err := s.file.ReadAt(b, int64(offset))
	if err != nil {
		return nil, errors.Wrap(err, "read block meta")
	}
	if n != blockMetaSize {
		return nil, errors.New("read block meta size error")
	}
	m := &blockMeta{}
	if err = m.Parse(b); err != nil {
		return nil, errors.Wrap(err, "parse block meta")
	}
	m.blockID = blockID
	return m, nil
}

func (s *storage) writeBlocks() error {

	return nil
}

func (s *storage) insert(key, value string) error {
	// keyHash := fnv32a(key)
	// blockID := getBlockID(keyHash)

	return nil
}

func (s *storage) hashExist(hash uint32, blockID uint16) (bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	meta, err := s.readBlockMeta(blockID)
	if err != nil {
		return false, err
	}
	if meta.keys == 0 {
		return false, nil
	}
	return false, nil
}

func (s *storage) readBlock(meta *blockMeta) (*blockMeta, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return nil, nil
}
