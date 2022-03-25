package archivedb

import (
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/tidwall/bfile"
)

const (
	storageMetaSize = 16
)

type storage struct {
	file      *os.File
	pager     *bfile.Pager
	endOffset int64
}

func openStorage(filePath string) (*storage, error) {
	file, err := os.OpenFile(filePath, fsMode, os.FileMode(0644))
	if err != nil {
		return nil, errors.Wrap(err, "opening storage")
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat storage file")
	}
	s := &storage{
		file:  file,
		pager: bfile.NewPager(file),
	}

	if stat.Size() == 0 {
		atomic.StoreInt64(&s.endOffset, storageMetaSize)
		if err := s.writeMeta(); err != nil {
			return nil, errors.Wrap(err, "write storage file")
		}
	}
	if err := s.readMeta(); err != nil {
		return nil, errors.Wrap(err, "read storage file")
	}

	return s, nil
}

func (s *storage) readMeta() error {
	var b [storageMetaSize]byte
	n, err := s.pager.ReadAt(b[:], 0)
	if err != nil {
		return err
	}
	if n != storageMetaSize {
		return errors.New("read storage meta error")
	}
	atomic.StoreInt64(&s.endOffset, int64(intconv.Uint64(b[0:8])))
	return nil
}

func (s *storage) writeMeta() error {
	var b [storageMetaSize]byte
	intconv.PutUint64(b[0:8], uint64(s.getEndOffset()))
	//intconv.PutUint64(b[8:16], 0)
	n, err := s.pager.WriteAt(b[:], 0)
	if err != nil {
		return err
	}
	if n != storageMetaSize {
		return errors.New("write storage meta error")
	}
	return nil
}

func (s *storage) getEndOffset() int64 {
	return atomic.LoadInt64(&s.endOffset)
}

func (s *storage) writeEntry(entry *Entry) error {
	ehBuf := entry.Header.Encode()
	stream := s.pager.Stream(s.getEndOffset())
	n, err := stream.Write(ehBuf)
	if err != nil {
		return err
	}
	if n != entryHeaderSize {
		return ErrInvalidEntryHeader
	}
	n, err = stream.Write(entry.Key)
	if err != nil {
		return err
	}
	if n != int(entry.Header.KeyLen) {
		return ErrInvalidEntryHeader
	}
	n, err = stream.Write(entry.Value)
	if err != nil {
		return err
	}
	if n != int(entry.Header.ValueLen) {
		return ErrInvalidEntryHeader
	}
	atomic.AddInt64(&s.endOffset, int64(entry.Size()))
	return s.writeMeta()
}

func (s *storage) readEntry(offset uint64) (*Entry, error) {
	ehBuf := make([]byte, entryHeaderSize)
	n, err := s.pager.ReadAt(ehBuf, int64(offset))
	if err != nil {
		return nil, err
	}
	if n != entryHeaderSize {
		return nil, ErrInvalidEntryHeader
	}
	eh := &entryHeader{}
	err = eh.Decode(ehBuf)
	if err != nil {
		return nil, err
	}

	keyBuf := make([]byte, eh.KeyLen)
	n, err = s.pager.ReadAt(keyBuf, int64(offset+entryHeaderSize))
	if err != nil {
		return nil, err
	}
	if n != int(eh.KeyLen) {
		return nil, ErrInvalidEntryHeader
	}

	valueBuf := make([]byte, eh.ValueLen)
	n, err = s.pager.ReadAt(valueBuf, int64(offset+entryHeaderSize+uint64(eh.KeyLen)))
	if err != nil {
		return nil, err
	}
	if n != int(eh.ValueLen) {
		return nil, ErrInvalidEntryHeader
	}
	entry := &Entry{
		Key:    keyBuf,
		Value:  valueBuf,
		Header: eh,
	}
	return entry, nil
}

func (s *storage) Sync() error {
	return s.pager.Flush()
}

func (s *storage) Close() error {
	if err := s.pager.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}
