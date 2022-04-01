package archivedb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strconv"

	"github.com/millken/archivedb/internal/mmap"
	"github.com/pkg/errors"
)

const (
	SegmentVersion        = 1
	SegmentMagic          = "ArSeG"
	SegmentSize    uint32 = 1 << 30 // 1GB

	SegmentHeaderSize = 6 // magic + version
)

var (
	ErrInvalidSegment        = errors.New("invalid segment")
	ErrInvalidSegmentVersion = errors.New("invalid segment version")
	ErrSegmentNotWritable    = errors.New("segment not writable")
)

type segmentHeader struct {
	Version uint8
}

func newSegmentHeader() segmentHeader {
	return segmentHeader{Version: SegmentVersion}
}

// WriteTo writes the header to w.
func (hdr *segmentHeader) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	buf.WriteString(SegmentMagic)
	binary.Write(&buf, binary.BigEndian, hdr.Version)
	return buf.WriteTo(w)
}

func decodeSegmentHeader(b []byte) (hdr segmentHeader, err error) {
	if len(b) < SegmentHeaderSize {
		return hdr, errors.Wrap(ErrInvalidSegment, "invalid segment header")
	}
	magic := b[0:len(SegmentMagic)]
	if !bytes.Equal(magic, []byte(SegmentMagic)) {
		return hdr, errors.Wrap(ErrInvalidSegment, "invalid magic")
	}
	hdr.Version = b[len(SegmentMagic)]
	return hdr, nil
}

type segment struct {
	mmap *mmap.MMap
	path string
	size uint32
	id   uint16
}

// newSegment returns a new instance of segment.
func newSegment(id uint16, path string) *segment {
	return &segment{
		id:   id,
		path: path,
	}
}

// createSegment generates an empty segment at path.
func createSegment(id uint16, path string) (*segment, error) {
	// Generate segment in temp location.
	f, err := os.Create(path + ".initializing")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Write header to file and close.
	hdr := newSegmentHeader()
	if _, err := hdr.WriteTo(f); err != nil {
		return nil, err
	} else if err := f.Sync(); err != nil {
		return nil, err
	} else if err := f.Close(); err != nil {
		return nil, err
	}

	// Swap with target path.
	if err := os.Rename(f.Name(), path); err != nil {
		return nil, err
	}

	// Open segment at new location.
	segment := newSegment(id, path)
	if err := segment.Open(); err != nil {
		return nil, err
	}
	return segment, nil
}

// ID returns the id the segment was initialized with.
func (s *segment) ID() uint16 { return s.id }

// Size returns the size of the data in the segment.
// This is only populated once InitForWrite() is called.
func (s *segment) Size() uint32 { return s.size }

func (s *segment) Open() error {
	// if err := func() (err error) {
	// 	if s.mapFile, err = mmap.OpenWithBufferSize(s.path, 40960); err != nil {
	// 		return err
	// 	}

	// 	// Read header.
	// 	buf, err := s.mapFile.ReadOffset(0, SegmentHeaderSize)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	hdr, err := decodeSegmentHeader(buf)
	// 	if err != nil {
	// 		return err
	// 	} else if hdr.Version != SegmentVersion {
	// 		return ErrInvalidSegmentVersion
	// 	}
	// 	for s.size = uint32(SegmentHeaderSize); s.size < uint32(s.mapFile.Size()); {
	// 		buf, err := s.mapFile.ReadOffset(int64(s.size), EntryHeaderSize)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		hdr, err := readEntryHeader(buf)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		if !isValidEntryFlag(hdr.Flag) {
	// 			break
	// 		}
	// 		s.size += hdr.EntrySize()
	// 	}
	// 	return nil
	// }(); err != nil {
	// 	s.Close()
	// 	return err
	// }

	return nil
}

func (s *segment) WriteEntry(e entry) error {
	if !s.CanWrite(e) {
		return ErrSegmentNotWritable
	}

	// // Write entry header.
	// n, err := s.mapFile.Write(e.hdr.Encode())
	// if err != nil {
	// 	return err
	// } else if n != EntryHeaderSize {
	// 	return errors.Wrapf(ErrInvalidEntryHeader, "write entry header length %d", n)
	// }
	// s.size += uint32(n)

	// n, err = s.mapFile.Write(e.key)
	// if err != nil {
	// 	return err
	// } else if n != int(e.hdr.KeySize) {
	// 	return errors.Wrapf(ErrInvalidEntryHeader, "write key length %d", n)
	// }
	// s.size += uint32(n)

	// n, err = s.mapFile.Write(e.value)
	// if err != nil {
	// 	return err
	// } else if n != int(e.hdr.ValueSize) {
	// 	return errors.Wrapf(ErrInvalidEntryHeader, "write value length %d", n)
	// }
	// s.size += uint32(n)
	return nil
}

func (s *segment) ReadEntry(off uint32) (e entry, err error) {
	if off >= s.size {
		return e, errors.Wrap(ErrInvalidOffset, "request offset exceeds segment size")
	}
	// buf := make([]byte, EntryHeaderSize)
	// if n, err := s.mapFile.ReadAt(buf, int64(off)); err != nil {
	// 	return e, err
	// } else if n != int(EntryHeaderSize) {
	// 	return e, errors.Wrapf(ErrInvalidEntryHeader, "read entry header length %d", n)
	// }
	// buf, err := s.mapFile.ReadOffset(int64(off), EntryHeaderSize)
	// if err != nil {
	// 	return e, err
	// }
	// e.hdr, err = readEntryHeader(buf)
	// if err != nil {
	// 	return e, err
	// }
	// if !isValidEntryFlag(e.hdr.Flag) {
	// 	return e, errors.Wrap(ErrInvalidOffset, "invalid entry flag")
	// }
	// start := off + EntryHeaderSize
	// // key := make([]byte, e.hdr.KeySize)
	// // if n, err := s.mapFile.ReadAt(key, int64(start)); err != nil {
	// // 	return e, err
	// // } else if n != int(e.hdr.KeySize) {
	// // 	return e, errors.Wrapf(ErrInvalidEntryHeader, "read key length %d", n)
	// // }
	// e.key, err = s.mapFile.ReadOffset(int64(start), int64(e.hdr.KeySize))
	// if err != nil {
	// 	return e, err
	// }
	// start += uint32(e.hdr.KeySize)
	// // value := make([]byte, e.hdr.ValueSize)
	// // if n, err := s.mapFile.ReadAt(value, int64(start)); err != nil {
	// // 	return e, err
	// // } else if n != int(e.hdr.ValueSize) {
	// // 	return e, errors.Wrapf(ErrInvalidEntryHeader, "read value length %d", n)
	// // }
	// e.value, err = s.mapFile.ReadOffset(int64(start), int64(e.hdr.ValueSize))
	// if err != nil {
	// 	return e, err
	// }
	return
}

func (s *segment) ForEachEntry(fn func(e entry) error) error {
	// hbuf := make([]byte, EntryHeaderSize)
	// for i := uint32(SegmentHeaderSize); i < s.size; {
	// 	if n, err := s.mapFile.ReadAt(hbuf, int64(i)); err != nil {
	// 		return err
	// 	} else if n != int(EntryHeaderSize) {
	// 		return errors.Wrapf(ErrInvalidEntryHeader, "read entry header length %d", n)
	// 	}
	// 	hdr, err := readEntryHeader(hbuf)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if !isValidEntryFlag(hdr.Flag) {
	// 		break
	// 	}
	// 	start := i + EntryHeaderSize
	// 	key := make([]byte, hdr.KeySize)
	// 	if n, err := s.mapFile.ReadAt(key, int64(start)); err != nil {
	// 		return err
	// 	} else if n != int(hdr.KeySize) {
	// 		return errors.Wrapf(ErrInvalidEntryHeader, "read key length %d", n)
	// 	}
	// 	start += uint32(hdr.KeySize)
	// 	value := make([]byte, hdr.ValueSize)
	// 	if n, err := s.mapFile.ReadAt(value, int64(start)); err != nil {
	// 		return err
	// 	} else if n != int(hdr.ValueSize) {
	// 		return errors.Wrapf(ErrInvalidEntryHeader, "read value length %d", n)
	// 	}
	// 	e := createEntry(hdr.Flag, key, value)
	// 	if err := fn(e); err != nil {
	// 		return err
	// 	}
	// 	i += hdr.EntrySize()
	// }
	return nil
}

// Close unmaps the segment.
func (s *segment) Close() (err error) {

	return s.mmap.Close()
}

// CanWrite returns true if segment has space to write entry data.
func (s *segment) CanWrite(e entry) bool {
	return s.size+e.Size() <= SegmentSize
}

// Flush flushes the buffer to disk.
func (s *segment) Flush() error {
	return nil
}

// parseSegmentFilename returns the id represented by the hexadecimal filename.
func parseSegmentFilename(filename string) (uint16, error) {
	i, err := strconv.ParseUint(filename, 16, 32)
	return uint16(i), err
}
