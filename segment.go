package archivedb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"unsafe"

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

type segmentMeta struct {
	Version uint8
}

func newSegmentMeta() segmentMeta {
	return segmentMeta{Version: SegmentVersion}
}

// WriteTo writes the header to w.
func (hdr *segmentMeta) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	buf.WriteString(SegmentMagic)
	binary.Write(&buf, binary.BigEndian, hdr.Version)
	return buf.WriteTo(w)
}

func decodeSegmentMeta(b []byte) (meta segmentMeta, err error) {
	if len(b) < SegmentHeaderSize {
		return meta, errors.Wrap(ErrInvalidSegment, "invalid segment meta")
	}
	magic := b[0:len(SegmentMagic)]
	if !bytes.Equal(magic, []byte(SegmentMagic)) {
		return meta, errors.Wrap(ErrInvalidSegment, "invalid magic")
	}
	meta.Version = b[len(SegmentMagic)]
	return meta, nil
}

type segment struct {
	mmap *mmap.File
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
	hdr := newSegmentMeta()
	if _, err := hdr.WriteTo(f); err != nil {
		return nil, err
	} else if err := f.Sync(); err != nil {
		return nil, err
	} else if err := f.Truncate(int64(SegmentSize)); err != nil {
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
	if err := func() (err error) {
		if s.mmap, err = mmap.OpenFile(s.path, mmap.Read|mmap.Write); err != nil {
			return err
		}

		// Read header.
		buf, err := s.mmap.ReadOff(0, SegmentHeaderSize)
		if err != nil {
			return err
		}
		meta, err := decodeSegmentMeta(buf)
		if err != nil {
			return err
		} else if meta.Version != SegmentVersion {
			return ErrInvalidSegmentVersion
		}
		for s.size = uint32(SegmentHeaderSize); s.size < uint32(s.mmap.Len()); {
			buf, err := s.mmap.ReadOff(int(s.size), hdrSize)
			if err != nil {
				return err
			}
			h := hdr(buf)
			if !h.getFlag().isEntryValid() {
				break
			}
			s.size += h.entrySize()
		}
		if n, err := s.mmap.Seek(int64(s.size), io.SeekStart); err != nil {
			return err
		} else if n != int64(s.size) {
			return ErrInvalidSegment
		}
		return nil
	}(); err != nil {
		s.Close()
		return err
	}

	return nil
}

func (s *segment) WriteEntry(e *entry) error {
	if !s.CanWrite(e) {
		return ErrSegmentNotWritable
	}

	// Write entry header.
	n, err := s.mmap.Write(e.hdr[:])
	if err != nil {
		return err
	} else if n != hdrSize {
		return errors.Wrapf(ErrInvalidEntryHeader, "write entry header length %d", n)
	}
	s.size += uint32(n)

	n, err = s.mmap.Write(e.key)
	if err != nil {
		return err
	} else if n != int(e.hdr.getKeySize()) {
		return errors.Wrapf(ErrInvalidEntryHeader, "write key length %d", n)
	}
	s.size += uint32(n)

	n, err = s.mmap.Write(e.value)
	if err != nil {
		return err
	} else if n != int(e.hdr.getValueSize()) {
		return errors.Wrapf(ErrInvalidEntryHeader, "write value length %d", n)
	}
	s.size += uint32(n)
	return nil
}

func (s *segment) ReadEntry(off uint32) (*entry, error) {
	e := &entry{}
	if off >= s.size {
		return e, errors.Wrap(ErrInvalidOffset, "request offset exceeds segment size")
	}

	buf, err := s.mmap.ReadOff(int(off), hdrSize)
	if err != nil {
		return e, err
	}

	e.hdr = (*hdr)(unsafe.Pointer(&buf[0]))
	if !e.hdr.getFlag().isEntryValid() {
		return e, errors.Wrap(ErrInvalidOffset, "invalid entry flag")
	}
	start := off + hdrSize
	e.key, err = s.mmap.ReadOff(int(start), int(e.hdr.getKeySize()))
	if err != nil {
		return e, err
	}
	start += uint32(e.hdr.getKeySize())
	e.value, err = s.mmap.ReadOff(int(start), int(e.hdr.getValueSize()))
	if err != nil {
		return e, err
	}
	return e, nil
}

func (s *segment) ForEachEntry(fn func(e *entry) error) error {
	var h hdr
	for i := uint32(SegmentHeaderSize); i < s.size; {
		if n, err := s.mmap.ReadAt(h[:], int64(i)); err != nil {
			return err
		} else if n != int(hdrSize) {
			return errors.Wrapf(ErrInvalidEntryHeader, "read entry header length %d", n)
		}

		if !h.getFlag().isEntryValid() {
			break
		}
		start := i + hdrSize
		keySize := h.getKeySize()
		key := make([]byte, keySize)
		if n, err := s.mmap.ReadAt(key, int64(start)); err != nil {
			return err
		} else if n != int(keySize) {
			return errors.Wrapf(ErrInvalidEntryHeader, "read key length %d", n)
		}
		start += uint32(keySize)
		valueSize := h.getValueSize()
		value := make([]byte, valueSize)
		if n, err := s.mmap.ReadAt(value, int64(start)); err != nil {
			return err
		} else if n != int(valueSize) {
			return errors.Wrapf(ErrInvalidEntryHeader, "read value length %d", n)
		}
		e := &entry{
			hdr:   &h,
			key:   key,
			value: value,
		}
		if err := fn(e); err != nil {
			return err
		}
		i += h.entrySize()
	}
	return nil
}

// Close unmaps the segment.
func (s *segment) Close() (err error) {

	return s.mmap.Close()
}

// CanWrite returns true if segment has space to write entry data.
func (s *segment) CanWrite(e *entry) bool {
	return s.size+e.Size() <= SegmentSize
}

// Flush flushes the buffer to disk.
func (s *segment) Flush() error {
	return s.mmap.Sync()
}

// parseSegmentFilename returns the id represented by the hexadecimal filename.
func parseSegmentFilename(filename string) (uint16, error) {
	i, err := strconv.ParseUint(filename, 16, 32)
	return uint16(i), err
}
