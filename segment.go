package archivedb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strconv"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

const (
	SegmentVersion        = 1
	SegmentMagic          = "ArSeG"
	SegmentSize    uint32 = 1 << 30 // 1GB

	SegmentHeaderSize = 6 // magic + version + id
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
	id   uint16
	path string

	data mmap.MMap
	size uint32 // current file size
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
	} else if err := f.Truncate(int64(SegmentSize)); err != nil {
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

// Data returns the raw data.
func (s *segment) Data() []byte { return s.data }

// ID returns the id the segment was initialized with.
func (s *segment) ID() uint16 { return s.id }

// Size returns the size of the data in the segment.
// This is only populated once InitForWrite() is called.
func (s *segment) Size() uint32 { return s.size }

func (s *segment) Open() error {
	if err := func() (err error) {
		f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		if s.data, err = mmap.Map(f, mmap.RDWR, 0); err != nil {
			return err
		}

		// Read header.
		hdr, err := decodeSegmentHeader(s.data)
		if err != nil {
			return err
		} else if hdr.Version != SegmentVersion {
			return ErrInvalidSegmentVersion
		}
		for s.size = uint32(SegmentHeaderSize); s.size < uint32(len(s.data)); {
			hdr, err := readEntryHeader(s.data[s.size : s.size+EntryHeaderSize])
			if err != nil {
				return err
			}
			if !isValidEntryFlag(EntryFlag(hdr.Flag)) {
				break
			}
			s.size += hdr.EntrySize()
		}
		return nil
	}(); err != nil {
		s.Close()
		return err
	}

	return nil
}

func (s *segment) WriteEntry(e entry) error {
	if !s.CanWrite(e) {
		return ErrSegmentNotWritable
	}

	n := copy(s.data[s.size:], e.hdr.Encode())
	if n != EntryHeaderSize {
		return errors.Wrapf(ErrInvalidEntryHeader, "write entry header length %d", n)
	}
	s.size += uint32(n)

	n = copy(s.data[s.size:], e.key)
	if n != int(e.hdr.KeySize) {
		return errors.Wrapf(ErrInvalidEntryHeader, "write key length %d", n)
	}
	s.size += uint32(n)

	n = copy(s.data[s.size:], e.value)
	if n != int(e.hdr.ValueSize) {
		return errors.Wrapf(ErrInvalidEntryHeader, "write value length %d", n)
	}
	s.size += uint32(n)
	return nil
}

func (s *segment) ReadEntry(off uint32) (e entry, err error) {
	if off >= s.size {
		return e, errors.Wrap(ErrInvalidOffset, "request offset exceeds segment size")
	}
	e.hdr, err = readEntryHeader(s.data[off : off+EntryHeaderSize])
	if err != nil {
		return e, err
	}
	if !isValidEntryFlag(EntryFlag(e.hdr.Flag)) {
		return e, errors.Wrap(ErrInvalidOffset, "invalid entry flag")
	}
	start := off + EntryHeaderSize
	e.key = s.data[start : start+uint32(e.hdr.KeySize)]
	e.value = s.data[start+uint32(e.hdr.KeySize) : start+uint32(e.hdr.KeySize)+uint32(e.hdr.ValueSize)]
	return
}

func (s *segment) ForEachEntry(fn func(e entry) error) error {
	if s.data == nil {
		return ErrSegmentNotWritable
	}
	for i := uint32(SegmentHeaderSize); i < s.size; {
		hdr, err := readEntryHeader(s.data[i : i+EntryHeaderSize])
		if err != nil {
			return err
		}
		if !isValidEntryFlag(EntryFlag(hdr.Flag)) {
			break
		}
		key := s.data[i+EntryHeaderSize : i+EntryHeaderSize+uint32(hdr.KeySize)]
		value := s.data[i+EntryHeaderSize+uint32(hdr.KeySize) : i+hdr.EntrySize()]
		e := createEntry(hdr.Flag, key, value)
		if err := fn(e); err != nil {
			return err
		}
		i += hdr.EntrySize()
	}
	return nil
}

// Close unmaps the segment.
func (s *segment) Close() (err error) {
	if s.data != nil {
		if e := s.data.Unmap(); e != nil && err == nil {
			err = e
		}
		s.data = nil
	}

	return err
}

// CanWrite returns true if segment has space to write entry data.
func (s *segment) CanWrite(e entry) bool {
	return s.size+e.Size() <= SegmentSize
}

// Flush flushes the buffer to disk.
func (s *segment) Flush() error {
	return s.data.Flush()
}

// parseSegmentFilename returns the id represented by the hexadecimal filename.
func parseSegmentFilename(filename string) (uint16, error) {
	i, err := strconv.ParseUint(filename, 16, 32)
	return uint16(i), err
}
