package mmap

import (
	"errors"
	"io"
	"os"
	"unsafe"
)

var (
	ErrBadFD            = errors.New("mmap: bad file descriptor")
	ErrClosed           = errors.New("mmap: closed")
	ErrInvalidOffset    = errors.New("mmap: invalid offset")
	ErrNegativePosition = errors.New("mmap: negative position")
	ErrInvalidWhence    = errors.New("mmap: invalid whence")
)

// Flag specifies how a mmap file should be opened.
type Flag int

const (
	maxBytes      = 1<<31 - 1
	Read     Flag = 0x1 // Read enables read-access to a mmap file.
	Write    Flag = 0x2 // Write enables write-access to a mmap file.
)

func (fl Flag) flag() int {
	var flag int

	switch fl {
	case Read:
		flag = os.O_RDONLY
	case Write:
		flag = os.O_WRONLY
	case Read | Write:
		flag = os.O_RDWR
	}

	return flag
}

type File struct {
	data []byte
	c    int
	ref  *[maxBytes]byte
	fd   *os.File
	flag Flag
	fi   os.FileInfo
}

// Open memory-maps the named file for reading.
func Open(filename string) (*File, error) {
	return openFile(filename, Read)
}

// OpenFile memory-maps the named file for reading/writing, depending on
// the flag value.
func OpenFile(filename string, flag Flag) (*File, error) {
	return openFile(filename, flag)
}

// Len returns the length of the underlying memory-mapped file.
func (f *File) Len() int {
	return len(f.data)
}

// Stat returns the FileInfo structure describing file.
// If there is an error, it will be of type *os.PathError.
func (f *File) Stat() (os.FileInfo, error) {
	if f == nil {
		return nil, os.ErrInvalid
	}

	return f.fi, nil
}

func (f *File) rflag() bool {
	return f.flag&Read != 0
}

func (f *File) wflag() bool {
	return f.flag&Write != 0
}

// Read implements the io.Reader interface.
func (f *File) Read(p []byte) (int, error) {
	if f == nil {
		return 0, os.ErrInvalid
	}

	if !f.rflag() {
		return 0, ErrBadFD
	}
	if f.c >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.c:])
	f.c += n
	return n, nil
}

// ReadAt implements the io.ReaderAt interface.
func (f *File) ReadAt(p []byte, off int64) (int, error) {
	if f == nil {
		return 0, os.ErrInvalid
	}

	if !f.rflag() {
		return 0, ErrBadFD
	}
	if f.data == nil {
		return 0, ErrClosed
	}
	if off < 0 || int64(len(f.data)) < off {
		return 0, ErrInvalidOffset
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Write implements the io.Writer interface.
func (f *File) Write(p []byte) (int, error) {
	if f == nil {
		return 0, os.ErrInvalid
	}

	if !f.wflag() {
		return 0, ErrBadFD
	}
	if f.c >= len(f.data) {
		return 0, io.ErrShortWrite
	}
	n := copy(f.data[f.c:], p)
	f.c += n
	if len(p) > n {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// WriteAt implements the io.WriterAt interface.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if f == nil {
		return 0, os.ErrInvalid
	}

	if !f.wflag() {
		return 0, ErrBadFD
	}
	if f.data == nil {
		return 0, ErrClosed
	}
	if off < 0 || int64(len(f.data)) < off {
		return 0, ErrInvalidOffset
	}
	n := copy(f.data[off:], p)
	if n < len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if f == nil {
		return 0, os.ErrInvalid
	}

	switch whence {
	case io.SeekStart:
		f.c = int(offset)
	case io.SeekCurrent:
		f.c += int(offset)
	case io.SeekEnd:
		f.c = len(f.data) - int(offset)
	default:
		return 0, ErrInvalidWhence
	}
	if f.c < 0 {
		return 0, ErrNegativePosition
	}
	return int64(f.c), nil
}

func (f *File) ReadOff(off, length int) ([]byte, error) {
	if !f.rflag() {
		return nil, ErrBadFD
	}
	if f.data == nil {
		return nil, ErrClosed
	}
	if off < 0 || length < 0 || (off+length) > len(f.data) {
		return nil, ErrInvalidOffset
	}
	return unsafeByteSlice(unsafe.Pointer(f.ref), 0, off, off+length), nil
}
