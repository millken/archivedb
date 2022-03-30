//go:build linux || darwin
// +build linux darwin

package mmap

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"

	"errors"
)

const (
	defaultBufSize = 4096
)

type MapFile struct {
	file *os.File
	data []byte
	size int64
	err  error
	buf  []byte
	n    int
}

func (mf *MapFile) Size() int64 {
	return mf.size
}

func (mf *MapFile) Close() error {
	if mf.file != nil {
		if err := mf.Flush(); err != nil {
			return err
		} else if err := mf.file.Close(); err != nil {
			return err
		}
		mf.file = nil
	}
	if mf.data == nil {
		return nil
	}
	data := mf.data
	mf.data = nil
	runtime.SetFinalizer(mf, nil)
	return syscall.Munmap(data)
}

// Flush writes any buffered data to the underlying io.Writer.
func (mf *MapFile) Flush() error {
	if mf.err != nil {
		return mf.err
	}
	if mf.n == 0 {
		return nil
	}
	n, err := mf.file.Write(mf.buf[0:mf.n])
	if n < mf.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < mf.n {
			copy(mf.buf[0:mf.n-n], mf.buf[n:mf.n])
		}
		mf.n -= n
		mf.err = err
		return err
	}
	mf.n = 0
	return nil
}

// Available returns how many bytes are unused in the buffer.
func (mf *MapFile) Available() int { return len(mf.buf) - mf.n }

// Buffered returns the number of bytes that have been written into the current buffer.
func (mf *MapFile) Buffered() int { return mf.n }

func (mf *MapFile) Write(p []byte) (int, error) {
	var nn int
	for len(p) > mf.Available() && mf.err == nil {
		var n int
		if mf.Buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			n, mf.err = mf.file.Write(p)
		} else {
			n = copy(mf.buf[mf.n:], p)
			mf.n += n
			mf.Flush()
		}
		nn += n
		p = p[n:]
	}
	if mf.err != nil {
		return nn, mf.err
	}
	n := copy(mf.buf[mf.n:], p)
	mf.n += n
	nn += n
	mf.size += int64(nn)
	return nn, nil
}

func (mf *MapFile) ReadAt(p []byte, off int64) (int, error) {
	if mf.data == nil {
		return 0, errors.New("mmap: closed")
	}
	if off < 0 || mf.size < off {
		return 0, fmt.Errorf("mmap: invalid ReadAt offset %d", off)
	}
	ps := int64(len(p))
	if mf.size < ps+off {
		return 0, io.EOF
	}

	n := copy(p, mf.data[off:])
	if n < len(p) {
		n += copy(p[n:], mf.buf[:])
	}
	return n, nil
}
func Open(path string) (*MapFile, error) {
	return OpenWithBufferSize(path, defaultBufSize)
}
func OpenWithBufferSize(path string, bufSize int) (*MapFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := fi.Size()
	if size == 0 {
		return &MapFile{}, nil
	}
	if size < 0 {
		return nil, fmt.Errorf("mmap: file %q has negative size", path)
	}
	if size != int64(int(size)) {
		return nil, fmt.Errorf("mmap: file %q is too large", path)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	if bufSize <= 0 {
		bufSize = defaultBufSize
	}
	m := &MapFile{
		file: f,
		data: data,
		buf:  make([]byte, bufSize),
		size: size,
	}
	runtime.SetFinalizer(m, (*MapFile).Close)
	return m, nil
}
