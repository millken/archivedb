//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd
// +build darwin dragonfly freebsd linux nacl netbsd openbsd

package mmap

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func openFile(filename string, fl Flag) (*File, error) {
	f, err := os.OpenFile(filename, fl.flag(), 0666)
	if err != nil {
		return nil, fmt.Errorf("mmap: could not open %q: %w", filename, err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("mmap: could not stat %q: %w", filename, err)
	}

	size := fi.Size()
	if size == 0 {
		return &File{fd: f, flag: fl, fi: fi}, nil
	}
	if size < 0 {
		return nil, fmt.Errorf("mmap: file %q has negative size", filename)
	}
	if size != int64(int(size)) {
		return nil, fmt.Errorf("mmap: file %q is too large", filename)
	}

	prot := syscall.PROT_READ
	if fl&Write != 0 {
		prot |= syscall.PROT_WRITE
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(size), prot, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap: could not mmap %q: %w", filename, err)
	}
	r := &File{
		data: data,
		ref:  (*[maxBytes]byte)(unsafe.Pointer(&data[0])),
		fd:   f,
		flag: fl,
		fi:   fi,
	}
	if err := unix.Madvise(data, unix.MADV_RANDOM); err != nil && err != unix.ENOSYS {
		// Ignore not implemented error in kernel because it still works.
		return nil, fmt.Errorf("madvise: %s", err)
	}
	runtime.SetFinalizer(r, (*File).Close)
	return r, nil
}

// Sync commits the current contents of the file to stable storage.
func (f *File) Sync() error {
	if !f.wflag() {
		return ErrBadFD
	}
	return unix.Msync(f.data, unix.MS_SYNC)
}

// Close closes the memory-mapped file.
func (f *File) Close() error {
	if f.data == nil {
		return nil
	}
	defer f.Close()

	data := f.data
	f.data = nil
	runtime.SetFinalizer(f, nil)
	return unix.Munmap(data)
}
