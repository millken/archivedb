package mmap

import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openFile(filename string, fl Flag) (*File, error) {
	f, err := os.OpenFile(filename, fl.flag(), 0666)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
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

	prot := uint32(windows.PAGE_READONLY)
	view := uint32(windows.FILE_MAP_READ)
	if fl&Write != 0 {
		prot = windows.PAGE_READWRITE
		view = windows.FILE_MAP_WRITE
	}

	low, high := uint32(size)&0xffffffff, uint32(size>>32)
	fmap, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, prot, high, low, nil)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(fmap)
	ptr, err := windows.MapViewOfFile(fmap, view, 0, 0, uintptr(size))
	if err != nil {
		return nil, err
	}
	data := (*[maxBytes]byte)(unsafe.Pointer(ptr))[:size]

	fd := &File{
		data: data,
		ref:  (*[maxBytes]byte)(unsafe.Pointer(&data[0])),
		fd:   f,
		fi:   fi,
		flag: fl,
	}
	runtime.SetFinalizer(fd, (*File).Close)
	return fd, nil

}

// Sync commits the current contents of the file to stable storage.
func (f *File) Sync() error {
	if !f.wflag() {
		return ErrBadFD
	}

	err := windows.FlushViewOfFile(f.addr(), uintptr(len(f.data)))
	if err != nil {
		return fmt.Errorf("mmap: could not sync view: %w", err)
	}

	err = windows.FlushFileBuffers(windows.Handle(f.fd.Fd()))
	if err != nil {
		return fmt.Errorf("mmap: could not sync file buffers: %w", err)
	}

	return nil
}

// Close closes the reader.
func (f *File) Close() error {
	if f.data == nil {
		return nil
	}
	defer f.fd.Close()

	addr := f.addr()
	f.data = nil
	runtime.SetFinalizer(f, nil)
	return windows.UnmapViewOfFile(addr)
}

func (f *File) addr() uintptr {
	data := f.data
	return uintptr(unsafe.Pointer(&data[0]))
}
