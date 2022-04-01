//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd
// +build darwin dragonfly freebsd linux nacl netbsd openbsd

package mmap

import (
	"syscall"
	"unsafe"
)

const (
	maxAllocSize = 1<<31 - 1
)

type MMap struct {
	dataref []byte
	data    *[maxAllocSize]byte
	datasz  int
}

func (m *MMap) Close() error {
	if m.dataref == nil {
		return nil
	}
	data := m.dataref
	m.dataref = nil
	m.data = nil
	m.datasz = 0
	//runtime.SetFinalizer(m, nil)
	return syscall.Munmap(data)
}

func (m *MMap) Read(off, len int) []byte {
	return unsafeByteSlice(unsafe.Pointer(m.data), 0, off, off+len)
}

func Map(fd int, sz int) (*MMap, error) {

	if sz == 0 {
		return &MMap{}, nil
	}
	data, err := syscall.Mmap(fd, 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	m := &MMap{
		dataref: data,
		data:    (*[maxAllocSize]byte)(unsafe.Pointer(&data[0])),
		datasz:  sz,
	}
	//runtime.SetFinalizer(m, (*MMap).Close)
	return m, nil
}
