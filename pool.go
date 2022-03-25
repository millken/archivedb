package archivedb

import (
	"sync"
)

var byte20Pool = &sync.Pool{
	New: func() interface{} {
		b := make([]byte, 20)
		return &b
	},
}

func acquireByte20() *[]byte {
	b := byte20Pool.Get().(*[]byte)
	return b
}

func releaseByte20(buf *[]byte) {
	if cap(*buf) > 20 {
		return
	}
	byte20Pool.Put(buf)
}

var entryPool = &sync.Pool{
	New: func() interface{} {
		return NewEntry(nil, nil)
	},
}

func acquireEntry() *Entry {
	v := entryPool.Get().(*Entry)
	return v
}

func releaseEntry(v *Entry) {
	entryPool.Put(v)
}

var entryHeaderPool = &sync.Pool{
	New: func() interface{} {
		return new(entryHeader)
	},
}

func acquireEntryHeader() *entryHeader {
	v := entryHeaderPool.Get().(*entryHeader)
	return v
}

func releaseEntryHeader(v *entryHeader) {
	entryHeaderPool.Put(v)
}
