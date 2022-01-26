package archivedb

import (
	"sync"
)

var byte20Pool = &sync.Pool{
	New: func() interface{} {
		b := make([]byte, 20)
		return b
	},
}

func acquireByte20() []byte {
	b := byte20Pool.Get().([]byte)
	return b
}

func releaseByte20(buf []byte) {
	if cap(buf) > 20 {
		return
	}
	byte20Pool.Put(buf)
}

var indexRecordPool = &sync.Pool{
	New: func() interface{} {
		return new(indexRecord)
	},
}

func acquireIndexRecord() *indexRecord {
	v := indexRecordPool.Get().(*indexRecord)
	return v
}

func releaseIndexRecord(v *indexRecord) {
	indexRecordPool.Put(v)
}
