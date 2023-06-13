package archivedb

import "fmt"

const (
	hdrSize = 10
)

type flag uint8

const (
	// flagEntryPut means the entry is added
	flagEntryPut flag = 1
	// flagEntryDelete means the entry is deleted
	flagEntryDelete flag = 2
)

// IsEntryPut returns true if the flag is flagEntryPut
func (f flag) isEntryPut() bool {
	return f == flagEntryPut
}

// IsDel returns true if the flag is flagEntryDel
func (f flag) isEntryDelete() bool {
	return f == flagEntryDelete
}

// IsEntryValid returns true if the flag is valid
func (f flag) isEntryValid() bool {
	return f.isEntryPut() || f.isEntryDelete()
}

func (f flag) String() string {
	switch f {
	case flagEntryPut:
		return "EntryPut"
	case flagEntryDelete:
		return "EntryDelete"
	default:
		return "Unknown"
	}
}

/*
* hdr format:
+----------+---------------+---------------+---------------+
| flag(1B) |  keySize (1B) | valueSize (4B)|  checksum (4B)|
+----------+---------------+---------------+---------------+
*
*/

type hdr [hdrSize]byte

func (h *hdr) getFlag() flag {
	return flag((*h)[0])
}

func (h *hdr) setFlag(f flag) *hdr {
	(*h)[0] = byte(f)
	return h
}

func (h *hdr) getKeySize() uint8 {
	return (*h)[1]
}

func (h *hdr) setKeySize(size uint8) *hdr {
	(*h)[1] = size
	return h
}

func (h *hdr) getValueSize() uint32 {
	return uint32((*h)[2]) | uint32((*h)[3])<<8 | uint32((*h)[4])<<16 | uint32((*h)[5])<<24
}

func (h *hdr) setValueSize(size uint32) *hdr {
	(*h)[2] = byte(size)
	(*h)[3] = byte(size >> 8)
	(*h)[4] = byte(size >> 16)
	(*h)[5] = byte(size >> 24)
	return h
}

func (h *hdr) getChecksum() uint32 {
	return uint32((*h)[6]) | uint32((*h)[7])<<8 | uint32((*h)[8])<<16 | uint32((*h)[9])<<24
}

func (h *hdr) setChecksum(checksum uint32) *hdr {
	(*h)[6] = byte(checksum)
	(*h)[7] = byte(checksum >> 8)
	(*h)[8] = byte(checksum >> 16)
	(*h)[9] = byte(checksum >> 24)
	return h
}

func (h *hdr) encode() []byte {
	return (*h)[:]
}

func (h *hdr) entrySize() uint32 {
	return uint32(hdrSize) + uint32(h.getKeySize()) + h.getValueSize()
}

func (h *hdr) String() string {
	return fmt.Sprintf("Flag: %d, KeySize: %d, ValueSize: %d, Checksum: %d",
		h.getFlag(), h.getKeySize(), h.getValueSize(), h.getChecksum())
}
