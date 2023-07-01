package archivedb

import "github.com/millken/archivedb/internal/radixtree"

type index struct {
	seg uint16
	off uint32
}

func loadIndexes(idx *radixtree.Tree[*index], segments []*segment) error {
	for _, segment := range segments {
		for size := uint32(SegmentHeaderSize); size < segment.Size(); {
			buf, err := segment.mmap.ReadOff(int(size), hdrSize)
			if err != nil {
				return err
			}
			h := hdr(buf)
			if !h.getFlag().isEntryValid() {
				break
			}
			off := int(size) + hdrSize
			key, err := segment.mmap.ReadOff(off, int(h.getKeySize()))
			if err != nil {
				return err
			}

			switch h.getFlag() {
			case flagEntryPut:
				idx.Put(key, &index{
					seg: segment.id,
					off: uint32(size),
				})
			case flagEntryDelete:
				idx.Delete(key)
			}
			size += h.entrySize()

		}
	}
	return nil
}
