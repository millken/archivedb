package archivedb

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	file := filepath.Join(dir, "segment001.test")
	segment, err := createSegment(0, file)
	if err != nil {
		t.Fatal(err)
	}

	// Write initial entry.
	entry1 := createEntry(EntryInsertFlag, []byte("foo"), []byte("bar"))
	err = segment.WriteEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}

	// Write a large entry (3mb).
	entry2 := createEntry(EntryInsertFlag, []byte("foo1"), bytes.Repeat([]byte("m"), 3*(1<<20)))
	err = segment.WriteEntry(entry2)
	if err != nil {
		t.Fatal(err)
	}

	// Write another entry that is too large for the remaining segment space.
	if err := segment.WriteEntry(createEntry(EntryInsertFlag, []byte("foo2"), bytes.Repeat([]byte("n"), (1<<30)))); err != ErrSegmentNotWritable {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := segment.Close(); err != nil {
		t.Fatal(err)
	}
	segment = newSegment(0, file)
	if err := segment.Open(); err != nil {
		t.Fatal(err)
	}

	// Verify two entries exist.
	var n int
	segment.ForEachEntry(func(e entry) error {
		switch n {
		case 0:
			if e.hdr.Flag != EntryInsertFlag || !bytes.Equal(entry1.key, e.key) || !bytes.Equal(entry1.value, e.value) {
				t.Fatalf("unexpected entry(0): %q", e)
			}
		case 1:
			if e.hdr.Flag != EntryInsertFlag || !bytes.Equal(entry2.key, e.key) || !bytes.Equal(entry2.value, e.value) {
				t.Fatalf("unexpected entry(1): %q", e)
			}
		default:
			t.Fatalf("too many entries")
		}
		n++
		return nil
	})
	if n != 2 {
		t.Fatalf("unexpected entry count: %d", n)
	}
	if err := segment.Close(); err != nil {
		t.Fatal(err)
	}
}
