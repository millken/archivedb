package archivedb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	require := require.New(t)
	testFile := "storage001.test"
	defer os.Remove(testFile)
	storage, err := openStorage(testFile)
	require.NoError(err)
	require.NotNil(storage)
	testEntry := []*Entry{
		NewEntry([]byte("foo"), []byte("bar")),
		NewEntry([]byte("foo1"), []byte("bar1")),
		NewEntry([]byte("foo2"), []byte("bar2")),
	}
	for _, entry := range testEntry {
		err = storage.writeEntry(entry)
		require.NoError(err)
	}
	require.NoError(storage.Close())

	storage, err = openStorage(testFile)
	require.NoError(err)
	require.NotNil(storage)
	off := uint64(storageMetaSize)
	for _, entry := range testEntry {
		v, err := storage.readEntry(off)
		require.NoError(err)
		off += entry.Size()
		require.Equal(entry, v)
	}
}

func BenchmarkStorageSet(b *testing.B) {
	b.ReportAllocs()
	require := require.New(b)
	testFile := "storage002.test"
	defer os.Remove(testFile)

	storage, err := openStorage(testFile)
	require.NoError(err)
	e := NewEntry([]byte("foo"), []byte("bar"))
	for i := 1; i < b.N; i++ {
		storage.writeEntry(e)
	}
}

func BenchmarkStorageGet(b *testing.B) {
	b.ReportAllocs()
	require := require.New(b)
	testFile := "storage003.test"
	defer os.Remove(testFile)

	storage, err := openStorage(testFile)
	require.NoError(err)
	e := NewEntry([]byte("foo"), []byte("bar"))
	n := 1000
	offs := make([]uint64, n)
	for i := 0; i < n; i++ {
		storage.writeEntry(e)
		offs[i] = uint64(i) * e.Size()
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		storage.readEntry(offs[i%n])
	}
}
