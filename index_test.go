package archivedb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	require := require.New(t)
	testFile := "index001.test"
	defer os.Remove(testFile)
	idx, err := openIndex(testFile)
	require.NoError(err)

	require.Equal(idx.Length(), int64(0))
	for i := uint64(1); i <= 5; i++ {
		require.NoError(idx.Insert(i, uint16(i), uint32(i)))
	}

	for i := uint64(1); i <= 5; i++ {
		it, ok := idx.Get(i)
		require.True(ok)
		require.Equal(it.ID(), uint16(i))
		require.Equal(it.Offset(), uint32(i))
	}
	require.Equal(idx.Length(), int64(5))
	require.NoError(idx.Close())
}

func BenchmarkIndexSet(b *testing.B) {
	b.ReportAllocs()
	require := require.New(b)
	testFile := "index002.test"
	defer os.Remove(testFile)

	idx, err := openIndex(testFile)
	require.NoError(err)
	for i := 0; i < b.N; i++ {
		if err = idx.Insert(uint64(i), uint16(i), uint32(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexSetSync(b *testing.B) {
	b.ReportAllocs()
	require := require.New(b)
	testFile := "index003.test"
	defer os.Remove(testFile)

	idx, err := openIndex(testFile)
	require.NoError(err)
	for i := 0; i < b.N; i++ {
		if err := idx.Insert(uint64(i), uint16(i), uint32(i)); err != nil {
			b.Fatal(err)
		}
		if err := idx.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexGet(b *testing.B) {
	b.ReportAllocs()
	require := require.New(b)
	testFile := "index002.test"
	defer os.Remove(testFile)

	idx, err := openIndex(testFile)
	require.NoError(err)
	n := 1000
	for i := 0; i < n; i++ {
		idx.Insert(uint64(i), uint16(i), uint32(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := idx.Get(uint64(i % 1000)); !ok {
			b.Fatal("not found")
		}
	}
}
