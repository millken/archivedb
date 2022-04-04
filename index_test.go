package archivedb

import (
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	require := require.New(t)
	testFile := "index001.test"
	if err := os.Remove(testFile); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
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
	idx, err = openIndex(testFile)
	require.NoError(err)
	tests := []struct {
		k uint64
		s uint16
		o uint32
	}{
		{65538, 15, 65538},
	}
	for _, tt := range tests {
		if err := idx.Insert(tt.k, tt.s, tt.o); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range tests {
		it, ok := idx.Get(tt.k)
		require.True(ok)
		require.Equal(it.ID(), tt.s)
		require.Equal(it.Offset(), tt.o)
	}
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
	require.NoError(idx.Close())
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

func TestIndexWrite(t *testing.T) {
	require := require.New(t)
	testFile := "index002.test"
	defer os.Remove(testFile)
	idx, err := openIndex(testFile)
	require.NoError(err)
	n := 1000000
	for i := 1; i <= n; i++ {
		require.NoError(idx.Insert(uint64(i), uint16(i%math.MaxUint16), uint32(i)))
	}

	require.Equal(idx.Length(), int64(n))
	require.NoError(idx.Close())
}

func BenchmarkIndexGet(b *testing.B) {
	b.ReportAllocs()
	require := require.New(b)
	testFile := "index002.test"
	defer os.Remove(testFile)

	idx, err := openIndex(testFile)
	require.NoError(err)
	n := 1000000
	for i := 1; i <= n; i++ {
		if err := idx.Insert(uint64(i), uint16(i%math.MaxUint16), uint32(i)); err != nil {
			b.Fatal(err)
		}
	}
	require.NoError(idx.Close())
	b.ResetTimer()
	idx, err = openIndex(testFile)
	require.NoError(err)
	for i := 1; i < b.N; i++ {
		k := uint64(i % n)
		if k == 0 {
			continue
		}
		if _, ok := idx.Get(k); !ok {
			b.Fatalf("not found key: %d", k)
		}
	}
	require.NoError(idx.Close())
}
