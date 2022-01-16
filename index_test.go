package archivedb

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexRecord(t *testing.T) {
	require := require.New(t)
	testCase := []*indexRecord{
		{1, 2, 3, 4},
		{4, 5, 6, 7},
	}
	resCase := [][]byte{
		[]byte([]byte{0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x4}),
		[]byte([]byte{0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x6, 0x0, 0x0, 0x0, 0x7}),
	}
	for i, test := range testCase {
		require.Equal(test.Bytes(), resCase[i])
	}
}

func TestIndexhashmap(t *testing.T) {
	require := require.New(t)
	absPath, _ := filepath.Abs("./")
	testFile := path.Join(absPath, "index001.test")
	defer os.Remove(testFile)
	idxOpts := &indexOptions{
		filePath: testFile,
	}
	idx, err := openIndex(idxOpts)
	require.NoError(err)
	testCase := []struct {
		k uint32
		v uint32
	}{
		{1, 2},
		{3, 4},
		{100, 200},
		{100000, 300000},
	}
	for _, test := range testCase {
		idx.hashmapSet(test.k, test.v)
		v, ok := idx.hashmapGet(test.k)
		require.True(ok)
		require.Equal(test.v, v)
	}

	//key not exist
	v, ok := idx.hashmapGet(2)
	require.False(ok)
	require.Equal(v, uint32(0))
}

func TestIndex(t *testing.T) {
	require := require.New(t)
	absPath, _ := filepath.Abs("./")
	testFile := path.Join(absPath, "index002.test")
	defer os.Remove(testFile)
	idxOpts := &indexOptions{
		filePath: testFile,
	}
	idx, err := openIndex(idxOpts)
	require.NoError(err)
	require.Equal(idx.totalKeys, uint32(0))
	require.Equal(idx.activeKeys, uint32(0))

	for i := 1; i <= 5; i++ {
		rec := &indexRecord{uint32(i), uint64(i), uint32(i), uint32(i)}
		require.NoError(idx.PutRecord(rec))
	}
	require.Equal(idx.totalKeys, uint32(5))
	require.Equal(idx.activeKeys, uint32(5))

	for i := 1; i <= 5; i++ {
		ir, err := idx.GetRecord(uint32(i) - 1)
		require.NoError(err)
		require.Equal(ir.keyHash, uint32(i))
		require.Equal(ir.dataOffset, uint64(i))
		require.Equal(ir.dataSize, uint32(i))
		require.Equal(ir.dataCRC32, uint32(i))
	}
}

func BenchmarkIndexPutRecord(b *testing.B) {
	require := require.New(b)
	absPath, _ := filepath.Abs("./")
	testFile := path.Join(absPath, "index005.test")
	defer os.Remove(testFile)
	idxOpts := &indexOptions{
		filePath: testFile,
	}
	idx, err := openIndex(idxOpts)
	require.NoError(err)
	for i := 1; i < b.N; i++ {
		rec := &indexRecord{uint32(i), uint64(i), uint32(i), uint32(i)}
		idx.PutRecord(rec)
	}
}

func BenchmarkIndexGetRecord(b *testing.B) {
	require := require.New(b)
	absPath, _ := filepath.Abs("./")
	testFile := path.Join(absPath, "index006.test")
	defer os.Remove(testFile)
	idxOpts := &indexOptions{
		filePath: testFile,
	}
	idx, err := openIndex(idxOpts)
	require.NoError(err)
	rec := &indexRecord{uint32(1), uint64(1), uint32(1), uint32(1)}
	idx.PutRecord(rec)
	for i := 0; i < b.N; i++ {
		idx.GetRecord(0)
	}
}
