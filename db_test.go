package archivedb

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type benchmarkTestCase struct {
	name string
	size int
}

func TestDB(t *testing.T) {
	require := require.New(t)
	testFile := "db001.test"
	defer os.Remove(testFile)
	defer os.Remove(testFile + ".idx")
	db, err := Open(testFile)
	require.NoError(err)
	require.NotNil(db)
	tests := []struct {
		key, value []byte
	}{
		{[]byte("foo"), []byte("bar")},
		{[]byte("foo1"), []byte("bar1")},
		{[]byte("foo2"), []byte("bar2")},
	}
	for _, test := range tests {
		err = db.Put(test.key, test.value)
		require.NoError(err)
	}
	for _, test := range tests {
		v, err := db.Get(test.key)
		require.NoError(err)
		require.Equal(test.value, v)
	}

	//test key not exist
	v, err := db.Get([]byte("not_exist"))
	require.ErrorIs(err, ErrKeyNotFound)
	require.Nil(v)
	require.NoError(db.Close())
}

func BenchmarkDB_Put(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	variants := map[string][]Option{
		"NoSync": {
			FsyncOption(false),
		},
		"Sync": {
			FsyncOption(true),
		},
	}

	for name, options := range variants {
		testdir, err := ioutil.TempDir(currentDir, "archivedb_bench")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(testdir)
		testfile := filepath.Join(testdir, "db001.bench")

		db, err := Open(testfile, options...)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		for _, tt := range tests {
			b.Run(tt.name+name, func(b *testing.B) {
				b.SetBytes(int64(tt.size))

				key := []byte("foo")
				value := []byte(strings.Repeat(" ", tt.size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					err := db.Put(key, value)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkDB_Get(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	testdir, err := ioutil.TempDir(currentDir, "archivedb_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)
	testfile := filepath.Join(testdir, "db002.bench")

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))

			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))

			options := []Option{}
			db, err := Open(testfile, options...)
			if err != nil {
				b.Fatal(err)
			}

			err = db.Put(key, value)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				val, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(val, value) {
					b.Errorf("unexpected value")
				}
			}
			b.StopTimer()
			db.Close()
		})
	}
}
