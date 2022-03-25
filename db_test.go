package archivedb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	//test delete
	for _, test := range tests {
		err := db.Delete(test.key)
		require.NoError(err)
		v, err = db.Get([]byte("foo"))
		require.ErrorIs(err, ErrKeyDeleted)
		require.Nil(v)
	}
	require.NoError(db.Close())
}

// Tests multiple goroutines simultaneously opening a database.
func TestOpen_MultipleGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	const (
		instances  = 30
		iterations = 30
	)
	testFile := "db002.test"
	defer os.Remove(testFile)
	defer os.Remove(testFile + ".idx")
	var wg sync.WaitGroup
	errCh := make(chan error, iterations*instances)
	for iteration := 0; iteration < iterations; iteration++ {
		for instance := 0; instance < instances; instance++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				db, err := Open(testFile)
				if err != nil {
					errCh <- err
					return
				}
				if err := db.Close(); err != nil {
					errCh <- err
					return
				}
			}()
		}
		wg.Wait()
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("error from inside goroutine: %v", err)
		}
	}
}

func TestDB_Concurrent(t *testing.T) {
	require := require.New(t)
	testFile := "db003.test"
	defer os.Remove(testFile)
	defer os.Remove(testFile + ".idx")
	db, err := Open(testFile)
	require.NoError(err)
	require.NotNil(db)
	const n, secs, maxkey = 4, 6, 1000

	runtime.GOMAXPROCS(runtime.NumCPU())

	var (
		closeWg sync.WaitGroup
		stop    uint32
		cnt     [n]uint32
	)

	for i := 0; i < n; i++ {
		closeWg.Add(1)
		go func(i int) {
			var put, get, found uint
			defer func() {
				t.Logf("goroutine %d stopped after %d ops, put=%d get=%d found=%d missing=%d",
					i, cnt[i], put, get, found, get-found)
				closeWg.Done()
			}()

			rnd := rand.New(rand.NewSource(int64(1000 + i)))
			for atomic.LoadUint32(&stop) == 0 {
				x := cnt[i]

				k := rnd.Intn(maxkey)
				kstr := fmt.Sprintf("%016d", k)

				if (rnd.Int() % 2) > 0 {
					put++
					db.Put([]byte(kstr), []byte(fmt.Sprintf("%d.%d.%-1000d", k, i, x)))
				} else {
					get++
					v, err := db.Get([]byte(kstr))
					if err == nil {
						found++
						rk, ri, rx := 0, -1, uint32(0)
						fmt.Sscanf(string(v), "%d.%d.%d", &rk, &ri, &rx)
						if rk != k {
							t.Errorf("invalid key want=%d got=%d", k, rk)
						}
						if ri < 0 || ri >= n {
							t.Error("invalid goroutine number: ", ri)
						} else {
							tx := atomic.LoadUint32(&(cnt[ri]))
							if rx > tx {
								t.Errorf("invalid seq number, %d > %d ", rx, tx)
							}
						}
					} else if err != ErrKeyNotFound {
						t.Error("Get: got error: ", err)
						return
					}
				}
				atomic.AddUint32(&cnt[i], 1)
			}
		}(i)
	}

	time.Sleep(secs * time.Second)
	atomic.StoreUint32(&stop, 1)
	closeWg.Wait()
}

func TestRandomWrites(t *testing.T) {
	require := require.New(t)
	testFile := "db004.test"
	defer os.Remove(testFile)
	defer os.Remove(testFile + ".idx")
	db, err := Open(testFile)
	require.NoError(err)

	keys := [64][]byte{}
	wants := [64]int{}
	for k := range keys {
		keys[k] = []byte(strconv.Itoa(k))
		wants[k] = -1
	}
	xxx := bytes.Repeat([]byte("x"), 512)

	rng := rand.New(rand.NewSource(123))
	const N = 1000
	for i := 0; i < N; i++ {
		k := rng.Intn(len(keys))
		if rng.Intn(20) != 0 {
			wants[k] = rng.Intn(len(xxx) + 1)
			if err := db.Put(keys[k], xxx[:wants[k]]); err != nil {
				t.Fatalf("i=%d: Put: %v", i, err)
			}
		} else {
			wants[k] = -1
			if err := db.Delete(keys[k]); err != nil {
				t.Fatalf("i=%d: Delete: %v", i, err)
			}
		}

		if i != N-1 || rng.Intn(50) != 0 {
			continue
		}
		for k := range keys {
			got := -1
			if v, err := db.Get(keys[k]); err != nil {
				if err != ErrKeyNotFound {
					t.Fatalf("Get: %v", err)
				}
			} else {
				got = len(v)
			}
			if got != wants[k] {
				t.Errorf("i=%d, k=%d: got %d, want %d", i, k, got, wants[k])
			}
		}
	}

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
		testdir, err := ioutil.TempDir(currentDir, "_bench")
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

	testdir, err := ioutil.TempDir(currentDir, "_bench")
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

func BenchmarkDB_Delete(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	testdir, err := ioutil.TempDir(currentDir, "_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testdir)
	testfile := filepath.Join(testdir, "db003.bench")
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	const keyCount = 10000
	var keys [keyCount][]byte
	for i := 0; i < keyCount; i++ {
		keys[i] = []byte(strconv.Itoa(rng.Int()))
	}
	val := bytes.Repeat([]byte("x"), 10)
	db, err := Open(testfile)
	if err != nil {
		b.Fatal(err)
	}
	for _, key := range keys {
		_ = db.Put(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = db.Delete(keys[i%keyCount])
		if err != nil {
			b.Fatal(err)
		}

	}
	b.StopTimer()

}
