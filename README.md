# archivedb

ArchiveDB is an embeddable, persistent and high performance Key/Value database written in pure Go.
Usually used where the data doesn't change much.

## Features

* Can store larger-than-memory data.
* Low memory usage.
* Safe for concurrent use by multiple goroutines.
* High throughput (See: [Performance](README.md#Performance) )

## Getting Started 

```go
package main

import (
	"log"
	"github.com/millken/archivedb"
)

func main() {
    db, _ := archivedb.Open("/tmp/db")
    defer db.Close()
    db.Put([]byte("Hello"), []byte("World"))
    val, _ := db.Get([]byte("Hello"))
    log.Printf("Hello %s", val)
}
```

## Performance

Benchmarks run on a Mac mini (M1 16G, 2020):

```sh
$ go test -benchmem -run=^$ -bench ^BenchmarkDB  
goos: darwin
goarch: arm64
pkg: github.com/millken/archivedb
BenchmarkDB_Put/128BNoSync-8             5812340               175.7 ns/op       728.50 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/256BNoSync-8             4740873               249.0 ns/op      1028.05 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/1KNoSync-8               1763916               644.7 ns/op      1588.41 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/2KNoSync-8               1000000              1122 ns/op        1826.03 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/4KNoSync-8                637262              2160 ns/op        1895.88 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/8KNoSync-8                218953              4923 ns/op        1664.09 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/16KNoSync-8                90084             14746 ns/op        1111.10 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/32KNoSync-8                38570             36176 ns/op         905.80 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/128BSync-8                 27231             47542 ns/op           2.69 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/256BSync-8                 23424             57431 ns/op           4.46 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/1KSync-8                   16497            220525 ns/op           4.64 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/2KSync-8                   10000            123557 ns/op          16.58 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/4KSync-8                    7692            170008 ns/op          24.09 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/8KSync-8                    5910            244692 ns/op          33.48 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/16KSync-8                   4047            338585 ns/op          48.39 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Put/32KSync-8                   4069            256939 ns/op         127.53 MB/s          32 B/op          2 allocs/op
BenchmarkDB_Get/128B-8                  16133332                74.65 ns/op     1714.63 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/256B-8                  14335069                84.11 ns/op     3043.46 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/512B-8                  10319188               116.9 ns/op      4380.53 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/1K-8                     5833773               205.9 ns/op      4973.34 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/2K-8                     3458556               348.8 ns/op      5870.92 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/4K-8                     1864291               637.6 ns/op      6423.74 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/8K-8                      962701              1275 ns/op        6427.36 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/16K-8                     496244              2402 ns/op        6820.14 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/32K-8                     252752              4756 ns/op        6889.57 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Delete-8                     8119098               149.8 ns/op            32 B/op          2 allocs/op
PASS
ok      github.com/millken/archivedb    45.040s
```

## License

Source code is available under the Apache License 2.0 [License](/LICENSE).