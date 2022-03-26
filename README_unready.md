# archivedb

ArchiveDB is an embeddable, persistent and high performance Key/Value database written in pure Go.  

## Features

* Support expiry time, historical version.
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
BenchmarkDB_Put/128BNoSync-8             2006725               557.1 ns/op       229.77 MB/s          40 B/op          2 allocs/op
BenchmarkDB_Put/256BNoSync-8             1848522               643.3 ns/op       397.97 MB/s          40 B/op          2 allocs/op
BenchmarkDB_Put/1KNoSync-8                888787              1338 ns/op         765.37 MB/s          40 B/op          2 allocs/op
BenchmarkDB_Put/2KNoSync-8                515025              2237 ns/op         915.46 MB/s          41 B/op          2 allocs/op
BenchmarkDB_Put/4KNoSync-8                279313              4049 ns/op        1011.66 MB/s          42 B/op          2 allocs/op
BenchmarkDB_Put/8KNoSync-8                161738              7202 ns/op        1137.52 MB/s          44 B/op          2 allocs/op
BenchmarkDB_Put/16KNoSync-8                85713             13616 ns/op        1203.30 MB/s          49 B/op          2 allocs/op
BenchmarkDB_Put/32KNoSync-8                45920             26238 ns/op        1248.88 MB/s          58 B/op          2 allocs/op
BenchmarkDB_Put/128BSync-8                231360              5212 ns/op          24.56 MB/s          70 B/op          2 allocs/op
BenchmarkDB_Put/256BSync-8                228577              5162 ns/op          49.60 MB/s          40 B/op          2 allocs/op
BenchmarkDB_Put/1KSync-8                  199863              5784 ns/op         177.05 MB/s          40 B/op          2 allocs/op
BenchmarkDB_Put/2KSync-8                  170752              6882 ns/op         297.59 MB/s          40 B/op          2 allocs/op
BenchmarkDB_Put/4KSync-8                  134230              8969 ns/op         456.69 MB/s          41 B/op          2 allocs/op
BenchmarkDB_Put/8KSync-8                   91084             12687 ns/op         645.73 MB/s          42 B/op          2 allocs/op
BenchmarkDB_Put/16KSync-8                  10000            442189 ns/op          37.05 MB/s          45 B/op          2 allocs/op
BenchmarkDB_Put/32KSync-8                   1670            614510 ns/op          53.32 MB/s          49 B/op          2 allocs/op
BenchmarkDB_Get/128B-8                   6254989               190.6 ns/op       671.67 MB/s         219 B/op          4 allocs/op
BenchmarkDB_Get/256B-8                   5705352               210.8 ns/op      1214.59 MB/s         347 B/op          4 allocs/op
BenchmarkDB_Get/512B-8                   4382640               242.7 ns/op      2109.44 MB/s         603 B/op          4 allocs/op
BenchmarkDB_Get/1K-8                     3646017               304.7 ns/op      3360.56 MB/s        1115 B/op          4 allocs/op
BenchmarkDB_Get/2K-8                     2708673               417.4 ns/op      4906.87 MB/s        2139 B/op          4 allocs/op
BenchmarkDB_Get/4K-8                     1779187               668.7 ns/op      6125.38 MB/s        4187 B/op          4 allocs/op
BenchmarkDB_Get/8K-8                     1000000              1157 ns/op        7079.92 MB/s        8283 B/op          4 allocs/op
BenchmarkDB_Get/16K-8                     579628              1956 ns/op        8378.31 MB/s       16475 B/op          4 allocs/op
BenchmarkDB_Get/32K-8                     292502              3649 ns/op        8980.82 MB/s       32859 B/op          4 allocs/op
PASS
ok      github.com/millken/archivedb    37.437s
```