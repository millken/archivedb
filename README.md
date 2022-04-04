[![Test status](https://github.com/millken/archivedb/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/millken/archivedb/actions?workflow=test)
[![Coverage Status](https://coveralls.io/repos/github/millken/archivedb/badge.svg?branch=main)](https://coveralls.io/github/millken/archivedb?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/millken/archivedb)](https://goreportcard.com/report/github.com/millken/archivedb)
[![GoDev](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/millken/archivedb)
[![GitHub release](https://img.shields.io/github/release/millken/archivedb.svg)](https://github.com/millken/archivedb/releases)

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
BenchmarkDB_Put/128BSync-8                 27298             46144 ns/op           2.77 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/256BSync-8                 23760             57195 ns/op           4.48 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/1KSync-8                   16921            103651 ns/op           9.88 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/2KSync-8                   10000            131920 ns/op          15.52 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/4KSync-8                    7287            179634 ns/op          22.80 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/8KSync-8                    5314            242691 ns/op          33.75 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/16KSync-8                   4059            344744 ns/op          47.53 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/32KSync-8                   2907            473765 ns/op          69.17 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/128BNoSync-8             7369100               168.0 ns/op       761.98 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/256BNoSync-8             5179245               258.5 ns/op       990.28 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/1KNoSync-8               1714375               747.3 ns/op      1370.31 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/2KNoSync-8               1005616              1268 ns/op        1615.51 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/4KNoSync-8                488199              2372 ns/op        1727.12 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/8KNoSync-8                267494              6651 ns/op        1231.69 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/16KNoSync-8                79537             16994 ns/op         964.10 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Put/32KNoSync-8                85688             50266 ns/op         651.89 MB/s          16 B/op          1 allocs/op
BenchmarkDB_Get/128B-8                  16074962                74.27 ns/op     1723.52 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/256B-8                  14393212                83.70 ns/op     3058.62 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/512B-8                  10312220               120.6 ns/op      4245.71 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/1K-8                     5814404               205.2 ns/op      4991.29 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/2K-8                     3458854               346.5 ns/op      5911.35 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/4K-8                     1886884               636.3 ns/op      6436.78 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/8K-8                      969164              1234 ns/op        6640.20 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/16K-8                     491949              2397 ns/op        6833.90 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Get/32K-8                     253663              4746 ns/op        6904.80 MB/s           0 B/op          0 allocs/op
BenchmarkDB_Delete-8                     7586367               160.7 ns/op            16 B/op          1 allocs/op
PASS
ok      github.com/millken/archivedb    47.461s
```

## License

Source code is available under the Apache License 2.0 [License](/LICENSE).