package mmap

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestMmap(t *testing.T) {
	const filename = "mmap.test"
	defer os.Remove(filename)
	if f, err := os.Create(filename); err != nil {
		t.Fatal(err)
	} else if err = f.Truncate(10); err != nil {
		t.Fatal(err)
	} else if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	mmap, err := Map(int(f.Fd()), 10)
	if err != nil {
		t.Fatal(err)
	}

	//write small data
	if n, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal("invalid write")
	}
	b := mmap.Read(0, 5)
	if !bytes.Equal(b, []byte("hello")) {
		t.Fatal("invalid data")
	}

	//write large data
	bigData := make([]byte, 1024)
	for i := range bigData {
		bigData[i] = byte(i)
	}
	if n, err := f.Write(bigData); err != nil {
		t.Fatal(err)
	} else if n != 1024 {
		t.Fatal("invalid write")
	}

	if err := mmap.Close(); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	const filename = "mmap_test.go"
	r, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	fi, _ := r.Stat()
	m, err := Map(int(r.Fd()), int(fi.Size()))
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	got := m.Read(int(0), int(fi.Size()))
	want, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("ioutil.ReadFile: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("\ngot  %q\nwant %q", string(got), string(want))
	}
}
