package mmap

import (
	"bytes"
	"io"
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
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	mmap, err := OpenFile(filename, Write|Read)
	if err != nil {
		t.Fatal(err)
	}

	//write small data
	if n, err := mmap.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal("invalid write")
	}
	//seek
	if _, err := mmap.Seek(5, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := mmap.Write([]byte("world")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal("invalid write")
	}
	b, err := mmap.ReadOff(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte("helloworld")) {
		t.Fatal("invalid data")
	}

	//write large data
	bigData := make([]byte, 1024)
	for i := range bigData {
		bigData[i] = byte(i)
	}
	if _, err := mmap.Write(bigData); err != io.ErrShortWrite {
		t.Fatal(err)
	}
	if err := mmap.Close(); err != nil {
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
	m, err := OpenFile(filename, Read)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	got, err := m.ReadOff(int(0), int(fi.Size()))
	if err != nil {
		t.Fatalf("ReadOff: %v", err)
	}
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
	got, err = m.ReadOff(int(0), int(fi.Size())+1)
	if err != ErrInvalidOffset {
		t.Fatalf("ReadOff: %v", err)
	}
	if got != nil {
		t.Fatalf("got %q, want nil", string(got))
	}
}
