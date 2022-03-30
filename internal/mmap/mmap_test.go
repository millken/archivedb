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

	m, err := OpenWithBufferSize(filename, 32)
	if err != nil {
		t.Fatal(err)
	}
	if m.Size() != 10 {
		t.Fatal("invalid length")
	}

	//write small data
	if n, err := m.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal("invalid write")
	} else if m.Size() != 10+5 {
		t.Fatal("invalid length")
	}
	b := make([]byte, 5)
	n, err := m.ReadAt(b, 10)
	if err != nil {
		t.Fatal(err)
	} else if n != 5 {
		t.Fatal("invalid read")
	} else if !bytes.Equal(b, []byte("hello")) {
		t.Fatal("invalid data")
	}

	//write large data
	bigData := make([]byte, 1024)
	for i := range bigData {
		bigData[i] = byte(i)
	}
	if n, err := m.Write(bigData); err != nil {
		t.Fatal(err)
	} else if n != 1024 {
		t.Fatal("invalid write")
	} else if m.Size() != 15+1024 {
		t.Fatal("invalid length")
	}

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	//read large data
	m, err = Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	if m.Size() != 15+1024 {
		t.Fatal("invalid length")
	}
	b = make([]byte, 1024)
	n, err = m.ReadAt(b, 15)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1024 {
		t.Fatal("invalid read")
	}
	if !bytes.Equal(b, bigData) {
		t.Fatal("invalid data")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen(t *testing.T) {
	const filename = "mmap_test.go"
	r, err := Open(filename)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := make([]byte, r.Size())
	if _, err := r.ReadAt(got, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
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
}
