package radixtree

//https://github.com/gammazero/radixtree/blob/master/tree_test.go
import (
	"encoding/binary"
	"testing"
)

func BenchmarkTree(b *testing.B) {
	tree := New[int]()
	key := make([]byte, 16)
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		tree.Put(key, i)

		n, _ := tree.Get(key)

		if n != i {
			b.Fatalf("unexpected value: %v", n)
		}
	}

}
