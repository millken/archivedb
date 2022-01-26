package archivedb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB(t *testing.T) {
	require := require.New(t)
	db, err := Open("./test001.db")
	require.NoError(err)
	require.NotNil(db)
	defer db.Close()
	tests := []struct {
		k []byte
		v []byte
	}{
		{[]byte("a1"), []byte("a1")},
		{[]byte("a2"), []byte("a2")},
		{[]byte("a3"), []byte("a3")},
	}
	for _, test := range tests {
		db.Set(test.k, test.v)
	}
}

func TestDB2(t *testing.T) {
	require := require.New(t)
	db, err := Open("./test001.db")
	require.NoError(err)
	require.NotNil(db)
	defer db.Close()
	r, err := db.Get([]byte("a1"))
	require.NoError(err)
	require.Equal(r, []byte("a1"))
}
func TestA(t *testing.T) {
	require := require.New(t)
	require.Equal(1, 40<<20)
}

func BenchmarkDBGet(b *testing.B) {
	require := require.New(b)
	db, err := Open("./test001.db")
	require.NoError(err)
	require.NotNil(db)
	defer db.Close()
	for i := 0; i < b.N; i++ {
		db.Get([]byte("a1"))
	}
}
