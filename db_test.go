package archivedb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		err = db.Set(test.key, test.value)
		require.NoError(err)
	}
	for _, test := range tests {
		v, err := db.Get(test.key)
		require.NoError(err)
		require.Equal(test.value, v)
	}

	//test key not exist
	v, err := db.Get([]byte("not_exist"))
	require.ErrorIs(err, ErrNotFound)
	require.Nil(v)
	require.NoError(db.Close())
}
