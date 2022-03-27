package archivedb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafeEntry(t *testing.T) {
	require := require.New(t)
	k, v := []byte("foo"), []byte("bar")
	e := createEntry(EntryInsertFlag, k, v)

	require.Equal(e.key, k)
	require.Equal(e.value, v)
	require.Equal(e.Size(), uint32(len(k)+len(v)+EntryHeaderSize), "size mismatch")

}
