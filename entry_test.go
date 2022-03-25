package archivedb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSafeEntry(t *testing.T) {
	require := require.New(t)
	k, v := []byte("foo"), []byte("bar")
	e := NewEntry(k, v)

	require.Equal(e.Key, k)
	require.Equal(e.Value, v)
	require.Equal(e.Size(), uint64(len(k)+len(v)+entryHeaderSize), "size mismatch")
	require.Equal(e.Header.ExpiresAt, uint64(0), "expiresAt mismatch")
	require.False(e.HasExpired())
	require.Equal(e.Header.ver, uint8(0))
	e = e.WithTTL(30 * time.Second)
	require.GreaterOrEqual(e.Header.ExpiresAt, uint64(time.Now().Unix()), "expiresAt mismatch")
	require.False(e.IsDeleted())
	e = e.addMeta(bitDelete)
	e = e.addMeta(bitDelete)
	require.True(e.IsDeleted())
	e = e.deleteMeta(bitDelete)
	e = e.deleteMeta(bitDelete)
	require.False(e.IsDeleted())

}
