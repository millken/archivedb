package archivedb

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	require := require.New(t)
	absPath, _ := filepath.Abs("./")
	testFile := path.Join(absPath, "index.test")
	defer os.Remove(testFile)
	idxOpts := &indexOptions{
		fileName: testFile,
	}
	idx, err := openIndex(idxOpts)
	require.NoError(err)
	require.Equal(idx.meta.totalKeys, uint64(0))
	require.Equal(idx.meta.activeKeys, uint64(0))

	idx.meta.activeKeys = 1
	idx.meta.totalKeys = 2

	require.NoError(idx.writeMeta())
	require.NoError(idx.readMeta())
	require.Equal(idx.meta.totalKeys, uint64(2))
	require.Equal(idx.meta.activeKeys, uint64(1))
}
