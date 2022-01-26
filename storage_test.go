package archivedb

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	require := require.New(t)
	absPath, _ := filepath.Abs("./")
	testFile := path.Join(absPath, "storage001.test")
	defer os.Remove(testFile)
	storage, err := initializeStorage(testFile)
	require.NoError(err)
	require.NotNil(storage)
	m, err := storage.readBlockMeta(32)
	require.NoError(err)
	require.Equal(uint32(32), m.keys)
}

type a struct {
	a uint
}

func TestAAA(t *testing.T) {
	aa := struct {
		b []*a
	}{}
	aa.b = append(aa.b, &a{3})
	t.Logf("%v", aa.b)
	aa.b = nil
	t.Logf("%v", aa.b)
}
