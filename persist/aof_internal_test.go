package persist

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_OpenPersister(t *testing.T) {
	path := "../data/fast_persister_error.db"

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	orgCreate := osCreate

	osCreate = os.O_RDONLY

	defer func() {
		osCreate = orgCreate

		filePath := filepath.Clean(path)

		_ = os.Remove(filePath)
	}()

	aof, keys, err := OpenPersister(path, 0)
	require.Error(t, err)
	assert.Nil(t, keys)
	assert.Nil(t, aof)
}

func Test_OpenPersister_closeError(t *testing.T) {
	path := "../data/fast_persister_close_error.db"

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	aof, keys, err := OpenPersister(path, 100)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	err = aof.file.Close()
	require.NoError(t, err)

	err = aof.Close()
	require.Error(t, err)
}
