package persist

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_OpenPersister(t *testing.T) {
	path := "../data/fast_persister_error.db"

	orgCreate := osCreate
	osCreate = os.O_RDONLY

	defer func() {
		osCreate = orgCreate
		filePath := filepath.Clean(path)
		_ = os.Remove(filePath)
	}()

	aof, keys, err := OpenPersister(path, 0)
	assert.NotNil(t, err)
	assert.Nil(t, keys)
	assert.Nil(t, aof)
}

func Test_OpenPersister_closeError(t *testing.T) {
	path := "../data/fast_persister_close_error.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	aof, keys, err := OpenPersister(path, 100)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	err = aof.file.Close()
	require.Nil(t, err)

	err = aof.Close()
	require.NotNil(t, err)
}
