package persist_test

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcelloh/fastdb/persist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	syncIime = 100
	dataDir  = "./../data"
)

func Test_OpenPersister_noData(t *testing.T) {
	path := "../data/fast_nodata.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	defer func() {
		err = aof.Close()
		assert.Nil(t, err)
	}()
}

func Test_OpenPersister_withData(t *testing.T) {
	path := "../data/fast_persister.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\ntext_1\nvalue for key 1\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	lines = "set\ntext_2\nvalue for key 2\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	lines = "del\ntext_2\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	err = aof.Close()
	require.Nil(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)
	assert.Equal(t, 1, len(keys))

	defer func() {
		err = aof.Close()
		assert.Nil(t, err)
	}()
}

func Test_OpenPersister_withWeirdData(t *testing.T) {
	path := "../data/fast_persister_weird.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\nmyBucket_1\nvalue for key 1\nwith enter\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	lines = "set\nmyBucket_2\nvalue for key 2\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	err = aof.Close()
	require.Nil(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(path, 0)
	assert.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)
	assert.Equal(t, 1, len(keys))

	defer func() {
		err = aof.Close()
		assert.Nil(t, err)
	}()
}

func Test_OpenPersister_writeError(t *testing.T) {
	path := "../data/fast_persister_write_error.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	err = aof.Close()
	require.Nil(t, err)

	lines := "set\ntext_1\na value\n"
	err = aof.Write(lines)
	assert.NotNil(t, err)
}

func Test_OpenPersister_withNoUnderscoredKey(t *testing.T) {
	path := "../data/fast_persister_wrong_key1.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\ntextone\na value\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	err = aof.Close()
	require.Nil(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(path, 0)
	assert.NotNil(t, err)
	assert.Nil(t, aof)
	assert.Nil(t, keys)
}

func Test_OpenPersister_withNoNumericKey(t *testing.T) {
	path := "../data/fast_persister_wrong_key.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\nwrong_key\na value\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	err = aof.Close()
	require.Nil(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(path, 0)
	assert.NotNil(t, err)
	assert.Nil(t, aof)
	assert.Nil(t, keys)
}

func Test_OpenPersister_withWrongInstruction(t *testing.T) {
	path := "../data/fast_persister_wrong_instruction.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "wrong\ntext_1\na value\n"
	err = aof.Write(lines)
	require.Nil(t, err)

	err = aof.Close()
	require.Nil(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(path, 0)
	assert.NotNil(t, err)
	assert.Nil(t, aof)
	assert.Nil(t, keys)

	defer func() {
		err = os.Remove(filePath)
		assert.Nil(t, err)
	}()
}

func Test_Defrag(t *testing.T) {
	path := "../data/fastdb_defrag100.db"
	filePath := filepath.Clean(path)

	defer func() {
		err := os.Remove(filePath)
		assert.Nil(t, err)

		_ = os.Remove(filePath + ".bak")
	}()

	total := 100

	aof, keys, err := persist.OpenPersister(path, syncIime)
	require.Nil(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	defer func() {
		err = aof.Close()
		assert.Nil(t, err)
	}()

	for i := 0; i < total; i++ {
		lines := "set\ntext_1\na value for key 1\n"
		err = aof.Write(lines)
		assert.Nil(t, err)
	}

	checkFileLines(t, filePath, total*3)

	keys["text"] = map[int][]byte{}
	keys["text"][1] = []byte("value for key 1")
	err = aof.Defrag(keys)
	assert.Nil(t, err)

	checkFileLines(t, filePath, 3)
}

func checkFileLines(t *testing.T, filePath string, checkCount int) {
	readFile, err := os.Open(filePath)
	assert.Nil(t, err)
	assert.NotNil(t, readFile)

	count := 0

	scanner := bufio.NewScanner(readFile)
	for scanner.Scan() {
		count++
	}

	err = readFile.Close()
	assert.Nil(t, err)
	assert.Equal(t, checkCount, count)
}
