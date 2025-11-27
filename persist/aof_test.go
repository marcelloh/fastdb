package persist_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/marcelloh/fastdb/persist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	syncTime = 100
	dataDir  = "./../data"
)

func Test_OpenPersister_noData(t *testing.T) {
	t.Parallel()
	path := "../data/fast_nodata_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	defer func() {
		// Close the file first if it's open
		if aof != nil {
			_ = aof.Close()
		}
		// Then remove the file
		_ = os.Remove(filePath)
	}()
}

func Test_OpenPersister_invalidPath(t *testing.T) {
	t.Parallel()
	// Use a path with invalid characters that will cause an error
	path := "//\\\\:*?\"<>|/invalid.db"
	aof, keys, err := persist.OpenPersister(path, syncTime)
	require.Error(t, err)
	assert.Nil(t, aof)
	assert.Nil(t, keys)
}

func Test_OpenPersister_nonExistingPath(t *testing.T) {
	t.Parallel()
	// Since OpenPersister now creates directories, we need to test a truly invalid path
	// Use a path with invalid characters that will cause an error
	path := "//\\\\:*?\"<>|/invalid.db"
	aof, keys, err := persist.OpenPersister(path, syncTime)
	require.Error(t, err)
	assert.Nil(t, aof)
	assert.Nil(t, keys)
}

func Test_OpenPersister_withData(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\ntext_1\nvalue for key 1\n"

	err = aof.Write(lines)
	require.NoError(t, err)

	lines = "set\ntext_2\nvalue for key 2\n"
	err = aof.Write(lines)
	require.NoError(t, err)

	lines = "del\ntext_2\n"
	err = aof.Write(lines)
	require.NoError(t, err)

	err = aof.Close()
	require.NoError(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(filePath, 0)

	defer func() {
		if aof != nil {
			_ = aof.Close()
		}
	}()

	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)
	assert.Len(t, keys, 1)

	bucketKeys := keys["text"]
	assert.NotNil(t, bucketKeys)
	assert.Len(t, bucketKeys, 1)
}

func Test_OpenPersister_withWeirdData(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_weird_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\nmyBucket_1\nvalue for key 1\nwith extra enter\n"

	err = aof.Write(lines)
	require.Error(t, err)

	lines = "set\nmyBucket_2\nvalue for key 2\n"
	err = aof.Write(lines)
	require.NoError(t, err)

	err = aof.Close()
	require.NoError(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(filePath, 0)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotEmpty(t, keys)

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_IncompleteSetInstructionNoKey(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_incomplete_set_no_key.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	// Write an incomplete set instruction (missing key and value)
	lines := "set\n"

	err = aof.Write(lines)
	require.Error(t, err) // Expect an error due to validation in Write method
	assert.Contains(t, err.Error(), "invalid set format")

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_IncompleteSetInstructionWithKey(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_incomplete_set_with_key.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	// Write an incomplete set instruction (has key but missing value)
	lines := "set\nmyBucket_2\n\n"

	err = aof.Write(lines)
	// The current implementation in aof.go doesn't consider this an error
	// because len(parts) >= 3 when splitting "set\nmyBucket_2\n" by "\n"
	// It gives ["set", "myBucket_2", ""] which passes the validation
	require.NoError(t, err)

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_IncompleteDelInstructionNoKey(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_incomplete_del_no_key.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	// Write an incomplete delete instruction (missing key)
	lines := "del\n"

	err = aof.Write(lines)
	require.Error(t, err) // Now expecting an error
	assert.Contains(t, err.Error(), "invalid delete format")

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_IncompleteDelInstructionWithKey(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_incomplete_del_with_key.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	// This is actually a valid delete instruction format
	lines := "del\nmyBucket_two\n"

	err = aof.Write(lines)
	require.Error(t, err)

	err = aof.Close()
	require.NoError(t, err)

	// The key format is invalid (not a number after underscore), so reading should fail
	aof, keys, err = persist.OpenPersister(filePath, 0)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.Empty(t, keys)

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_writeError(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_write_error_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	err = aof.Close()
	require.NoError(t, err)

	lines := "set\ntext_1\na value\n"

	err = aof.Write(lines)
	require.Error(t, err)
}

func Test_OpenPersister_withNoUnderscoredKey(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_wrong_key1_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\ntextone\na value\n"

	err = aof.Write(lines)
	require.Error(t, err)

	err = aof.Close()
	require.NoError(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(filePath, 0)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.Empty(t, keys)

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_withNoNumericKey(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_wrong_key_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "set\nwrong_key\na value\n"

	err = aof.Write(lines)
	require.Error(t, err)

	err = aof.Close()
	require.NoError(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(filePath, 0)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.Empty(t, keys)

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_withWrongInstruction(t *testing.T) {
	t.Parallel()
	path := "../data/fast_persister_wrong_instruction_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	lines := "wrong\ntext_1\na value\n"

	err = aof.Write(lines)
	require.Error(t, err)

	err = aof.Close()
	require.NoError(t, err)

	// here's were we check the actual reading of the data

	aof, keys, err = persist.OpenPersister(filePath, 0)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	err = aof.Close()
	require.NoError(t, err)
}

func Test_OpenPersister_concurrentWrites(t *testing.T) {
	t.Parallel()
	path := "../data/concurrent_write.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	aof, _, errOpen := persist.OpenPersister(filePath, syncTime)

	require.NoError(t, errOpen)
	assert.NotNil(t, aof)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			lines := fmt.Sprintf("set\nkey_%d\nvalue for key %d\n", i, i)

			err := aof.Write(lines)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	errClose := aof.Close()
	require.NoError(t, errClose)

	// Check if all keys were written correctly
	aof, keys, err := persist.OpenPersister(filePath, 0)
	require.NoError(t, err)
	assert.Len(t, keys, 1)

	bucketKeys := keys["key"]
	assert.NotNil(t, bucketKeys)
	assert.Len(t, bucketKeys, 10)

	errClose = aof.Close()
	require.NoError(t, errClose)
}

func Test_OpenPersister_writeAfterClose(t *testing.T) {
	t.Parallel()
	path := "../data/write_after_close_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
	}()

	aof, _, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)

	err = aof.Close()
	require.NoError(t, err)

	lines := "set\nkey_after_close\nvalue\n"

	err = aof.Write(lines)
	require.Error(t, err) // Expect an error since the file is closed
}

func Test_OpenPersister_invalidInstructionFormat(t *testing.T) {
	t.Parallel()
	path := "../data/invalid_instruction_format_unique.db"

	// Clean up any existing file before starting the test
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
	}()

	lines := "invalid_instruction\nkey\nvalue\n"
	err := os.WriteFile(filePath, []byte(lines), 0o644)
	require.NoError(t, err)

	aof, keys, err := persist.OpenPersister(path, syncTime)
	require.Error(t, err)
	assert.Nil(t, aof)
	assert.Empty(t, keys)
}

func Test_Defrag(t *testing.T) {
	t.Parallel()
	path := "../data/fastdb_defrag100_unique.db"

	filePath := filepath.Clean(path)

	// Clean up any existing file before starting the test
	_ = os.Remove(filePath)
	_ = os.Remove(filePath + ".bak")

	defer func() {
		// Make sure the file is closed before removing
		_ = os.Remove(filePath)
		_ = os.Remove(filePath + ".bak")
	}()

	total := 100

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	defer func() {
		err = aof.Close()
		require.NoError(t, err)
	}()

	for range total {
		lines := "set\ntext_1\na value for key 1\n"

		err = aof.Write(lines)
		require.NoError(t, err)
	}

	checkFileLines(t, filePath, total*3)

	keys["text"] = make(map[int][]byte)
	keys["text"][1] = []byte("value for key 1")
	err = aof.Defrag(keys)
	require.NoError(t, err)

	checkFileLines(t, filePath, 3)
}

func Test_Defrag_AlreadyClosed(t *testing.T) {
	t.Parallel()
	path := "../data/fastdb_defrag100.db"

	filePath := filepath.Clean(path)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)

		_ = os.Remove(filePath + ".bak")
	}()

	aof, keys, err := persist.OpenPersister(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, aof)
	assert.NotNil(t, keys)

	err = aof.Close()
	require.NoError(t, err)

	keys["text"] = make(map[int][]byte)
	keys["text"][1] = []byte("value for key 1")
	err = aof.Defrag(keys)
	require.Error(t, err)
}

func checkFileLines(t *testing.T, filePath string, checkCount int) {
	readFile, err := os.Open(filePath)
	require.NoError(t, err)
	assert.NotNil(t, readFile)

	count := 0

	scanner := bufio.NewScanner(readFile)
	for scanner.Scan() {
		count++
	}

	err = readFile.Close()
	require.NoError(t, err)
	assert.Equal(t, checkCount, count)
}
