package fastdb_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/marcelloh/fastdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	syncIime = 10
	dataDir  = "./data"
	memory   = ":memory:"
)

type someRecord struct {
	UUID string
	Text string
	ID   int
}

func Test_Open_File_noData(t *testing.T) {
	path := "data/fastdb_open_no_data.db"

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()
}

func Test_Open_Memory(t *testing.T) {
	path := memory

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()
}

func Test_SetGetDel_oneRecord(t *testing.T) {
	path := "data/fastdb_set.db"
	filePath := filepath.Clean(path)

	store, err := fastdb.Open(filePath, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)

		err = os.Remove(filePath)
		require.NoError(t, err)
	}()

	var newKey int

	newKey = store.GetNewIndex("texts")
	assert.Equal(t, 1, newKey)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	recordData, err := json.Marshal(record)
	require.NoError(t, err)

	err = store.Set("texts", record.ID, recordData)
	require.NoError(t, err)

	newKey = store.GetNewIndex("texts")
	assert.Equal(t, 2, newKey)

	info := store.Info()
	assert.Equal(t, "1 record(s) in 1 bucket(s)", info)

	memData, ok := store.Get("texts", 1)
	assert.True(t, ok)

	memRecord := &someRecord{}
	err = json.Unmarshal(memData, &memRecord)
	require.NoError(t, err)
	assert.NotNil(t, memRecord)
	assert.Equal(t, record.UUID, memRecord.UUID)
	assert.Equal(t, record.Text, memRecord.Text)
	assert.Equal(t, record.ID, memRecord.ID)

	// delete in non existing bucket
	ok, err = store.Del("notexisting", 1)
	require.NoError(t, err)
	assert.False(t, ok)

	// delete non existing key
	ok, err = store.Del("texts", 123)
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = store.Del("texts", 1)
	require.NoError(t, err)
	assert.True(t, ok)

	newKey = store.GetNewIndex("texts")
	assert.Equal(t, 1, newKey)

	info = store.Info()
	assert.Equal(t, "0 record(s) in 0 bucket(s)", info)
}

func Fuzz_SetGetDel_oneRecord(f *testing.F) {
	path := "data/fastdb_fuzzset.db"
	// path := ":memory:"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	store, err := fastdb.Open(filePath, 1000)
	require.NoError(f, err)
	assert.NotNil(f, store)

	defer func() {
		err = store.Close()
		require.NoError(f, err)

		err = os.Remove(filePath)
		require.NoError(f, err)
	}()

	var newKey int

	newKey = store.GetNewIndex("texts")
	assert.Equal(f, 1, newKey)

	testcases := []int{1, 2, 3, 4, 5}
	for _, tc := range testcases {
		f.Add(tc) // Use f.Add to provide a seed corpus
	}

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	f.Fuzz(func(t *testing.T, id int) {
		if id < 0 {
			return
		}

		record.ID = id

		recordData, err := json.Marshal(record)
		require.NoError(t, err)

		err = store.Set("texts", record.ID, recordData)
		require.NoError(t, err)

		newKey = store.GetNewIndex("texts")
		assert.Equal(t, id+1, newKey)

		info := store.Info()
		assert.Equal(t, "1 record(s) in 1 bucket(s)", info)

		memData, ok := store.Get("texts", id)
		assert.True(t, ok)

		memRecord := &someRecord{}
		err = json.Unmarshal(memData, &memRecord)
		require.NoError(t, err)

		ok, err = store.Del("texts", id)
		require.NoError(t, err)
		assert.True(t, ok)

		newKey = store.GetNewIndex("texts")
		assert.Equal(t, 1, newKey)

		info = store.Info()
		assert.Equal(t, "0 record(s) in 0 bucket(s)", info)
	})
}

func Test_Get_wrongRecord(t *testing.T) {
	path := memory

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()

	// store a record
	err = store.Set("bucket", 1, []byte("a text"))
	require.NoError(t, err)

	// get same record back
	memData, ok := store.Get("bucket", 1)
	assert.True(t, ok)
	assert.NotNil(t, memData)

	// get record back from wrong bucket
	memData, ok = store.Get("wrong_bucket", 1)
	assert.False(t, ok)
	assert.Nil(t, memData)

	// get record back from good bucket with wrong key
	memData, ok = store.Get("bucket", 2)
	assert.False(t, ok)
	assert.Nil(t, memData)
}

func Test_Defrag_1000lines(t *testing.T) {
	path := "data/fastdb_defrag1000.db"
	filePath := filepath.Clean(path)

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)

		err = os.Remove(filePath)
		require.NoError(t, err)

		_ = os.Remove(filePath + ".bak")
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	// create a lot of records first
	total := 1000

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	var recordData []byte

	for range total {
		record.ID = rdom.Intn(10) + 1
		recordData, err = json.Marshal(record)
		require.NoError(t, err)

		err = store.Set("records", record.ID, recordData)
		require.NoError(t, err)
	}

	checkFileLines(t, filePath, total*3)

	err = store.Defrag()
	require.NoError(t, err)

	checkFileLines(t, filePath, 30)
}

func Test_Defrag_1000000lines(t *testing.T) {
	path := "data/fastdb_defrag1000000.db"
	filePath := filepath.Clean(path)

	store, err := fastdb.Open(path, 250)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)

		err = os.Remove(filePath)
		require.NoError(t, err)

		_ = os.Remove(filePath + ".bak")
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	// create a lot of records first
	total := 1000000

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	var recordData []byte

	for range total {
		record.ID = rdom.Intn(10) + 1
		recordData, err = json.Marshal(record)
		require.NoError(t, err)

		err = store.Set("records", record.ID, recordData)
		require.NoError(t, err)
	}

	checkFileLines(t, filePath, total*3)

	err = store.Defrag()
	require.NoError(t, err)

	checkFileLines(t, filePath, 30)
}

func Test_GetAllFromMemory_1000(t *testing.T) {
	total := 1000
	path := memory

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	var recordData []byte

	for i := 1; i <= total; i++ {
		record.ID = i
		recordData, err = json.Marshal(record)
		require.NoError(t, err)

		err = store.Set("records", record.ID, recordData)
		require.NoError(t, err)
	}

	records, err := store.GetAll("records")
	require.NoError(t, err)
	assert.NotNil(t, records)
	assert.Len(t, records, total)

	records, err = store.GetAll("wrong_bucket")
	require.Error(t, err)
	assert.Nil(t, records)
}

func Test_GetAllFromFile_1000(t *testing.T) {
	total := 1000
	path := "data/fastdb_1000.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)

		err = os.Remove(filePath)
		require.NoError(t, err)
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	var recordData []byte

	for i := 1; i <= total; i++ {
		record.ID = rdom.Intn(1000000)
		recordData, err = json.Marshal(record)
		require.NoError(t, err)

		err = store.Set("user", record.ID, recordData)
		require.NoError(t, err)
	}

	records, err := store.GetAll("user")
	require.NoError(t, err)
	assert.NotNil(t, records)
}

func Test_GetAllSortedFromFile_10000(t *testing.T) {
	total := 10000
	path := "data/fastdb_1000.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	var recordData []byte

	for i := 1; i <= total; i++ {
		// while loop
		record.ID = rdom.Intn(1000000000)
		_, ok := store.Get("user", record.ID)

		for ok {
			record.ID = rdom.Intn(1000000000)
			_, ok = store.Get("user", record.ID)
		}

		recordData, err = json.Marshal(record)
		require.NoError(t, err)

		err = store.Set("user", record.ID, recordData)
		require.NoError(t, err)
	}

	records, err := store.GetAllSorted("user")
	require.NoError(t, err)
	assert.NotNil(t, records)
	assert.Len(t, records, total)
}

func Test_GetAllSortedFromMemory_10000(t *testing.T) {
	total := 10000
	path := memory

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	var recordData []byte

	for i := 1; i <= total; i++ {
		// while loop
		record.ID = rdom.Intn(1000000000)
		_, ok := store.Get("sortedRecords", record.ID)

		for ok {
			record.ID = rdom.Intn(1000000000)
			_, ok = store.Get("sortedRecords", record.ID)
		}

		recordData, err = json.Marshal(record)
		require.NoError(t, err)
		err = store.Set("sortedRecords", record.ID, recordData)
		require.NoError(t, err)
	}

	records, err := store.GetAllSorted("sortedRecords")
	require.NoError(t, err)
	assert.NotNil(t, records)
	assert.Len(t, records, total)

	records, err = store.GetAllSorted("wrong_bucket")
	require.Error(t, err)
	assert.Nil(t, records)
}

func Test_Set_error(t *testing.T) {
	path := "data/fastdb_set_error.db"
	filePath := filepath.Clean(path)

	store, err := fastdb.Open(filePath, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = os.Remove(filePath)
		require.NoError(t, err)
	}()

	err = store.Close()
	require.NoError(t, err)

	// store a record
	err = store.Set("bucket", 1, []byte("a text"))
	require.Error(t, err)
}

func Test_Set_wrongBucket(t *testing.T) {
	path := "data/fastdb_set_bucket_error.db"
	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	// store a record
	err = store.Set("under_score", 1, []byte("a text for key 1"))
	require.NoError(t, err)

	err = store.Set("under_score", 2, []byte("a text for key 2"))
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	store2, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store2)

	defer func() {
		err = store2.Close()
		require.NoError(t, err)
	}()
}

func TestConcurrentOperationsWithDelete(t *testing.T) {
	path := "testdb_concurrent_delete"
	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()

	const (
		numGoroutines = 100
		numOperations = 100
		bucket        = "test"
	)

	var wg sync.WaitGroup

	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()

			for j := range numOperations {
				key := id*numOperations + j
				value := []byte(fmt.Sprintf("value_%d_%d", id, j))

				// Set operation
				err := store.Set(bucket, key, value)
				assert.NoError(t, err)

				// Get operation
				retrievedValue, ok := store.Get(bucket, key)
				assert.True(t, ok)
				assert.Equal(t, value, retrievedValue)

				// Delete operation (delete every other entry)
				if j%2 == 0 {
					deleted, err := store.Del(bucket, key)
					assert.NoError(t, err)
					assert.True(t, deleted)

					// Verify deletion
					_, ok = store.Get(bucket, key)
					assert.False(t, ok)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	for i := range numGoroutines {
		for j := range numOperations {
			key := i*numOperations + j
			expectedValue := []byte(fmt.Sprintf("value_%d_%d", i, j))

			retrievedValue, ok := store.Get(bucket, key)
			if j%2 == 0 {
				// Even entries should have been deleted
				assert.False(t, ok)
			} else {
				// Odd entries should still exist
				assert.True(t, ok)
				assert.Equal(t, expectedValue, retrievedValue)
			}
		}
	}
}

func Benchmark_Get_File_1000(b *testing.B) {
	path := "data/bench-get.db"
	total := 1000

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(b, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	require.NoError(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	var recordData []byte

	for i := 1; i <= total; i++ {
		record.ID = rdom.Intn(1000000)
		recordData, err = json.Marshal(record)
		require.NoError(b, err)

		err = store.Set("bench_bucket", record.ID, recordData)
		require.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		_, _ = store.Get("bench_bucket", rand.Intn(1000000))
	}

	err = store.Close()
	require.NoError(b, err)
}

func Benchmark_Get_Memory_1000(b *testing.B) {
	path := memory
	total := 1000

	store, err := fastdb.Open(path, syncIime)
	require.NoError(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	var recordData []byte

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	for i := 1; i <= total; i++ {
		record.ID = rdom.Intn(1000000)
		recordData, err = json.Marshal(record)
		require.NoError(b, err)

		err = store.Set("bench_bucket", record.ID, recordData)
		require.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		_, _ = store.Get("bench_bucket", rand.Intn(1000000))
	}

	err = store.Close()
	require.NoError(b, err)
}

func Benchmark_Set_File_NoSyncTime(b *testing.B) {
	path := "data/bench-set.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(b, err)
	}()

	store, err := fastdb.Open(path, 0)
	require.NoError(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	var recordData []byte

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		record.ID = rand.Intn(1000000)
		recordData, err = json.Marshal(record)
		require.NoError(b, err)

		err = store.Set("user", record.ID, recordData)
		require.NoError(b, err)
	}

	err = store.Close()
	require.NoError(b, err)
}

func Benchmark_Set_File_WithSyncTime(b *testing.B) {
	path := "data/bench-set.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(b, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	require.NoError(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	var recordData []byte

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		record.ID = rand.Intn(1000000)
		recordData, err = json.Marshal(record)
		require.NoError(b, err)

		err = store.Set("user", record.ID, recordData)
		require.NoError(b, err)
	}

	err = store.Close()
	require.NoError(b, err)
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

func Benchmark_Set_Memory(b *testing.B) {
	path := memory

	store, err := fastdb.Open(path, syncIime)
	require.NoError(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	var recordData []byte

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		record.ID = rand.Intn(1000000)
		recordData, err = json.Marshal(record)
		require.NoError(b, err)

		err = store.Set("user", record.ID, recordData)
		require.NoError(b, err)
	}

	err = store.Close()
	require.NoError(b, err)
}
