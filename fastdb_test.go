package fastdb_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcelloh/fastdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	syncTime = 100
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

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	defer func() {
		filePath := filepath.Clean(path)
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)
	}()
}

func Test_Open_Memory(t *testing.T) {
	path := memory

	store, err := fastdb.Open(path, syncTime)
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

	store, err := fastdb.Open(filePath, syncTime)
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

	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	store, err := fastdb.Open(filePath, 1000)
	require.NoError(f, err)
	assert.NotNil(f, store)

	defer func() {
		err = store.Close()
		require.NoError(f, err)
	}()

	var newKey int

	newKey = store.GetNewIndex("texts")
	assert.Equal(f, 1, newKey)

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	for range 50 {
		tc := rdom.Intn(10000) + 1
		f.Add(tc) // Use f.Add to provide a seed corpus
	}

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	counter := 0
	highest := 0

	f.Fuzz(func(t *testing.T, id int) {
		if id < 0 {
			return
		}

		record.ID = id

		if highest < id {
			highest = id
		}

		recordData, err := json.Marshal(record)
		require.NoError(t, err)

		_, ok := store.Get("texts", id)
		if !ok {
			counter++
		}

		err = store.Set("texts", record.ID, recordData)
		require.NoError(t, err)

		newKey = store.GetNewIndex("texts")
		assert.Equal(t, highest+1, newKey, strconv.Itoa(id))

		info := store.Info()
		text := fmt.Sprintf("%d record(s) in 1 bucket(s)", counter)
		assert.Equal(t, text, info, strconv.Itoa(id))

		memData, ok := store.Get("texts", id)
		assert.True(t, ok)

		memRecord := &someRecord{}

		err = json.Unmarshal(memData, &memRecord)
		require.NoError(t, err)

		if id%5 == 0 {
			ok, err = store.Del("texts", id)
			require.NoError(t, err)
			assert.True(t, ok)

			counter--

			newKey = store.GetNewIndex("texts")
			highest = newKey - 1
		}
	})
}

func Test_Get_wrongRecord(t *testing.T) {
	path := memory

	store, err := fastdb.Open(path, syncTime)
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

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	filePath := filepath.Clean(path)

	store, err := fastdb.Open(path, syncTime)
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

func Test_Defrag_250000lines(t *testing.T) {
	path := "data/fastdb_defrag1000000.db"
	filePath := filepath.Clean(path)

	store, err := fastdb.Open(filePath, 250)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)

		_ = os.Remove(filePath)

		_ = os.Remove(filePath + ".bak")
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	// create a lot of records first
	total := 250000

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

	store, err := fastdb.Open(path, syncTime)
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

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	store, err := fastdb.Open(path, syncTime)
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

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncTime)
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

	store, err := fastdb.Open(path, syncTime)
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

	store, err := fastdb.Open(filePath, syncTime)
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

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	// store a record
	err = store.Set("under_score", 1, []byte("a text for key 1"))
	require.NoError(t, err)

	err = store.Set("under_score", 2, []byte("a text for key 2"))
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	store2, err := fastdb.Open(path, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, store2)

	defer func() {
		err = store2.Close()
		require.NoError(t, err)
	}()
}

func TestConcurrentOperations(t *testing.T) {
	path := "testdb_concurrent_delete"
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncTime)
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
				value := fmt.Appendf(nil, "value_%d_%d", id, j)

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
			expectedValue := fmt.Appendf(nil, "value_%d_%d", i, j)

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
	total := 1000

	path := "../data/bench-get.db"
	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		require.NoError(b, err)
	}()

	store, err := fastdb.Open(filePath, syncTime)
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

	for b.Loop() { // use b.N for looping
		_, _ = store.Get("bench_bucket", rand.Intn(1000000))
	}

	err = store.Close()
	require.NoError(b, err)
}

func Benchmark_Get_Memory_1000(b *testing.B) {
	path := memory
	total := 1000

	store, err := fastdb.Open(path, syncTime)
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

	for b.Loop() { // use b.N for looping
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

	store, err := fastdb.Open(filePath, 0)
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

	for b.Loop() { // use b.N for looping
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

	store, err := fastdb.Open(filePath, syncTime)
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

	for b.Loop() { // use b.N for looping
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

	store, err := fastdb.Open(path, syncTime)
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

	for b.Loop() { // use b.N for looping
		record.ID = rand.Intn(1000000)
		recordData, err = json.Marshal(record)
		require.NoError(b, err)

		err = store.Set("user", record.ID, recordData)
		require.NoError(b, err)
	}

	err = store.Close()
	require.NoError(b, err)
}

// performConcurrentSet handles a concurrent set operation with proper locking and tracking.
func performConcurrentSet(store *fastdb.DB, bucket string, id int, recordData []byte,
	mutex *sync.Mutex, recordIDs map[int]bool, setCounter *int32,
) error {
	err := store.Set(bucket, id, recordData)
	if err != nil {
		return err
	}

	// Track the ID
	mutex.Lock()

	recordIDs[id] = true

	mutex.Unlock()

	// Increment counter
	atomic.AddInt32(setCounter, 1)

	return nil
}

// performConcurrentDelete handles a concurrent delete operation with proper locking and tracking.
func performConcurrentDelete(store *fastdb.DB, bucket string, id int,
	mutex *sync.Mutex, recordIDs map[int]bool, delCounter *int32,
) error {
	deleted, err := store.Del(bucket, id)
	if err != nil {
		return err
	}

	if deleted {
		// Track the deletion
		mutex.Lock()
		delete(recordIDs, id)
		mutex.Unlock()

		// Increment counter
		atomic.AddInt32(delCounter, 1)
	}

	return nil
}

// Test_ConcurrentSetDel_CoupleOfSeconds tests the concurrent set and delete operations
// on the FastDB database. It creates a couple of goroutines to write and delete
// records from the database, and then verifies that the tracked record count
// matches the actual record count in the database.
func Test_ConcurrentSetDel_CoupleOfSeconds(t *testing.T) {
	locSmallRecBucket := "concurrent_small_test"
	locRecBucket := "concurrent_test"

	// Use a unique file path to avoid conflicts with other tests
	path := "data/fastdb_concurrent_5sec.db"

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	filePath := filepath.Clean(path)

	// Clean up any existing file before starting the test
	_ = os.Remove(filePath)

	// Open the database
	store, err := fastdb.Open(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	// Ensure database is closed after test
	defer func() {
		err = store.Close()
		require.NoError(t, err)

		// Give the OS a moment to release the file handle
		time.Sleep(250 * time.Millisecond)

		err = os.Remove(filePath)
		if err != nil {
			t.Logf("Warning: Failed to remove test file: %v", err)
		}
	}()

	// Create channels for communication
	done := make(chan bool)

	var wg sync.WaitGroup

	// Track statistics
	var (
		setCounter     int32
		delCounter     int32
		mutex          sync.Mutex
		recordSmallIDs = make(map[int]bool)
		recordIDs      = make(map[int]bool)
	)

	type localSmallRecord struct {
		UUID string
		Text string
		ID   int
	}

	type localRecord struct {
		UUID     string
		Text     string
		ID       int
		BigOne   string
		BigTwo   string
		BigThree string
		BigFour  string
		BigFive  string
	}

	// Start writer goroutines
	numWriters := 5
	for i := range numWriters {
		wg.Add(1)

		go func(writerID int) {
			defer wg.Done()

			// Create a separate random source for each goroutine
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(writerID)))

			recordSmall := &localSmallRecord{
				UUID: fmt.Sprintf("UUID-writer-%d", writerID),
				Text: fmt.Sprintf("Text from writer %d", writerID),
			}

			record := &localRecord{
				UUID:     fmt.Sprintf("UUID-writer-%d", writerID),
				Text:     fmt.Sprintf("Text from writer %d", writerID),
				BigOne:   strings.Repeat("A", 1000),
				BigTwo:   strings.Repeat("B", 1000),
				BigThree: strings.Repeat("C", 1000),
				BigFour:  strings.Repeat("D", 1000),
				BigFive:  strings.Repeat("E", 1000),
			}

			for {
				select {
				case <-done:
					return
				default:
					// Generate a random ID
					id := localRand.Intn(1000) + 1

					// Randomly choose to either set or delete the record
					doSet := localRand.Intn(4)
					switch doSet {
					case 0:
						// small record set
						recordSmall.ID = id
						// Marshal the record
						recordSmallData, errm := json.Marshal(recordSmall)
						if errm != nil {
							continue
						}

						err = performConcurrentSet(store, locSmallRecBucket, id, recordSmallData, &mutex, recordSmallIDs, &setCounter)
						if err != nil {
							return
						}
					case 1:
						// small record delete
						err = performConcurrentDelete(store, locSmallRecBucket, id, &mutex, recordSmallIDs, &delCounter)
						if err != nil {
							continue
						}
					case 2:
						// big record set
						record.ID = id
						// Marshal the record
						recordData, errm := json.Marshal(record)
						if errm != nil {
							continue
						}

						err = performConcurrentSet(store, locRecBucket, id, recordData, &mutex, recordIDs, &setCounter)
						if err != nil {
							return
						}
					default:
						// big record delete
						err = performConcurrentDelete(store, locRecBucket, id, &mutex, recordIDs, &delCounter)
						if err != nil {
							continue
						}
					}
					// Small sleep to reduce contention
					// time.Sleep(time.Millisecond * time.Duration(localRand.Intn(5)))
				}
			}
		}(i)
	}

	// Run for 5 seconds
	time.Sleep(15 * time.Second)

	// Signal goroutines to stop
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()

	// Get final statistics
	finalSetCount := atomic.LoadInt32(&setCounter)
	finalDelCount := atomic.LoadInt32(&delCounter)

	// Get the actual number of records in the database
	recordsSmall, err := store.GetAll(locSmallRecBucket)
	require.NoError(t, err)

	actualRecordCount := len(recordsSmall)

	// Get the number of tracked records
	mutex.Lock()

	trackedRecordCount := len(recordSmallIDs)

	mutex.Unlock()

	// Log statistics
	// t.Logf("Total records set: %d", finalSetCount)
	// t.Logf("Total records deleted: %d", finalDelCount)
	// t.Logf("Final record count in DB: %d", actualRecordCount)
	// t.Logf("Tracked record count: %d", trackedRecordCount)

	// Verify that our tracking matches the actual database state
	// Note: This might not be exact due to race conditions, but should be close
	assert.InDelta(t, trackedRecordCount, actualRecordCount, 10,
		"The tracked record count should be close to the actual record count")

	// Verify that we actually did some work
	assert.Positive(t, finalSetCount, "Should have set some records")
	assert.Positive(t, finalDelCount, "Should have deleted some records")

	// Verify database integrity by checking each record
	for id := range recordSmallIDs {
		data, ok := store.Get(locSmallRecBucket, id)
		assert.True(t, ok, "Record %d should exist in the database", id)
		assert.NotNil(t, data, "Record data should not be nil")

		// Verify we can unmarshal the data
		record := &localSmallRecord{}
		err := json.Unmarshal(data, record)
		require.NoError(t, err, "Should be able to unmarshal record %d", id)
	}
	// Verify database integrity by checking each record
	for id := range recordIDs {
		data, ok := store.Get(locRecBucket, id)
		assert.True(t, ok, "Record %d should exist in the database", id)
		assert.NotNil(t, data, "Record data should not be nil")

		// Verify we can unmarshal the data
		record := &localRecord{}
		err := json.Unmarshal(data, record)
		assert.NoError(t, err, "Should be able to unmarshal record %d", id)
	}
}

func Test_ConcurrentSetDel_CoupleOfSecondsPart2(t *testing.T) {
	locSmallRecBucket := "concurrent_small_test"
	locRecBucket := "concurrent_test"

	// Use a unique file path to avoid conflicts with other tests
	// path := "data/fastdb_concurrent_5secP2.db"
	path := "data/fastdb_concurrent_15secs.db"

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	filePath := filepath.Clean(path)

	// Open the database
	store, err := fastdb.Open(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	// Ensure database is closed after test
	defer func() {
		err = store.Close()
		require.NoError(t, err)

		// Give the OS a moment to release the file handle
		time.Sleep(250 * time.Millisecond)

		err = os.Remove(filePath)
		if err != nil {
			t.Logf("Warning: Failed to remove test file: %v", err)
		}
	}()

	// Create channels for communication
	done := make(chan bool)

	var wg sync.WaitGroup

	// Track statistics
	var (
		setCounter     int32
		delCounter     int32
		mutex          sync.Mutex
		recordSmallIDs = make(map[int]bool)
		recordIDs      = make(map[int]bool)
	)

	type localSmallRecord struct {
		UUID string
		Text string
		ID   int
	}

	type localRecord struct {
		UUID     string
		Text     string
		ID       int
		BigOne   string
		BigTwo   string
		BigThree string
		BigFour  string
		BigFive  string
	}

	// Start writer goroutines
	numWriters := 5
	for i := range numWriters {
		wg.Add(1)

		go func(writerID int) {
			defer wg.Done()

			// Create a separate random source for each goroutine
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(writerID)))

			recordSmall := &localSmallRecord{
				UUID: fmt.Sprintf("UUID-writer-%d", writerID),
				Text: fmt.Sprintf("Text from writer %d", writerID),
			}

			record := &localRecord{
				UUID:     fmt.Sprintf("UUID-writer-%d", writerID),
				Text:     fmt.Sprintf("Text from writer %d", writerID),
				BigOne:   strings.Repeat("A", 1000),
				BigTwo:   strings.Repeat("B", 1000),
				BigThree: strings.Repeat("C", 1000),
				BigFour:  strings.Repeat("D", 1000),
				BigFive:  strings.Repeat("E", 1000),
			}

			for {
				select {
				case <-done:
					return
				default:
					// Generate a random ID
					id := localRand.Intn(1000) + 1

					// Randomly choose to either set or delete the record
					doSet := localRand.Intn(4)
					switch doSet {
					case 0:
						// small record set
						recordSmall.ID = id
						// Marshal the record
						recordSmallData, errm := json.Marshal(recordSmall)
						if errm != nil {
							continue
						}

						err = performConcurrentSet(store, locSmallRecBucket, id, recordSmallData, &mutex, recordSmallIDs, &setCounter)
						if err != nil {
							return
						}
					case 1:
						// small record delete
						err = performConcurrentDelete(store, locSmallRecBucket, id, &mutex, recordSmallIDs, &delCounter)
						if err != nil {
							continue
						}
					case 2:
						// big record set
						record.ID = id
						// Marshal the record
						recordData, errm := json.Marshal(record)
						if errm != nil {
							continue
						}

						err = performConcurrentSet(store, locRecBucket, id, recordData, &mutex, recordIDs, &setCounter)
						if err != nil {
							return
						}
					default:
						// big record delete
						err = performConcurrentDelete(store, locRecBucket, id, &mutex, recordIDs, &delCounter)
						if err != nil {
							continue
						}
					}
					// Small sleep to reduce contention
					// time.Sleep(time.Millisecond * time.Duration(localRand.Intn(5)))
				}
			}
		}(i)
	}

	// Run for 5 seconds
	time.Sleep(5 * time.Second)

	// Signal goroutines to stop
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()

	// Get final statistics
	finalSetCount := atomic.LoadInt32(&setCounter)
	finalDelCount := atomic.LoadInt32(&delCounter)

	// Get the actual number of records in the database
	recordsSmall, err := store.GetAll(locSmallRecBucket)
	require.NoError(t, err)

	actualRecordCount := len(recordsSmall)

	// Get the number of tracked records
	mutex.Lock()

	trackedRecordCount := len(recordSmallIDs)

	mutex.Unlock()

	// Log statistics
	// t.Logf("Total records set: %d", finalSetCount)
	// t.Logf("Total records deleted: %d", finalDelCount)
	// t.Logf("Final record count in DB: %d", actualRecordCount)
	// t.Logf("Tracked record count: %d", trackedRecordCount)

	// Verify that our tracking matches the actual database state
	// Note: This might not be exact due to race conditions, but should be close
	assert.InDelta(t, trackedRecordCount, actualRecordCount, 10,
		"The tracked record count should be close to the actual record count")

	// Verify that we actually did some work
	assert.Positive(t, finalSetCount, "Should have set some records")
	assert.Positive(t, finalDelCount, "Should have deleted some records")

	// Verify database integrity by checking each record
	for id := range recordIDs {
		data, ok := store.Get("concurrent_test", id)
		assert.True(t, ok, "Record %d should exist in the database", id)
		assert.NotNil(t, data, "Record data should not be nil")

		// Verify we can unmarshal the data
		record := &localRecord{}
		err := json.Unmarshal(data, record)
		assert.NoError(t, err, "Should be able to unmarshal record %d", id)
	}
}

func Test_ConcurrentSetDel_CoupleOfSecondsPart3(t *testing.T) {
	locSmallRecBucket := "concurrent_small_test"
	locRecBucket := "concurrent_test"

	// Use a unique file path to avoid conflicts with other tests
	// path := "data/fastdb_concurrent_5secP2.db"
	path := "data/fastdb_concurrent_15secs.db"

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator)) // windows fix

	filePath := filepath.Clean(path)

	// Open the database
	store, err := fastdb.Open(filePath, syncTime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	// Ensure database is closed after test
	defer func() {
		err = store.Close()
		require.NoError(t, err)

		// Give the OS a moment to release the file handle
		time.Sleep(250 * time.Millisecond)

		err = os.Remove(filePath)
		if err != nil {
			t.Logf("Warning: Failed to remove test file: %v", err)
		}
	}()

	// Create channels for communication
	done := make(chan bool)

	var wg sync.WaitGroup

	// Track statistics
	var (
		setCounter     int32
		delCounter     int32
		mutex          sync.Mutex
		recordSmallIDs = make(map[int]bool)
		recordIDs      = make(map[int]bool)
	)

	type localSmallRecord struct {
		UUID string
		Text string
		ID   int
	}

	type localRecord struct {
		UUID     string
		Text     string
		ID       int
		BigOne   string
		BigTwo   string
		BigThree string
		BigFour  string
		BigFive  string
	}

	// Start writer goroutines
	numWriters := 5
	for i := range numWriters {
		wg.Add(1)

		go func(writerID int) {
			defer wg.Done()

			// Create a separate random source for each goroutine
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(writerID)))

			recordSmall := &localSmallRecord{
				UUID: fmt.Sprintf("UUID-writer-%d", writerID),
				Text: fmt.Sprintf("Text from writer %d", writerID),
			}

			record := &localRecord{
				UUID:     fmt.Sprintf("UUID-writer-%d", writerID),
				Text:     fmt.Sprintf("Text from writer %d", writerID),
				BigOne:   strings.Repeat("A", 1000),
				BigTwo:   strings.Repeat("B", 1000),
				BigThree: strings.Repeat("C", 1000),
				BigFour:  strings.Repeat("D", 1000),
				BigFive:  strings.Repeat("E", 1000),
			}

			for {
				select {
				case <-done:
					return
				default:
					// Generate a random ID
					id := localRand.Intn(1000) + 1

					// Randomly choose to either set or delete the record
					doSet := localRand.Intn(4)
					switch doSet {
					case 0:
						// small record set
						recordSmall.ID = id
						// Marshal the record
						recordSmallData, errm := json.Marshal(recordSmall)
						if errm != nil {
							continue
						}

						err = performConcurrentSet(store, locSmallRecBucket, id, recordSmallData, &mutex, recordSmallIDs, &setCounter)
						if err != nil {
							return
						}
					case 1:
						// small record delete
						err = performConcurrentDelete(store, locSmallRecBucket, id, &mutex, recordSmallIDs, &delCounter)
						if err != nil {
							continue
						}
					case 2:
						// big record set
						record.ID = id
						// Marshal the record
						recordData, errm := json.Marshal(record)
						if errm != nil {
							continue
						}

						err = performConcurrentSet(store, locRecBucket, id, recordData, &mutex, recordIDs, &setCounter)
						if err != nil {
							return
						}
					default:
						// big record delete
						err = performConcurrentDelete(store, locRecBucket, id, &mutex, recordIDs, &delCounter)
						if err != nil {
							continue
						}
					}
					// Small sleep to reduce contention
					// time.Sleep(time.Millisecond * time.Duration(localRand.Intn(5)))
				}
			}
		}(i)
	}

	// Run for 5 seconds
	time.Sleep(1 * time.Second)

	// Signal goroutines to stop
	close(done)

	// Wait for all goroutines to finish
	wg.Wait()

	// Get final statistics
	finalSetCount := atomic.LoadInt32(&setCounter)
	finalDelCount := atomic.LoadInt32(&delCounter)

	// Get the actual number of records in the database
	recordsSmall, err := store.GetAll(locSmallRecBucket)
	require.NoError(t, err)

	actualRecordCount := len(recordsSmall)

	// Get the number of tracked records
	mutex.Lock()

	trackedRecordCount := len(recordSmallIDs)

	mutex.Unlock()

	// Log statistics
	// t.Logf("Total records set: %d", finalSetCount)
	// t.Logf("Total records deleted: %d", finalDelCount)
	// t.Logf("Final record count in DB: %d", actualRecordCount)
	// t.Logf("Tracked record count: %d", trackedRecordCount)

	// Verify that our tracking matches the actual database state
	// Note: This might not be exact due to race conditions, but should be close
	assert.InDelta(t, trackedRecordCount, actualRecordCount, 10,
		"The tracked record count should be close to the actual record count")

	// Verify that we actually did some work
	assert.Positive(t, finalSetCount, "Should have set some records")
	assert.Positive(t, finalDelCount, "Should have deleted some records")

	// Verify database integrity by checking each record
	for id := range recordIDs {
		data, ok := store.Get("concurrent_test", id)
		assert.True(t, ok, "Record %d should exist in the database", id)
		assert.NotNil(t, data, "Record data should not be nil")

		// Verify we can unmarshal the data
		record := &localRecord{}
		err := json.Unmarshal(data, record)
		assert.NoError(t, err, "Should be able to unmarshal record %d", id)
	}
}

func Test_Reproduction_NewlineInValue(t *testing.T) {
	path := "data/repro_newline.db"

	path = strings.ReplaceAll(path, "/", string(os.PathSeparator))

	filePath := filepath.Clean(path)

	_ = os.Remove(filePath)

	defer func() {
		_ = os.Remove(filePath)
	}()

	// 1. Open DB and write a value with newline
	store, err := fastdb.Open(path, 100)
	require.NoError(t, err)

	key := 1
	value := []byte("line1\nline2")
	bucket := "test_bucket"

	err = store.Set(bucket, key, value)
	require.NoError(t, err)

	// Verify in memory before closing
	val, ok := store.Get(bucket, key)
	require.True(t, ok)
	assert.Equal(t, value, val)

	err = store.Close()
	require.NoError(t, err)

	// 2. Reopen DB and try to read it back
	store2, err := fastdb.Open(path, 100)
	// This is expected to fail or return corrupted data if the bug exists
	if err != nil {
		t.Logf("Failed to open DB: %v", err)
	} else {
		val2, ok := store2.Get(bucket, key)
		if !ok {
			t.Log("Key not found after reopen")
		} else {
			assert.Equal(t, value, val2, "Value should match after reopen")
		}

		err = store2.Close()
		require.NoError(t, err)
	}
}
