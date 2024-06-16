package fastdb_test

import (
	"bufio"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcelloh/fastdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	syncIime = 100
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

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		require.NoError(t, err)

		filePath := filepath.Clean(path)
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

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)

		_ = os.Remove(filePath + ".bak")
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

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)

		_ = os.Remove(filePath + ".bak")
	}()

	store, err := fastdb.Open(path, 250)
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
		if err != nil {
			log.Fatal(err)
		}
	}()

	defer func() {
		err = store.Close()
		if err != nil {
			log.Fatal(err)
		}
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

	defer func() {
		err := os.Remove(filePath)
		require.NoError(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		if err != nil {
			log.Fatal(err)
		}
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

func Test_Set_error(t *testing.T) {
	path := "data/fastdb_set_error.db"

	store, err := fastdb.Open(path, syncIime)
	require.NoError(t, err)
	assert.NotNil(t, store)

	defer func() {
		filePath := filepath.Clean(path)
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
