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
)

const (
	syncIime = 100
	dataDir  = "./data"
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
		assert.Nil(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		assert.Nil(t, err)
	}()
}

func Test_Open_Memory(t *testing.T) {
	path := ":memory:"

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		assert.Nil(t, err)
	}()
}

func Test_SetGetDel_oneRecord(t *testing.T) {
	path := "data/fastdb_set.db"

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		assert.Nil(t, err)

		filePath := filepath.Clean(path)
		err = os.Remove(filePath)
		assert.Nil(t, err)
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	recordData, _ := json.Marshal(record)

	err = store.Set("texts", record.ID, recordData)
	assert.Nil(t, err)

	info := store.Info()
	assert.Equal(t, "1 record(s) in 1 bucket(s)", info)

	memData, ok := store.Get("texts", 1)
	assert.True(t, ok)

	memRecord := &someRecord{}
	err = json.Unmarshal(memData, &memRecord)
	assert.Nil(t, err)
	assert.NotNil(t, memRecord)
	assert.Equal(t, record.UUID, memRecord.UUID)
	assert.Equal(t, record.Text, memRecord.Text)
	assert.Equal(t, record.ID, memRecord.ID)

	// delete in non existing bucket
	ok, err = store.Del("notexisting", 1)
	assert.Nil(t, err)
	assert.False(t, ok)

	// delete non existing key
	ok, err = store.Del("texts", 123)
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = store.Del("texts", 1)
	assert.Nil(t, err)
	assert.True(t, ok)

	info = store.Info()
	assert.Equal(t, "0 record(s) in 0 bucket(s)", info)
}

func Test_Get_wrongRecord(t *testing.T) {
	path := ":memory:"

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		assert.Nil(t, err)
	}()

	// store a record
	err = store.Set("bucket", 1, []byte("a text"))
	assert.Nil(t, err)

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
		assert.Nil(t, err)

		_ = os.Remove(filePath + ".bak")
	}()

	total := 1000

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store)

	defer func() {
		err = store.Close()
		assert.Nil(t, err)
	}()

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	s1 := rand.NewSource(time.Now().UnixNano())
	rdom := rand.New(s1)

	for i := 0; i < total; i++ {
		record.ID = rdom.Intn(10) + 1
		recordData, _ := json.Marshal(record)

		err = store.Set("records", record.ID, recordData)
		assert.Nil(t, err)
	}

	checkFileLines(t, filePath, total*3)

	err = store.Defrag()
	assert.Nil(t, err)

	checkFileLines(t, filePath, 30)
}

func Test_GetAllFromMemory_1000(t *testing.T) {
	total := 1000
	path := ":memory:"

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
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

	for i := 1; i <= total; i++ {
		record.ID = i
		recordData, _ := json.Marshal(record)

		err = store.Set("records", record.ID, recordData)
		assert.Nil(t, err)
	}

	records, err := store.GetAll("records")
	assert.Nil(t, err)
	assert.NotNil(t, records)
	assert.Equal(t, total, len(records))

	records, err = store.GetAll("wrong_bucket")
	assert.NotNil(t, err)
	assert.Nil(t, records)
}

func Test_GetAllFromFile_1000(t *testing.T) {
	total := 1000
	path := "data/fastdb_1000.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
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

	for i := 1; i <= total; i++ {
		record.ID = rdom.Intn(1000000)
		recordData, _ := json.Marshal(record)

		err = store.Set("user", record.ID, recordData)
		assert.Nil(t, err)
	}

	records, err := store.GetAll("user")
	assert.Nil(t, err)
	assert.NotNil(t, records)
}

func Test_Set_error(t *testing.T) {
	path := "data/fastdb_set_error.db"

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store)

	defer func() {
		filePath := filepath.Clean(path)
		err = os.Remove(filePath)
		assert.Nil(t, err)
	}()

	err = store.Close()
	assert.Nil(t, err)

	// store a record
	err = store.Set("bucket", 1, []byte("a text"))
	assert.NotNil(t, err)
}

func Test_Set_wrongBucket(t *testing.T) {
	path := "data/fastdb_set_bucket_error.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		assert.Nil(t, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store)

	// store a record
	err = store.Set("under_score", 1, []byte("a text for key 1"))
	assert.Nil(t, err)

	err = store.Set("under_score", 2, []byte("a text for key 2"))
	assert.Nil(t, err)

	err = store.Close()
	assert.Nil(t, err)

	store2, err := fastdb.Open(path, syncIime)
	assert.Nil(t, err)
	assert.NotNil(t, store2)

	defer func() {
		err = store2.Close()
		assert.Nil(t, err)
	}()
}

func Benchmark_Get_File_1000(b *testing.B) {
	path := "data/bench-get.db"
	total := 1000

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		assert.Nil(b, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(b, err)
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

	for i := 1; i <= total; i++ {
		record.ID = rdom.Intn(1000000)
		recordData, _ := json.Marshal(record)

		err = store.Set("bench_bucket", record.ID, recordData)
		assert.Nil(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		_, _ = store.Get("bench_bucket", rand.Intn(1000000))
	}

	err = store.Close()
	assert.Nil(b, err)
}

func Benchmark_Get_Memory_1000(b *testing.B) {
	path := ":memory:"
	total := 1000

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(b, err)
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

	for i := 1; i <= total; i++ {
		record.ID = rdom.Intn(1000000)
		recordData, _ := json.Marshal(record)

		err = store.Set("bench_bucket", record.ID, recordData)
		assert.Nil(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		_, _ = store.Get("bench_bucket", rand.Intn(1000000))
	}

	err = store.Close()
	assert.Nil(b, err)
}

func Benchmark_Set_File_NoSyncTime(b *testing.B) {
	path := "data/bench-set.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		assert.Nil(b, err)
	}()

	store, err := fastdb.Open(path, 0)
	assert.Nil(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		record.ID = rand.Intn(1000000)
		recordData, _ := json.Marshal(record)

		_ = store.Set("user", record.ID, recordData)
	}

	err = store.Close()
	assert.Nil(b, err)
}

func Benchmark_Set_File_WithSyncTime(b *testing.B) {
	path := "data/bench-set.db"

	filePath := filepath.Clean(path)
	_ = os.Remove(filePath)

	defer func() {
		err := os.Remove(filePath)
		assert.Nil(b, err)
	}()

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		record.ID = rand.Intn(1000000)
		recordData, _ := json.Marshal(record)

		_ = store.Set("user", record.ID, recordData)
	}

	err = store.Close()
	assert.Nil(b, err)
}

func Benchmark_Set_Memory(b *testing.B) {
	path := ":memory:"

	store, err := fastdb.Open(path, syncIime)
	assert.Nil(b, err)
	assert.NotNil(b, store)

	x1 := rand.NewSource(time.Now().UnixNano())
	_ = rand.New(x1)

	record := &someRecord{
		ID:   1,
		UUID: "UUIDtext",
		Text: "a text",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ { // use b.N for looping
		record.ID = rand.Intn(1000000)
		recordData, _ := json.Marshal(record)

		_ = store.Set("user", record.ID, recordData)
	}

	err = store.Close()
	assert.Nil(b, err)
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
