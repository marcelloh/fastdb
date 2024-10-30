package fastdb

/* ------------------------------- Imports --------------------------- */

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"

	"github.com/marcelloh/fastdb/persist"
)

/* ---------------------- Constants/Types/Variables ------------------ */

// DB represents a collection of key-value pairs that persist on disk or memory.
type DB struct {
	aof  *persist.AOF
	keys map[string]map[int][]byte
	mu   sync.RWMutex
}

// SortRecord represents a record from a sorted collection of sliced records
type SortRecord struct {
	SortField any
	Data      []byte
}

/* -------------------------- Methods/Functions ---------------------- */

/*
Open opens a database at the provided path.
If the file doesn't exist, it will be created automatically.
If the path is ':memory:' then the database will be opened in memory only.
*/
func Open(path string, syncIime int) (*DB, error) {
	var (
		aof *persist.AOF
		err error
	)

	keys := map[string]map[int][]byte{}

	if path != ":memory:" {
		aof, keys, err = persist.OpenPersister(path, syncIime)
	}

	return &DB{aof: aof, keys: keys}, err //nolint:wrapcheck // it is already wrapped
}

/*
Defrag optimises the file to reflect the latest state.
*/
func (fdb *DB) Defrag() error {
	defer fdb.lockUnlock()()

	var err error

	err = fdb.aof.Defrag(fdb.keys)
	if err != nil {
		err = fmt.Errorf("defrag error: %w", err)
	}

	return err
}

/*
Del deletes one map value in a bucket.
*/
func (fdb *DB) Del(bucket string, key int) (bool, error) {
	defer fdb.lockUnlock()()

	var err error

	// bucket exists?
	_, found := fdb.keys[bucket]
	if !found {
		return found, nil
	}

	// key exists in bucket?
	_, found = fdb.keys[bucket][key]
	if !found {
		return found, nil
	}

	if fdb.aof != nil {
		lines := "del\n" + bucket + "_" + strconv.Itoa(key) + "\n"

		err = fdb.aof.Write(lines)
		if err != nil {
			return false, fmt.Errorf("del->write error: %w", err)
		}
	}

	delete(fdb.keys[bucket], key)

	if len(fdb.keys[bucket]) == 0 {
		delete(fdb.keys, bucket)
	}

	return true, nil
}

/*
Get returns one map value from a bucket.
*/
func (fdb *DB) Get(bucket string, key int) ([]byte, bool) {
	fdb.mu.RLock()
	defer fdb.mu.RUnlock()

	data, ok := fdb.keys[bucket][key]

	return data, ok
}

/*
GetAll returns all map values from a bucket in random order.
*/
func (fdb *DB) GetAll(bucket string) (map[int][]byte, error) {
	fdb.mu.RLock()
	defer fdb.mu.RUnlock()

	bmap, found := fdb.keys[bucket]
	if !found {
		return nil, fmt.Errorf("bucket (%s) not found", bucket)
	}

	return bmap, nil
}

/*
GetAllSorted returns all map values from a bucket in Key sorted order.
*/
func (fdb *DB) GetAllSorted(bucket string) ([]*SortRecord, error) {
	memRecords, err := fdb.GetAll(bucket)
	if err != nil {
		return nil, err
	}

	sortedKeys := slices.Sorted(maps.Keys(memRecords))

	sortedRecords := make([]*SortRecord, len(memRecords))

	for count, key := range sortedKeys {
		sortedRecords[count] = &SortRecord{SortField: key, Data: memRecords[key]}
		// count++
	}

	return sortedRecords, nil
}

/*
GetNewIndex returns the next available index for a bucket.
*/
func (fdb *DB) GetNewIndex(bucket string) (newKey int) {
	memRecords, err := fdb.GetAll(bucket)
	if err != nil {
		return 1
	}

	lkey := 0
	for key := range memRecords {
		if key > lkey {
			lkey = key
		}
	}

	newKey = lkey + 1

	return newKey
}

/*
Info returns info about the storage.
*/
func (fdb *DB) Info() string {
	count := 0
	for i := range fdb.keys {
		count += len(fdb.keys[i])
	}

	return fmt.Sprintf("%d record(s) in %d bucket(s)", count, len(fdb.keys))
}

/*
Set stores one map value in a bucket.
*/
func (fdb *DB) Set(bucket string, key int, value []byte) error {
	defer fdb.lockUnlock()()

	if key < 0 {
		return errors.New("set->key should be positive")
	}

	if fdb.aof != nil {
		lines := "set\n" + bucket + "_" + strconv.Itoa(key) + "\n" + string(value) + "\n"

		err := fdb.aof.Write(lines)
		if err != nil {
			return fmt.Errorf("set->write error: %w", err)
		}
	}

	_, found := fdb.keys[bucket]
	if !found {
		fdb.keys[bucket] = map[int][]byte{}
	}

	fdb.keys[bucket][key] = value

	return nil
}

/*
Close closes the database.
*/
func (fdb *DB) Close() error {
	if fdb.aof != nil {
		defer fdb.lockUnlock()()

		err := fdb.aof.Close()
		if err != nil {
			return fmt.Errorf("close error: %w", err)
		}
	}

	fdb.keys = map[string]map[int][]byte{}

	return nil
}

/*
lockUnlock locks the database and unlocks it later

if you call it like this: defer fdb.lockUnlock()()
the first function call locks it and because it returns a function,
that function will actually be called as the defer.
*/
func (fdb *DB) lockUnlock() func() {
	fdb.mu.Lock()
	//nolint:gocritic // leave it here
	// log.Println("> Locked")

	return func() {
		fdb.mu.Unlock()
		//nolint:gocritic // leave it here
		// log.Println("> Unlocked")
	}
}
