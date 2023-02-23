package fastdb

/* ------------------------------- Imports --------------------------- */

import (
	"errors"
	"fmt"
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

	keys := make(map[string]map[int][]byte)

	if path != ":memory:" {
		aof, keys, err = persist.OpenPersister(path, syncIime)
	}

	return &DB{aof: aof, keys: keys}, err //nolint:wrapcheck // it is already wrapped
}

/*
Defrag optimises the file to reflect the latest state.
*/
func (fdb *DB) Defrag() error {
	fdb.mu.Lock()
	defer fdb.mu.Unlock()

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
	var err error

	fdb.mu.Lock()
	defer fdb.mu.Unlock()

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
GetAll returns all map values from a bucket.
*/
func (fdb *DB) GetAll(bucket string) (map[int][]byte, error) {
	fdb.mu.RLock()
	defer fdb.mu.RUnlock()

	bmap, found := fdb.keys[bucket]
	if !found {
		return nil, errors.New("bucket not found")
	}

	return bmap, nil
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
	fdb.mu.Lock()
	defer fdb.mu.Unlock()

	if fdb.aof != nil {
		lines := "set\n" + bucket + "_" + strconv.Itoa(key) + "\n" + string(value) + "\n"

		err := fdb.aof.Write(lines)
		if err != nil {
			return fmt.Errorf("sel->write error: %w", err)
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
		fdb.mu.Lock()
		defer fdb.mu.Unlock()

		err := fdb.aof.Close()
		if err != nil {
			return fmt.Errorf("close error: %w", err)
		}
	}

	fdb.keys = make(map[string]map[int][]byte)

	return nil
}
