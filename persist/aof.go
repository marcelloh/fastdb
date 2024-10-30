package persist

/* ------------------------------- Imports --------------------------- */

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* ---------------------- Constants/Types/Variables ------------------ */

const fileMode = 0o600

// AOF is Append Only File.
type AOF struct {
	file     *os.File
	syncTime int
	mu       sync.RWMutex
	stop     bool
}

var (
	lock     = &sync.Mutex{}
	osCreate = os.O_CREATE
)

/* -------------------------- Methods/Functions ---------------------- */

/*
OpenPersister opens the append only file and reads in all the data.
*/
func OpenPersister(path string, syncIime int) (*AOF, map[string]map[int][]byte, error) {
	aof := &AOF{syncTime: syncIime, stop: false}

	filePath := filepath.Clean(path)

	_, err := os.Stat(filepath.Dir(filePath))
	if err != nil {
		return nil, nil, fmt.Errorf("openPersister (%s) error: %w", path, err)
	}

	keys, err := aof.getData(filePath)
	if err != nil {
		return nil, nil, err
	}

	go aof.flush()

	return aof, keys, nil
}

/*
getData opens a file and reads the data into the memory.
*/
func (aof *AOF) getData(path string) (map[string]map[int][]byte, error) {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	filePath := filepath.Clean(path)
	if filePath != path {
		return nil, fmt.Errorf("getData error: invalid path '%s'", path)
	}

	var (
		file *os.File
		err  error
	)

	file, err = os.OpenFile(filePath, os.O_RDWR|osCreate, fileMode) //nolint:gosec // path is clean
	if err != nil {
		return nil, fmt.Errorf("openfile (%s) error: %w", path, err)
	}

	aof.file = file

	return aof.readDataFromFile(path)
}

/*
readDataFromFile reads the file and fills the keys map.
Returns the keys map and an error if something went wrong.
It also closes the file if there was an error, and returns
an error with the close error if there is one.
*/
func (aof *AOF) readDataFromFile(path string) (map[string]map[int][]byte, error) {
	keys, err := aof.fileReader()
	if err != nil {
		closeErr := aof.file.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("fileReader (%s) error: %w; close error: %w", path, err, closeErr)
		}

		return nil, fmt.Errorf("fileReader (%s) error: %w", path, err)
	}

	return keys, err
}

/*
fileReader reads the file and fills the keys.
*/
func (aof *AOF) fileReader() (map[string]map[int][]byte, error) {
	var (
		count int
		err   error
	)

	keys := make(map[string]map[int][]byte, 1)
	scanner := bufio.NewScanner(aof.file)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // Increase buffer size

	for scanner.Scan() {
		count++
		instruction := scanner.Text()

		count, err = aof.processInstruction(instruction, scanner, count, keys)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

/*
processInstruction processes an instruction from the AOF file and fills the keys.
*/
func (aof *AOF) processInstruction(
	instruction string,
	scanner *bufio.Scanner,
	count int,
	keys map[string]map[int][]byte,
) (int, error) {
	switch instruction {
	case "set":
		return aof.handleSetInstruction(scanner, count, keys)
	case "del":
		return aof.handleDelInstruction(scanner, count, keys)
	default:
		return count, fmt.Errorf("file (%s) has wrong instruction format '%s' on line: %d", aof.file.Name(), instruction, count)
	}
}

/*
handleSetInstruction handles the set instruction.
*/
func (aof *AOF) handleSetInstruction(scanner *bufio.Scanner, inpCount int, keys map[string]map[int][]byte) (int, error) {
	count := inpCount

	if !scanner.Scan() {
		return count, fmt.Errorf("file (%s) has incomplete set instruction on line: %d", aof.file.Name(), count)
	}

	key := scanner.Text()

	if !scanner.Scan() {
		return count, fmt.Errorf("file (%s) has incomplete set instruction on line: %d", aof.file.Name(), count)
	}

	line := scanner.Text()

	err := aof.setBucketAndKey(key, line, keys)
	if err != nil {
		return count, err
	}

	count += 2

	return count, nil
}

/*
handleDelInstruction handles the del instruction.
*/
func (aof *AOF) handleDelInstruction(scanner *bufio.Scanner, inpCount int, keys map[string]map[int][]byte) (int, error) {
	count := inpCount

	if !scanner.Scan() {
		return count, fmt.Errorf("file (%s) has incomplete del instruction on line: %d", aof.file.Name(), count)
	}

	key := scanner.Text()

	bucket, keyID, ok := aof.parseBucketAndKey(key)
	if !ok {
		return count, fmt.Errorf("file (%s) has wrong key format: '%s' on line: %d", aof.file.Name(), key, count)
	}

	delete(keys[bucket], keyID)

	count++

	return count, nil
}

/*
setBucketAndKey sets a key-value pair in a bucket.
*/
func (aof *AOF) setBucketAndKey(key, value string, keys map[string]map[int][]byte) error {
	bucket, keyID, ok := aof.parseBucketAndKey(key)
	if !ok {
		return fmt.Errorf("file (%s) has wrong key format: %s", aof.file.Name(), key)
	}

	if _, found := keys[bucket]; !found {
		keys[bucket] = map[int][]byte{}
	}

	keys[bucket][keyID] = []byte(value)

	return nil
}

/*
parseBucketAndKey parses a key in the format "bucket_keyid" and returns
the bucket name, key id and true if the key is valid.
Otherwise it returns empty string, 0 and false.
*/
func (*AOF) parseBucketAndKey(key string) (string, int, bool) {
	uPos := strings.LastIndex(key, "_")
	if uPos < 0 {
		return "", 0, false
	}

	bucket := key[:uPos]

	keyID, err := strconv.Atoi(key[uPos+1:])
	if err != nil {
		return "", 0, false
	}

	return bucket, keyID, true
}

/*
Write writes to the file.
*/
func (aof *AOF) Write(lines string) error {
	_, err := aof.file.WriteString(lines)
	if err == nil && aof.syncTime == 0 {
		err = aof.file.Sync()
	}

	if err != nil {
		err = fmt.Errorf("write error: %#v %w", aof.file.Name(), err)
	}

	return err
}

/*
Flush starts a goroutine to sync the database.
The routine will stop if the file is closed
*/
func (aof *AOF) flush() {
	if aof.syncTime == 0 {
		return
	}

	flushPause := time.Millisecond * time.Duration(aof.syncTime)
	tick := time.NewTicker(flushPause)

	defer func() {
		tick.Stop()
	}()

	for range tick.C {
		err := aof.file.Sync()
		if err != nil {
			break
		}
	}
}

/*
Defrag will only store the last key information, so all the history is lost
This can mean a smaller filesize, which is quicker to read.
*/
func (aof *AOF) Defrag(keys map[string]map[int][]byte) (err error) {
	lock.Lock()
	defer lock.Unlock()

	// close current file (to flush the last parts)
	err = aof.Close()
	if err != nil {
		return fmt.Errorf("defrag->close error: %w", err)
	}

	err = aof.makeBackup()
	if err != nil {
		return fmt.Errorf("defrag->makeBackup error: %w", err)
	}

	err = aof.writeFile(keys)
	if err != nil {
		return fmt.Errorf("defrag->writeFile error: %w", err)
	}

	return nil
}

/*
Close stops the flush routine, flushes the last data to disk and closes the file.
*/
func (aof *AOF) Close() error {
	err := aof.file.Sync()
	if err != nil {
		return fmt.Errorf("close->Sync error: %s %w", aof.file.Name(), err)
	}

	err = aof.file.Close()
	if err != nil {
		return fmt.Errorf("close error: %s %w", aof.file.Name(), err)
	}

	// to be sure that the flushing is stopped
	flushPause := time.Millisecond * time.Duration(aof.syncTime)
	time.Sleep(flushPause)

	return nil
}

/*
makeBackup creates a backup of the current file.
*/
func (aof *AOF) makeBackup() (err error) {
	path := filepath.Clean(aof.file.Name())

	source, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("defrag->open error: %w", err)
	}

	defer func() {
		err = source.Close()
	}()

	// copy current file to backup
	destination, err := os.Create(filepath.Clean(path + ".bak"))
	if err != nil {
		return fmt.Errorf("defrag->create error: %w", err)
	}

	defer func() {
		err = destination.Close()
		if err != nil {
			err = fmt.Errorf("defrag->close error: %w", err)
		}
	}()

	_, err = io.Copy(destination, source)
	if err != nil {
		return fmt.Errorf("defrag->copy error: %w", err)
	}

	return nil
}

func (aof *AOF) writeFile(keys map[string]map[int][]byte) error {
	var err error

	path := aof.file.Name()

	// create and open temp file
	err = os.Remove(path)
	if err != nil {
		return fmt.Errorf("writeFile->remove (%#v) error: %w", path, err)
	}

	_, err = aof.getData(path)
	if err != nil {
		return fmt.Errorf("writeFile->getData error: %w", err)
	}

	// write keys to file
	go aof.flush()

	for bucket := range keys {
		startLine := "set\n" + bucket + "_"
		for key := range keys[bucket] {
			lines := startLine + strconv.Itoa(key) + "\n" + string(keys[bucket][key]) + "\n"

			err = aof.Write(lines)
			if err != nil {
				return fmt.Errorf("write error:%w", err)
			}
		}
	}

	return nil
}
