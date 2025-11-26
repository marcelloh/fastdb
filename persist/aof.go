package persist

/* ------------------------------- Imports --------------------------- */

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* ---------------------- Constants/Types/Variables ------------------ */

const (
	fileMode = 0o600
	delLen   = 3
	setLen   = 4
)

// AOF is Append Only File.
type AOF struct {
	file     *os.File
	syncTime int
	mu       sync.RWMutex
}

// Lock     = &sync.Mutex{}
var osCreate = os.O_CREATE

/* -------------------------- Methods/Functions ---------------------- */

/*
OpenPersister opens the append only file and reads in all the data.
*/
func OpenPersister(path string, syncTime int) (*AOF, map[string]map[int][]byte, error) {
	aof := &AOF{syncTime: syncTime}

	filePath := filepath.Clean(path)
	if filePath != path {
		return nil, nil, fmt.Errorf("openPersister error: invalid path '%s'", path)
	}

	_, err := os.Stat(filepath.Dir(filePath))
	if errors.Is(err, fs.ErrNotExist) {
		err = os.MkdirAll(filepath.Dir(filePath), fileMode)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("openPersister (%s) error: %w", path, err)
	}

	err = aof.checkFileForCorruption(filePath)
	if err != nil {
		return nil, nil, err
	}

	keys, err := aof.getData(filePath)
	if err != nil {
		return nil, nil, err
	}

	go aof.flush()

	return aof, keys, nil
}

/*
Write writes to the file.
*/
func (aof *AOF) Write(lines string) error {
	defer aof.lockUnlock()()

	err := validateData(lines)
	if err != nil {
		return fmt.Errorf("validateData error: %w", err)
	}

	_, err = aof.file.WriteString(lines)
	if err == nil && aof.syncTime == 0 {
		err = aof.file.Sync()
	}

	if err != nil {
		err = fmt.Errorf("write error: %#v %w", aof.file.Name(), err)
	}

	return err
}

/*
Defrag will only store the last key information, so all the history is lost
This can mean a smaller filesize, which is quicker to read.
*/
func (aof *AOF) Defrag(keys map[string]map[int][]byte) (err error) {
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
	defer aof.lockUnlock()()

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

// validateData validates the data before writing
func validateData(lines string) error {
	lineParts := strings.Split(lines, "\n")

	switch lineParts[0] {
	case "del":
		// For delete operations, ensure we have exactly 3 lines (del, key, newline)
		if len(lineParts) != delLen || lineParts[2] != "" {
			return fmt.Errorf("invalid delete format: expected 'del\\nkey\\n', got '%s'", lines)
		}

	case "set":
		if len(lineParts) != setLen || lineParts[3] != "" {
			lines = strings.ReplaceAll(lines, "\n", "\\n")
			return fmt.Errorf("invalid set format: expected at least 'set\\nkey\\nvalue', got '%s'", lines)
		}

	default:
		return fmt.Errorf("invalid command: '%s'", lineParts[0])
	}

	keyParts := strings.Split(lineParts[1], "_")
	if len(keyParts) < 2 {
		return fmt.Errorf("invalid key format (invalid parts): '%s'", lineParts[1])
	}

	// Check if the ID part is a valid integer
	_, err := strconv.Atoi(keyParts[len(keyParts)-1])
	if err != nil {
		return fmt.Errorf("invalid key format (ID not a number): '%s': %w", lineParts[1], err)
	}

	return nil
}

// checkFileForCorruption checks the AOF file for obvious corruption.
func (aof *AOF) checkFileForCorruption(path string) error {
	defer aof.lockUnlock()()

	path = filepath.Clean(path)

	file, err := os.OpenFile(path, os.O_RDWR|osCreate, fileMode)
	if err != nil {
		return fmt.Errorf("openfile (%s) error: %w", path, err)
	}

	scanner := bufio.NewScanner(file)
	lineCount, corruptionErr := scanAndValidateFile(scanner)

	// Close the file
	err = file.Close()
	if err != nil {
		return fmt.Errorf("close file (%s) error: %w", path, err)
	}

	if corruptionErr != nil {
		return fmt.Errorf("database corrupted (%s) on line: %d error: %w", path, lineCount, corruptionErr)
	}

	return nil
}

// scanAndValidateFile scans the file and validates each line for corruption.
func scanAndValidateFile(scanner *bufio.Scanner) (int, error) {
	lineCount := 0

	for scanner.Scan() {
		lines := ""
		line := scanner.Text()
		lineCount++

		switch line {
		case "set":
			lines += line + "\n"

			scanner.Scan()

			line = scanner.Text()
			lineCount++

			lines += line + "\n"

			scanner.Scan()

			line = scanner.Text()
			lineCount++

			lines += line + "\n"
		case "del":
			lines += line + "\n"

			scanner.Scan()

			line = scanner.Text()
			lineCount++

			lines += line + "\n"
		default:
			return lineCount, fmt.Errorf("error: wrong instruction format '%s' on line: %d", line, lineCount)
		}

		err := validateData(lines)
		if err != nil {
			return lineCount, fmt.Errorf("validateData error: %w", err)
		}
	}

	return lineCount, nil
}

/*
getData opens a file and reads the data into the memory.
*/
func (aof *AOF) getData(path string) (map[string]map[int][]byte, error) {
	defer aof.lockUnlock()()

	var (
		file *os.File
		err  error
	)

	path = filepath.Clean(path)

	file, err = os.OpenFile(path, os.O_RDWR|osCreate, fileMode)
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
	// Increase buffer size
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) //nolint:mnd // ignore magic number

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
	if !scanner.Scan() {
		return count, fmt.Errorf("file (%s) has incomplete instruction on line: %d", aof.file.Name(), count)
	}

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
	key := scanner.Text()

	// Check for JSON-like content or other invalid characters in the key which would indicate corruption
	if strings.Contains(key, "{") || strings.Contains(key, "}") ||
		strings.Contains(key, "\":\"") || strings.Contains(key, "set") ||
		strings.Contains(key, "del") || strings.Contains(key, "\"") {
		return count, fmt.Errorf("file (%s) has wrong instruction format '%s' on line: %d", aof.file.Name(), key, count)
	}

	bucket, keyID, ok := aof.parseBucketAndKey(key)
	if !ok {
		return count, fmt.Errorf("file (%s) has wrong key format: '%s' on line: %d", aof.file.Name(), key, count)
	}

	// Check if bucket exists before trying to delete
	if _, exists := keys[bucket]; exists {
		delete(keys[bucket], keyID)

		// If bucket is empty, delete it
		if len(keys[bucket]) == 0 {
			delete(keys, bucket)
		}
	}

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
		keys[bucket] = make(map[int][]byte)
	}

	// unescape newlines
	value = strings.ReplaceAll(value, "\\n", "\n")
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

/*
lockUnlock locks the database and unlocks it later

if you call it like this: defer fdb.lockUnlock()()
the first function call locks it and because it returns a function,
that function will actually be called as the defer.
*/
func (aof *AOF) lockUnlock() func() {
	aof.mu.Lock()
	//nolint:gocritic // leave it here
	// log.Println("> Locked")

	return func() {
		aof.mu.Unlock()
		//nolint:gocritic // leave it here
		// log.Println("> Unlocked")
	}
}
