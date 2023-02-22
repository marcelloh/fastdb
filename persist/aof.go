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

const fileMode = 0o600

// AOF is Append Only File.
type AOF struct {
	file     *os.File
	syncIime int
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
	aof := &AOF{syncIime: syncIime, stop: false}

	dataDir := "./data"

	_, err := os.Stat(dataDir)
	if errors.Is(err, fs.ErrNotExist) {
		dataDir = "./../data"
	}

	filePath := filepath.Join(dataDir, filepath.Clean(path))

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

	var (
		file *os.File
		err  error
	)

	file, err = os.OpenFile(path, os.O_RDWR|osCreate, fileMode) //nolint:gosec // path is clean
	if err != nil {
		return nil, fmt.Errorf("openfile (%s) error: %w", path, err)
	}

	aof.file = file

	keys, err := aof.fileReader()
	if err != nil {
		orgErr := err

		err = aof.file.Close()
		if err == nil {
			err = orgErr
		}
	}

	if err != nil {
		err = fmt.Errorf("fileReader (%s) error: %w", path, err)
	}

	return keys, err
}

/*
fileReader reads the file and fill the keys.
*/
func (aof *AOF) fileReader() (map[string]map[int][]byte, error) {
	var (
		count int
		line  string
		key   string
		nrID  int
		err   error
	)

	keys := make(map[string]map[int][]byte)

	scanner := bufio.NewScanner(aof.file)
	for scanner.Scan() {
		line = scanner.Text()
		count++

		switch line {
		case "set":
			scanner.Scan()
			key = scanner.Text()
			count++

			pieces := strings.Split(key, "_")
			if len(pieces) != 2 {
				return nil, fmt.Errorf("file (%s) has wrong key format on line: %d", aof.file.Name(), count)
			}

			nrID, err = strconv.Atoi(pieces[1])
			if err != nil {
				return nil, fmt.Errorf("file (%s) has wrong key format on line: %d %w", aof.file.Name(), count, err)
			}

			_, found := keys[pieces[0]]
			if !found {
				keys[pieces[0]] = map[int][]byte{}
			}

			scanner.Scan()
			line = scanner.Text()
			count++
			keys[pieces[0]][nrID] = []byte(line)
		case "del":
			scanner.Scan()
			key = scanner.Text()
			count++
			delete(keys, key)
		default:
			return nil, fmt.Errorf("file (%s) has wrong instruction format on line: %d", aof.file.Name(), count)
		}
	}

	return keys, nil
}

/*
Write writes to the file.
*/
func (aof *AOF) Write(lines string) error {
	_, err := aof.file.WriteString(lines)
	if err == nil && aof.syncIime == 0 {
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
	if aof.syncIime == 0 {
		return
	}

	flushPause := time.Millisecond * time.Duration(aof.syncIime)

	tick := time.NewTicker(flushPause)
	defer tick.Stop()

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
	flushPause := time.Millisecond * time.Duration(aof.syncIime)
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
		for key := range keys[bucket] {
			lines := "set\n" + bucket + "_" + strconv.Itoa(key) + "\n" + string(keys[bucket][key]) + "\n"

			err = aof.Write(lines)
			if err != nil {
				return fmt.Errorf("write error:%w", err)
			}
		}
	}

	return nil
}
