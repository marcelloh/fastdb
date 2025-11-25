/*
Package main holds some examples of the usage of the library.
*/
package main

/* ------------------------------- Imports --------------------------- */

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/marcelloh/fastdb"
	"github.com/tidwall/gjson"
)

/* ---------------------- Constants/Types/Variables ------------------ */

const maxRecords = 100000

const showMax = 15

type user struct {
	ID    int
	UUID  string
	Email string
}

type record struct {
	SortField any
	Data      []byte
}

/* -------------------------- Methods/Functions ---------------------- */

/*
main is the bootstrap of the application.
*/
func main() {
	store, err := fastdb.Open(":memory:", 0)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = store.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	total := maxRecords // nr of records to work with

	start := time.Now()

	fillData(store, total)
	log.Printf("created %d records in %s", total, time.Since(start))

	start = time.Now()

	dbRecords, err := store.GetAll("user")
	if err != nil {
		log.Panic(err)
	}

	log.Printf("read %d records in %s", total, time.Since(start))

	sortByUUID(dbRecords)
}

/*
sortByUUID sorts the records by UUID.
*/
func sortByUUID(dbRecords map[int][]byte) {
	start := time.Now()
	count := 0
	keys := make([]record, len(dbRecords))

	for key := range dbRecords {
		jsonKey := string(dbRecords[key])
		value := gjson.Get(jsonKey, "UUID").Str + strconv.Itoa(key)

		keys[count] = record{SortField: value, Data: dbRecords[key]}
		count++
	}

	sort.Slice(keys, func(i, j int) bool {
		iVal, iOk := keys[i].SortField.(string)

		jVal, jOk := keys[j].SortField.(string)

		if !iOk || !jOk {
			// Handle the error case - perhaps provide a default comparison or panic with a message
			panic("SortField is not a string")
		}

		return iVal < jVal
	})

	log.Printf("sort %d records by UUID in %s", count, time.Since(start))

	for key := range keys {
		if key >= showMax {
			break
		}

		fmt.Printf("value : %v\n", string(keys[key].Data))
	}
}

func fillData(store *fastdb.DB, total int) {
	user := &user{
		ID:    1,
		UUID:  "UUIDtext_",
		Email: "test@example.com",
	}

	for i := 1; i <= total; i++ {
		user.ID = i
		user.UUID = "UUIDtext_" + generateRandomString(10) + strconv.Itoa(user.ID)

		userData, err := json.Marshal(user)
		if err != nil {
			log.Fatal(err)
		}

		err = store.Set("user", user.ID, userData)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// generateRandomString creates a random string of the specified length
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	bytes := make([]byte, length)

	_, err := rand.Read(bytes)
	if err != nil {
		log.Fatal(err)
	}

	for i := range bytes {
		bytes[i] = charset[int(bytes[i])%len(charset)]
	}

	return string(bytes)
}
