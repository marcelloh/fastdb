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
	store, err := fastdb.Open(":memory:", 100)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = store.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	total := 100000 // nr of records to work with

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
		json := string(dbRecords[key])
		value := gjson.Get(json, "UUID").Str + strconv.Itoa(key)
		keys[count] = record{SortField: value, Data: dbRecords[key]}
		count++
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].SortField.(string) < keys[j].SortField.(string)
	})

	log.Printf("sort %d records by UUID in %s", count, time.Since(start))

	for key, value := range keys {
		if key >= 15 {
			break
		}

		fmt.Printf("value : %v\n", string(value.Data))
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
		user.UUID = "UUIDtext_" + generateRandomString(8) + strconv.Itoa(user.ID)

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

	b := make([]byte, length)

	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}

	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}

	return string(b)
}
