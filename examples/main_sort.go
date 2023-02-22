/*
Package main holds some examples of the usage of the library.
*/
package main

/* ------------------------------- Imports --------------------------- */

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
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

	total := 100000
	start := time.Now()

	fillData(store, total)
	log.Printf("created %d records in %s", total, time.Since(start))

	start = time.Now()
	dbRecords, err := store.GetAll("user")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("read %d records in %s", total, time.Since(start))

	sortByKey(store, dbRecords)
	sortByUUID(store, dbRecords)
}

/*
sortByKey sorts the records by key.
*/
func sortByKey(store *fastdb.DB, dbRecords map[int][]byte) {
	start := time.Now()
	count := 0
	keys := make([]record, len(dbRecords))

	for key := range dbRecords {
		myKM := record{SortField: key, Data: dbRecords[key]}
		keys[count] = myKM
		count++
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].SortField.(int) < keys[j].SortField.(int)
	})

	log.Printf("sort %d records by key in %s", count, time.Since(start))

	for key, value := range keys {
		if key >= 15 {
			break
		}

		fmt.Printf("value : %v\n", string(value.Data))
	}
}

/*
sortByUUID sorts the records by UUID.
*/
func sortByUUID(store *fastdb.DB, dbRecords map[int][]byte) {
	start := time.Now()
	count := 0
	keys := make([]record, len(dbRecords))

	for key := range dbRecords {
		json := string(dbRecords[key])

		value := gjson.Get(json, "UUID").Str + strconv.Itoa(key)

		myKM := record{SortField: value, Data: dbRecords[key]}

		keys[count] = myKM
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

	rdom := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 1; i <= total; i++ {
		user.ID = i
		user.UUID = "UUIDtext_" + strconv.Itoa(rdom.Intn(100000000)) + strconv.Itoa(user.ID)
		userData, _ := json.Marshal(user)

		err := store.Set("user", user.ID, userData)
		if err != nil {
			log.Fatal(err)
		}
	}
}
