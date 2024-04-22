package pebble

import (
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenSharedTableCacheT(t *testing.T) {
	pbOptions := &Options{}
	// pbOptions.Experimental.EnableUniversalCompaction = true
	db, err := Open("~/demo3", pbOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Load data here ...

	// initialize keys
	keys := make([]string, 0)

	// Config ops numbers
	nOpsSets := 200
	keyLength := 1000
	inserts := 6
	updates := 3
	deletes := 1

	for j := 0; j < nOpsSets; j++ {
		fmt.Println(j)
		for i := 0; i < inserts; i++ {
			insertOp(db, &keys, keyLength)
		}
		for i := 0; i < updates; i++ {
			updateOp(db, &keys)
		}
		for i := 0; i < deletes; i++ {
			deleteOp(db, &keys)
		}

	}

	// close db
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Make sure that the Open function is using the passed in table cache
	// when the TableCache option is set.
	require.Equalf(
		t, 1, 1,
		"expected tableCache for both d0 and d1 to be the same",
	)
}

func generateRandomKey(keyLength int) []byte {
	var s []byte = make([]byte, keyLength)
	for i := range s {
		s[i] = byte(rand.Intn(58) + 65)
	}
	return s
}

func updateOp(d *DB, keys *[]string) {
	fmt.Println("update")
	nkeys := len(*keys)
	selectedKey := ""
	for {
		selectedKey := (*keys)[rand.Intn(nkeys-1)]
		if selectedKey != "deleted" {
			break
		}
	}

	key := []byte(selectedKey)
	if err := d.Set(key, generateRandomKey(10), Sync); err != nil {
		log.Fatal(err)
	}

}

func insertOp(d *DB, keys *[]string, keyLength int) {
	fmt.Println("insert")
	key := generateRandomKey(keyLength)
	if err := d.Set(key, generateRandomKey(keyLength), Sync); err != nil {
		log.Fatal(err)
	}
	*keys = append(*keys, string(key))
}

func deleteOp(d *DB, keys *[]string) {
	fmt.Println("delete")
	nkeys := len(*keys)
	index := rand.Intn(nkeys - 1)
	(*keys)[index] = "deleted"
	selectedKey := (*keys)[index]
	key := []byte(selectedKey)
	if err := d.Delete(key, Sync); err != nil {
		log.Fatal(err)
	}
}
