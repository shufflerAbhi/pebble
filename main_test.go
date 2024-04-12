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

	for i := 0; i < 500; i++ {
		fmt.Println(i)
		key := generateRandomKey(1000)
		if err := db.Set(key, generateRandomKey(1000), Sync); err != nil {
			log.Fatal(err)
		}
		_, closer, err := db.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Printf("%s %s\n", key, value)
		if err := closer.Close(); err != nil {
			log.Fatal(err)
		}
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
