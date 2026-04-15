package db

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
)

// func TestInsert(t *testing.T) {
// 	db := NewDB()
// 	key := []byte("Hello")
// 	value := []byte("World")
// 	if err := db.Insert(key, value); err != nil {
// 		t.Fatal(err)
// 	}
// }
// func TestRead(t *testing.T) {
// 	db := NewDB()
// 	key := []byte("Hello")
// 	value := []byte("World")
// 	if err := db.Insert(key, value); err != nil {
// 		t.Fatal(err)
// 	}
// 	readValue, err := db.Read(key)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

//		if !bytes.Equal(readValue, value) {
//			t.Fatalf("expected: %s, got %s\n", value, readValue)
//		}
//	}
func TestDB(t *testing.T) {
	defer func() {
		clearFolders("../../sstables")
		clearFolders("../../vlogs")
	}()
	db := NewDB()

	entries := make(map[string][]byte)
	totalEntries := 10_000
	for range totalEntries {
		key := make([]byte, 512)
		value := make([]byte, 512)
		rand.Read(key)
		rand.Read(value)
		entries[string(key)] = value
	}

	for k, v := range entries {
		if err := db.Insert([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Printf("Inserted %d entries\n", totalEntries)
	for k, targetValue := range entries {
		v, err := db.Read([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(targetValue, v) {
			t.Fatalf("expected %x, got %x\n", targetValue, v)
		}
	}

}

func clearFolders(path string) {
	// dir := fmt.Sprintf("../../sstables")
	// vlogsPath := fmt.Sprintf("../../vlogs")
	dir, err := os.ReadDir(path)
	if err != nil {
		panic(err)
	}

	for _, file := range dir {
		os.Remove(fmt.Sprintf("%s/%s", path, file.Name()))
	}
}
