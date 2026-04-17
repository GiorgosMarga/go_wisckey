package db

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"
)

// func TestInsertDifferentSizes(t *testing.T) {
// 	clearFolders("../../sstables")
// 	clearFolders("../../vlogs")
// 	defer func() {
// 		clearFolders("../../sstables")
// 		clearFolders("../../vlogs")
// 	}()
// 	db := NewDB()

// 	entries := make(map[string][]byte)
// 	totalEntries := 100_000
// 	for range totalEntries {
// 		size := 2<<mathr.Intn(10) + 1
// 		key := make([]byte, size)
// 		value := make([]byte, size)
// 		rand.Read(key)
// 		rand.Read(value)
// 		entries[string(key)] = value
// 	}

// 	for k, v := range entries {
// 		if err := db.Insert([]byte(k), v); err != nil {
// 			t.Fatal(err)
// 		}
// 	}
// 	fmt.Printf("Inserted %d entries\n", totalEntries)
// 	for k, targetValue := range entries {
// 		v, err := db.Read([]byte(k))
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if !bytes.Equal(targetValue, v) {
// 			t.Fatalf("expected %x, got %x\n", targetValue, v)
// 		}
// 	}
// }

// func TestDB(t *testing.T) {
// 	clearFolders("../../sstables")
// 	clearFolders("../../vlogs")
// 	defer func() {
// 		clearFolders("../../sstables")
// 		clearFolders("../../vlogs")
// 	}()
// 	db := NewDB()

// 	entries := make(map[string][]byte)
// 	totalEntries := 10_000
// 	for range totalEntries {
// 		key := make([]byte, 512)
// 		value := make([]byte, 512)
// 		rand.Read(key)
// 		rand.Read(value)
// 		entries[string(key)] = value
// 	}
// 	fmt.Println(len(entries))

// 	for k, v := range entries {
// 		if err := db.Insert([]byte(k), v); err != nil {
// 			t.Fatal(err)
// 		}
// 	}
// 	fmt.Printf("Inserted %d entries\n", totalEntries)
// 	for k, targetValue := range entries {
// 		v, err := db.Read([]byte(k))
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if !bytes.Equal(targetValue, v) {
// 			t.Fatalf("expected %x, got %x\n", targetValue, v)
// 		}
// 	}

//		// reads might happen from the immtuable memtable so there is a chance that the sstable hasnt finished renaming
//		time.Sleep(1 * time.Second)
//	}
func TestCloseAndRecover(t *testing.T) {
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = NewDB()

	for k, targetValue := range entries {
		v, err := db.Read([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(targetValue, v) {
			t.Fatalf("expected %x, got %x\n", targetValue, v)
		}
	}

	// reads might happen from the immtuable memtable so there is a chance that the sstable hasnt finished renaming
	time.Sleep(1 * time.Second)
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
