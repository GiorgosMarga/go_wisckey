package db

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

func TestInsert(t *testing.T) {
	db := NewDB()
	key := []byte("Hello")
	value := []byte("World")
	if err := db.Insert(key, value); err != nil {
		t.Fatal(err)
	}
}
func TestRead(t *testing.T) {
	db := NewDB()
	key := []byte("Hello")
	value := []byte("World")
	if err := db.Insert(key, value); err != nil {
		t.Fatal(err)
	}
	readValue, err := db.Read(key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(readValue, value) {
		t.Fatalf("expected: %s, got %s\n", value, readValue)
	}
}
func TestDB(t *testing.T) {
	db := NewDB()

	entries := make(map[string][]byte)
	for range 10_000 {
		key := make([]byte, 512)
		value := make([]byte, 512)
		rand.Read(key)
		rand.Read(value)
		entries[fmt.Sprintf("%x", key)] = value
	}

	for k, v := range entries {
		if err := db.Insert([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}

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
