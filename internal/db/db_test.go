package db

import (
	"bytes"
	"testing"
)

func TestInsert(t *testing.T) {
	db, err := NewDB()
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("Hello")
	value := []byte("World")
	if err := db.Insert(key, value); err != nil {
		t.Fatal(err)
	}
}
func TestRead(t *testing.T) {
	db, err := NewDB()
	if err != nil {
		t.Fatal(err)
	}
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
