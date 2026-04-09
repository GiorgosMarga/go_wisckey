package db

import "testing"

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
