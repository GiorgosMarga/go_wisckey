package vlog

import (
	"bytes"
	"testing"
)

func TestInsert(t *testing.T) {
	vlog, err := NewVLog()
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("Hello")
	value := []byte("World")
	offset, err := vlog.Append(key, value)
	if err != nil {
		t.Fatal(err)
	}

	readValue, err := vlog.Read(offset)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(readValue, value) {
		t.Fatalf("expected %s, got %s\n", string(value), string(readValue))
	}
}
