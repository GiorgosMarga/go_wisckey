package lsm

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
)

func TestNewSSTable(t *testing.T) {
	lsm := NewLSM(1024)
	for range 3 {
		key := make([]byte, 512)
		rand.Read(key)

		if err := lsm.Insert(key, 0, 0); err != nil {
			if strings.Contains(err.Error(), "exists") {
				fmt.Printf("Key %x\n", key)
			}
			t.Fatal(err)
		}
	}
}
func TestReadFromSSTable(t *testing.T) {
	lsm := NewLSM(1024)
	targetKey := bytes.Repeat([]byte{0}, 512)
	for i := range 3 {
		key := bytes.Repeat([]byte{byte(i)}, 512)
		if err := lsm.Insert(key, 0, 0); err != nil {
			t.Fatal(err)
		}
	}

	_, err := lsm.Get(targetKey)
	if err != nil {
		t.Fatal(err)
	}
}
