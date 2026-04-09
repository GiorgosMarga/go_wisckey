package lsm

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestInsert(t *testing.T) {
	a := NewAVL()

	for range 10 {
		k := rand.Intn(899)
		if err := a.Insert(MemtableEntry{Key: fmt.Appendf(nil, "%03d", 100+k), VLogId: 0, VLogOffset: 0}); err != nil {
			t.Fatal(err)
		}
	}
	tree := a.(*AVL)
	tree.InOrder()
	tree.PreOrder()
}
