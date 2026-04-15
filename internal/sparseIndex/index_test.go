package sparseindex

import (
	"testing"
)

func TestInsertSameByte(t *testing.T) {
	sp := New()
	sp.Insert('a', 0xdeadbeef)
	sp.Insert('a', 10)
	sp.Insert('a', 15)
	offset, err := sp.Get('a')
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0xdeadbeef {
		t.Fatalf("wrong offset")
	}
}
func TestEncode(t *testing.T) {
	sp := New()
	sp.Insert('a', 0xdeadbeef)
	sp.Insert('b', 0xdeadcafe)
	sp.Insert('a', 10)
	sp.Insert('a', 15)
	buf := sp.Encode()
	newSp := NewFromBuf(buf)

	offset, err := newSp.Get('a')
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0xdeadbeef {
		t.Fatalf("wrong offset")
	}

	offset, err = newSp.Get('b')
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0xdeadcafe {
		t.Fatalf("wrong offset")
	}
}
