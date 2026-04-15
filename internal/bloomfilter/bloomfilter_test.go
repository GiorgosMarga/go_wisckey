package bloomfilter

import (
	"fmt"
	"testing"
)

func TestHash3(t *testing.T) {
	h1 := DefaultHash(0x9747b28c)
	h2 := DefaultHash(0x85ebca6b)
	h3 := DefaultHash(0xc2b2ae35)

	bf := New(h1, h2, h3)

	bf.Insert([]byte("Hello world"))
	bf.Insert([]byte("Hello world1"))
	bf.Insert([]byte("Hello world2"))
	bf.Insert([]byte("Hello world3"))
	bf.Insert([]byte("Hello world4"))
	bf.Insert([]byte("Hello world5"))
	fmt.Println(bf.MayExist([]byte("Hello world")))
	fmt.Println(bf.MayExist([]byte("Hello world1")))
	fmt.Println(bf.MayExist([]byte("Hello world2")))
	fmt.Println(bf.MayExist([]byte("Hello world3")))
	fmt.Println(bf.MayExist([]byte("Hello world4")))
	fmt.Println(bf.MayExist([]byte("Hello world5")))
	fmt.Println(bf.MayExist([]byte("Hello world6")))
}

func TestEncode(t *testing.T) {
	h1 := DefaultHash(0x9747b28c)
	h2 := DefaultHash(0x85ebca6b)
	h3 := DefaultHash(0xc2b2ae35)

	bf := New(h1, h2, h3)

	bf.Insert([]byte("Hello world"))
	bf.Insert([]byte("Hello world1"))
	bf.Insert([]byte("Hello world2"))
	bf.Insert([]byte("Hello world3"))
	bf.Insert([]byte("Hello world4"))
	bf.Insert([]byte("Hello world5"))
	bf.Insert([]byte("Hello world6"))
	bf.Insert([]byte("Hello world7"))
	bf.Insert([]byte("Hello world8"))
	bf.Insert([]byte("Hello world9"))

	buf := bf.Encode()

	decodedBf := NewFromBuf(buf, h1, h2, h3)

	if decodedBf.MayExist([]byte("Hello world")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world2")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world3")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world4")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world5")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world6")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world7")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world8")) == false {
		t.Fatal()
	}
	if decodedBf.MayExist([]byte("Hello world9")) == false {
		t.Fatal()
	}
}
