package bloomfilter

import (
	"fmt"
	"testing"
)

func TestHash3(t *testing.T) {
	h1 := DefaultHash(0x9747b28c)
	h2 := DefaultHash(0x85ebca6b)
	h3 := DefaultHash(0xc2b2ae35)

	bf := NewBloomFilter(h1, h2, h3)

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
