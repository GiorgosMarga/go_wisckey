package bloomfilter

import (
	"encoding/binary"
)

type HashFunc func([]byte) uint32

type BloomFilter struct {
	hashFuncs []HashFunc
	filter    int64
}

func NewBloomFilter(hashFuncs ...HashFunc) *BloomFilter {
	funcs := make([]HashFunc, 0, len(hashFuncs))
	funcs = append(funcs, hashFuncs...)
	for i := len(funcs); i < 3; i++ {
		funcs = append(funcs, DefaultHash(uint32(i)))
	}
	return &BloomFilter{
		hashFuncs: funcs,
	}
}

func (bf *BloomFilter) Insert(k []byte) {
	for _, fn := range bf.hashFuncs {
		pos := fn(k) % 64
		bf.filter |= 1 << int64(pos)
	}
}
func (bf *BloomFilter) MayExist(k []byte) bool {
	var res int64 = 1
	for _, fn := range bf.hashFuncs {
		pos := fn(k) % 64
		res &= (bf.filter & (1 << int64(pos))) >> pos
	}
	return res == 1
}
func DefaultHash(seed uint32) HashFunc {
	return func(b []byte) uint32 {
		return MurMur3(b, seed)
	}
}

// https://en.wikipedia.org/wiki/MurmurHash
func MurMur3(key []byte, seed uint32) uint32 {
	var (
		c1 uint32 = 0xcc9e2d51
		c2 uint32 = 0x1b873593
		r1 uint   = 15
		r2 uint   = 13
		m  uint32 = 5
		n  uint32 = 0xe6546b64
	)
	hash := seed
	chunks := len(key) / 4
	for i := range chunks {
		k := binary.LittleEndian.Uint32(key[i*4 : i*4+4])
		k *= c1
		k = rol32(k, r1)
		k *= c2
		hash = hash ^ k
		hash = rol32(uint32(hash), r2)
		hash = (hash * m) + n
	}
	remainingBytes := key[chunks*4:]
	var b uint32
	switch len(remainingBytes) {
	case 3:
		b |= uint32(remainingBytes[2]) << 16
		fallthrough
	case 2:
		b |= uint32(remainingBytes[1]) << 8
		fallthrough
	case 1:
		b |= uint32(remainingBytes[0])
		b *= c1
		b = rol32(b, r1)
		b *= c2
		hash ^= b
	}
	hash ^= uint32(len(key))
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16
	return hash
}

func rol32(x uint32, n uint) uint32 {
	return (x << n) | (x >> (32 - n))
}
