package sparseindex

import (
	"encoding/binary"
	"fmt"
)

type SparseIndex struct {
	pos map[byte]uint64
}

func New() *SparseIndex {
	return &SparseIndex{
		pos: make(map[byte]uint64),
	}
}
func NewFromBuf(buf []byte) *SparseIndex {
	sp := &SparseIndex{
		pos: make(map[byte]uint64),
	}
	totalItems := len(buf) / 9
	for i := range totalItems {
		key := buf[i*9]
		value := binary.LittleEndian.Uint64(buf[(i*9)+1:])
		sp.pos[key] = value
	}
	return sp
}
func (sp *SparseIndex) Insert(k byte, offset uint64) {
	if _, exists := sp.pos[k]; exists {
		return
	}
	sp.pos[k] = offset
}
func (sp *SparseIndex) Get(k byte) (uint64, error) {
	if _, exists := sp.pos[k]; !exists {
		return 0, fmt.Errorf("key doesnt exist")
	}
	return sp.pos[k], nil
}

func (sp *SparseIndex) Encode() []byte {
	bufSize := len(sp.pos) * 9 // 1byte for key and 8 bytes for value
	buf := make([]byte, bufSize)
	offset := 0
	for k, v := range sp.pos {
		buf[offset] = k
		offset += 1
		binary.LittleEndian.PutUint64(buf[offset:], uint64(v))
		offset += 8
	}
	return buf
}

func (sp *SparseIndex) Print() {
	for k, v := range sp.pos {
		fmt.Printf("%s -> %d\n", string(k), v)
	}
}
