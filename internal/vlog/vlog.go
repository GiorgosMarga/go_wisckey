package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"sync"
)

var (
	ErrInvalidCRC = errors.New("invalid crc")
)

type vlogEntry struct {
	key []byte
	val []byte
}

type VLog struct {
	f   *os.File
	mtx *sync.RWMutex

	id   int64
	tail int64
	head int64
}

func NewVLog(id int64) (*VLog, error) {
	filepath := path.Join("../../", "vlogs", fmt.Sprintf("%d", id))

	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	return &VLog{
		f:    f,
		mtx:  &sync.RWMutex{},
		id:   id,
		head: 0,
	}, nil
}

func (v *VLog) Append(key, value []byte) (int64, error) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	crc := crc32.ChecksumIEEE(append(key, value...))

	buf := make([]byte, 8+len(key)+len(value)) // keysize 2 + valsize 2 + crc 4
	offset := 0
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(key)))
	offset += 2
	copy(buf[offset:], key)
	offset += len(key)
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(value)))
	offset += 2
	copy(buf[offset:], value)
	offset += len(value)
	binary.LittleEndian.PutUint32(buf[offset:], crc)

	vlogOffset := v.head
	if _, err := v.f.WriteAt(buf, v.head); err != nil {
		return 0, err
	}

	v.head += int64(len(buf))
	return vlogOffset, nil
}
func (v *VLog) Read(vLogOffset int64) ([]byte, error) {
	entry, err := v.readEntry(vLogOffset)
	if err != nil {
		return nil, err
	}
	return entry.val, nil
}

func (v *VLog) readEntry(vLogOffset int64) (*vlogEntry, error) {
	buf := make([]byte, 2)

	// read key
	var readOffset int64 = 0
	n, err := v.f.ReadAt(buf, vLogOffset+readOffset)
	if err != nil {
		return nil, err
	}
	readOffset += int64(n)
	keyLen := binary.LittleEndian.Uint16(buf[:n])
	key := make([]byte, keyLen)

	n, err = v.f.ReadAt(key, vLogOffset+readOffset)
	if err != nil {
		return nil, err
	}
	readOffset += int64(n)
	if n != int(keyLen) {
		return nil, fmt.Errorf("could not read the entire data")
	}

	// read value
	n, err = v.f.ReadAt(buf, vLogOffset+readOffset)
	if err != nil {
		return nil, err
	}
	readOffset += int64(n)
	valueLen := binary.LittleEndian.Uint16(buf[:n])
	value := make([]byte, valueLen)

	n, err = v.f.ReadAt(value, vLogOffset+readOffset)
	if err != nil {
		return nil, err
	}
	readOffset += int64(n)

	// read crc
	crcBuf := make([]byte, 4)
	_, err = v.f.ReadAt(crcBuf, vLogOffset+readOffset)
	crc := binary.LittleEndian.Uint32(crcBuf)
	if err != nil {
		return nil, err
	}

	if crc != crc32.ChecksumIEEE(append(key, value...)) {
		return nil, ErrInvalidCRC
	}
	return &vlogEntry{
		key: key,
		val: value,
	}, nil
}
