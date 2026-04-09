package vlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"
	"time"
)

type VLog struct {
	f   *os.File
	mtx *sync.RWMutex

	id   int64
	head int64
}

func NewVLog() (*VLog, error) {
	id := time.Now().UnixMilli()
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

func (v *VLog) Append(data []byte) (int64, error) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	buf := make([]byte, 2+len(data))
	binary.LittleEndian.PutUint16(buf, uint16(len(data)))
	copy(buf, data)

	offset := v.head
	if _, err := v.f.WriteAt(buf, v.head); err != nil {
		return 0, err
	}

	v.head += int64(len(buf))
	return offset, nil
}
func (v *VLog) Read(offset int64) ([]byte, error) {
	buf := make([]byte, 2)
	n, err := v.f.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	var dataLen uint16
	binary.LittleEndian.Uint16(buf[:n])
	data := make([]byte, dataLen)

	n, err = v.f.ReadAt(data, offset+2)
	if err != nil {
		return nil, err
	}

	if n != int(dataLen) {
		return nil, fmt.Errorf("could not read the entire data")
	}

	return data, nil
}
