package lsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"time"
)

type LSM struct {
	memtable Memtable
	maxSize  int
}

func NewLSM(maxSize int) *LSM {
	return &LSM{
		memtable: NewAVL(),
		maxSize:  maxSize,
	}
}

// TODO: writeSSTable should not block the insert. A new memtable should be available for writing the data
func (lsm *LSM) Insert(key []byte, id, offset int64) error {
	// check if key fits in the lsm
	if lsm.memtable.Size()+len(key) > lsm.maxSize {
		// need flush current lsm in disk and create a new one
		if err := lsm.writeSSTable(); err != nil {
			return err
		}
		lsm.memtable = NewAVL()
	}

	entry := MemtableEntry{
		Key:        key,
		VLogId:     id,
		VLogOffset: offset,
	}
	if err := lsm.memtable.Insert(entry); err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (MemtableEntry, error) {
	return lsm.memtable.Read(key)
}

func (lsm *LSM) writeSSTable() error {
	filename := path.Join("sstables", fmt.Sprintf("%d", time.Now().UnixMicro()))

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()
	entries := lsm.memtable.GetEntries()

	for _, entry := range entries {
		entryBuf := entry.Encode()
		buf := make([]byte, 2+len(entryBuf))
		binary.LittleEndian.PutUint16(buf, uint16(len(entryBuf)))
		copy(buf[2:], entryBuf)
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	return f.Sync()
}
