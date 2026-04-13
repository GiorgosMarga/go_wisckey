package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/GiorgosMarga/wisckey/internal/bloomfilter"
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

func (lsm *LSM) Get(key []byte) (*MemtableEntry, error) {
	entry, err := lsm.memtable.Read(key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
	}
	if entry != nil {
		return entry, nil
	}

	// search in sstables

	sstables, err := os.ReadDir("../../sstables")
	if err != nil {
		return nil, err
	}

	for _, sstableFile := range sstables {
		sstable, err := os.Open(fmt.Sprintf("../../sstables/%s", sstableFile.Name()))
		if err != nil {
			return nil, err
		}
		entry, err := lsm.searchSSTable(sstable, key)
		if err != nil {
			if !errors.Is(err, ErrKeyNotFound) {
				return nil, err
			}
		}
		if entry != nil {
			return entry, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (lsm *LSM) searchSSTable(sstable *os.File, targetKey []byte) (*MemtableEntry, error) {
	var offset int64 = 0
	for {
		keyLenBuf := make([]byte, 8)
		_, err := sstable.ReadAt(keyLenBuf, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, ErrKeyNotFound
			}
			return nil, err
		}
		offset += 8
		keyLen := binary.LittleEndian.Uint64(keyLenBuf)
		key := make([]byte, keyLen)
		_, err = sstable.ReadAt(key, offset)
		if err != nil {
			return nil, err
		}
		offset += int64(keyLen)
		if bytes.Equal(key, targetKey) {
			buf := make([]byte, 16)
			_, err = sstable.ReadAt(buf, offset)
			if err != nil {
				return nil, err
			}
			return &MemtableEntry{
				Key:        targetKey,
				VLogId:     int64(binary.LittleEndian.Uint64(buf)),
				VLogOffset: int64(binary.LittleEndian.Uint64(buf[8:])),
			}, nil
		} else {
			// skip entry
			offset += 16 // 8 vlogid + 8 vlog offset
		}
	}
}

func (lsm *LSM) writeSSTable() error {
	filename := path.Join(
		"..", "..", "sstables", fmt.Sprintf("%d", time.Now().UnixMicro()))

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()
	entries := lsm.memtable.GetEntries()

	bf := bloomfilter.NewBloomFilter(bloomfilter.DefaultHash(0x9747b28c), bloomfilter.DefaultHash(0x85ebca6b), bloomfilter.DefaultHash(0xc2b2ae35))

	for _, entry := range entries {
		bf.Insert(entry.Key)
		entryBuf := entry.Encode()
		if _, err := f.Write(entryBuf); err != nil {
			return err
		}
	}
	// write footer
	bfOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	encodedFilter := bf.Encode()
	if _, err := f.WriteAt(encodedFilter, bfOffset); err != nil {
		return err
	}

	// implement sparse index

	return f.Sync()
}
