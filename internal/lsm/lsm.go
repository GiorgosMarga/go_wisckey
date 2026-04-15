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
	sparseindex "github.com/GiorgosMarga/wisckey/internal/sparseIndex"
)

var (
	h1 = bloomfilter.DefaultHash(0x9747b28c)
	h2 = bloomfilter.DefaultHash(0x85ebca6b)
	h3 = bloomfilter.DefaultHash(0xc2b2ae35)
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
		sstable.Close()
	}
	fmt.Println("Not found")
	return nil, ErrKeyNotFound
}

func (lsm *LSM) searchSSTable(sstable *os.File, targetKey []byte) (*MemtableEntry, error) {
	footer := make([]byte, 32)

	if _, err := sstable.Seek(-32, io.SeekEnd); err != nil {
		return nil, err
	}

	if _, err := sstable.Read(footer); err != nil {
		return nil, err
	}

	bfOffset := binary.LittleEndian.Uint64(footer)
	bfSize := binary.LittleEndian.Uint32(footer[8:])
	sparseIdxOffset := binary.LittleEndian.Uint64(footer[12:])
	sparseIdxSize := binary.LittleEndian.Uint32(footer[20:])
	magicNumber := binary.LittleEndian.Uint64(footer[24:])

	if magicNumber != 0xdeadbeefdeadbeef {
		return nil, fmt.Errorf("invalid sstable file")
	}

	bfBuf := make([]byte, bfSize)
	if _, err := sstable.ReadAt(bfBuf, int64(bfOffset)); err != nil {
		return nil, err
	}

	bf := bloomfilter.NewFromBuf(bfBuf, h1, h2, h3)

	if !bf.MayExist(targetKey) {
		return nil, ErrKeyNotFound
	}

	sparseIdxBuf := make([]byte, sparseIdxSize)
	if _, err := sstable.ReadAt(sparseIdxBuf, int64(sparseIdxOffset)); err != nil {
		return nil, err
	}
	spIdx := sparseindex.NewFromBuf(sparseIdxBuf)

	offset, err := spIdx.Get(targetKey[0])
	if err != nil {
		return nil, ErrKeyNotFound
	}
	for {
		// bfOffset is where the bloomfilter data starts and where the key-vlogId-vlogOffset data ends
		if offset >= bfOffset {
			return nil, ErrKeyNotFound
		}
		keyLenBuf := make([]byte, 8)
		_, err := sstable.ReadAt(keyLenBuf, int64(offset))
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, ErrKeyNotFound
			}
			return nil, err
		}
		offset += 8
		keyLen := binary.LittleEndian.Uint64(keyLenBuf)
		key := make([]byte, keyLen)
		_, err = sstable.ReadAt(key, int64(offset))
		if err != nil {
			return nil, err
		}
		offset += keyLen
		switch bytes.Compare(key, targetKey) {
		case 0:
			buf := make([]byte, 16)
			_, err = sstable.ReadAt(buf, int64(offset))
			if err != nil {
				return nil, err
			}
			return &MemtableEntry{
				Key:        targetKey,
				VLogId:     int64(binary.LittleEndian.Uint64(buf)),
				VLogOffset: int64(binary.LittleEndian.Uint64(buf[8:])),
			}, nil
		case -1:
			// skip entry
			offset += 16 // 8 vlogid + 8 vlog offset
		default:
			// key doesnt exist
			return nil, ErrKeyNotFound
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

	bf := bloomfilter.New(h1, h2, h3)
	spIndex := sparseindex.New()

	var offset int64 = 0
	// TODO: consider using a bufio writer
	for _, entry := range entries {
		bf.Insert(entry.Key)
		spIndex.Insert(entry.Key[0], uint64(offset))
		n, err := f.WriteAt(entry.Encode(), offset)
		if err != nil {
			return err
		}
		offset += int64(n)
	}
	// write footer
	bfOffset := offset
	bfB := bf.Encode()
	n, err := f.WriteAt(bfB, offset)
	if err != nil {
		return err
	}
	offset += int64(n)

	// write sparse index
	sparseOffset := offset
	spB := spIndex.Encode()
	n, err = f.WriteAt(spB, offset)
	if err != nil {
		return err
	}
	offset += int64(n)

	footer := make([]byte, 0, 32)

	// bloom filter metadata
	footer = binary.LittleEndian.AppendUint64(footer, uint64(bfOffset))
	footer = binary.LittleEndian.AppendUint32(footer, uint32(len(bfB)))

	// sparse index metadata
	footer = binary.LittleEndian.AppendUint64(footer, uint64(sparseOffset))
	footer = binary.LittleEndian.AppendUint32(footer, uint32(len(spB)))

	// magic number
	footer = binary.LittleEndian.AppendUint64(footer, uint64(0xdeadbeefdeadbeef))

	// write footer
	if _, err := f.WriteAt(footer, offset); err != nil {
		return err
	}
	return f.Sync()
}
