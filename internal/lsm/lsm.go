package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/GiorgosMarga/wisckey/internal/bloomfilter"
	sparseindex "github.com/GiorgosMarga/wisckey/internal/sparseIndex"
)

var (
	h1 = bloomfilter.DefaultHash(0x9747b28c)
	h2 = bloomfilter.DefaultHash(0x85ebca6b)
	h3 = bloomfilter.DefaultHash(0xc2b2ae35)
)

type SSTable struct {
	f             *os.File
	bloomFilter   *bloomfilter.BloomFilter
	sparseIdx     *sparseindex.SparseIndex
	dataEndOffset uint64
}

type LSM struct {
	mtx               *sync.Mutex
	activeMemtable    Memtable
	immutableMemtable Memtable
	sstablesCache     map[string]*SSTable
	flushCh           chan struct{}
	flushCond         *sync.Cond
	maxSize           int
}

func NewLSM(maxSize int) *LSM {
	mtx := &sync.Mutex{}
	l := &LSM{
		activeMemtable:    NewAVL(),
		immutableMemtable: nil,
		maxSize:           maxSize,
		sstablesCache:     make(map[string]*SSTable),
		flushCh:           make(chan struct{}, 1),
		mtx:               mtx,
		flushCond:         sync.NewCond(mtx),
	}
	go l.flushLoop()
	return l
}

func (lsm *LSM) flushLoop() {
	for range lsm.flushCh {
		if err := lsm.writeSSTable(); err != nil {
			fmt.Println(err)
			continue
		}
		lsm.mtx.Lock()
		lsm.immutableMemtable = nil
		lsm.flushCond.Signal()
		lsm.mtx.Unlock()
	}
}

// TODO: writeSSTable should not block the insert. A new memtable should be available for writing the data
func (lsm *LSM) Insert(key []byte, id, offset int64) error {
	// check if key fits in the lsm
	if lsm.activeMemtable.Size()+len(key) > lsm.maxSize {
		// need flush current lsm in disk and create a new one
		lsm.mtx.Lock()
		for lsm.immutableMemtable != nil {
			lsm.flushCond.Wait()
		}
		lsm.immutableMemtable = lsm.activeMemtable
		lsm.activeMemtable = NewAVL()
		lsm.mtx.Unlock()

		lsm.flushCh <- struct{}{}

	}

	entry := MemtableEntry{
		Key:        key,
		VLogId:     id,
		VLogOffset: offset,
	}
	if err := lsm.activeMemtable.Insert(entry); err != nil {
		return err
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*MemtableEntry, error) {
	// search active
	entry, err := lsm.activeMemtable.Read(key)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
	}
	if entry != nil {
		return entry, nil
	}

	// search immutable
	lsm.mtx.Lock()
	if lsm.immutableMemtable != nil {
		entry, err := lsm.immutableMemtable.Read(key)
		if err != nil {
			if !errors.Is(err, ErrKeyNotFound) {
				lsm.mtx.Unlock()
				return nil, err
			}
		}
		if entry != nil {
			lsm.mtx.Unlock()
			return entry, nil
		}
	}
	lsm.mtx.Unlock()

	// search in sstables

	sstables, err := os.ReadDir("../../sstables")
	if err != nil {
		return nil, err
	}

	var (
		sstable *SSTable
		exists  bool
	)

	for _, sstableFile := range sstables {
		// first seatch the cache
		sstable, exists = lsm.sstablesCache[sstableFile.Name()]
		if !exists {
			sstable, err = lsm.openSSTable(fmt.Sprintf("../../sstables/%s", sstableFile.Name()))
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

func (lsm *LSM) openSSTable(filename string) (*SSTable, error) {
	sstable, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

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

	sparseIdxBuf := make([]byte, sparseIdxSize)
	if _, err := sstable.ReadAt(sparseIdxBuf, int64(sparseIdxOffset)); err != nil {
		return nil, err
	}

	spIdx := sparseindex.NewFromBuf(sparseIdxBuf)

	return &SSTable{
		f:             sstable,
		bloomFilter:   bf,
		sparseIdx:     spIdx,
		dataEndOffset: bfOffset,
	}, nil
}

func (lsm *LSM) searchSSTable(sstable *SSTable, targetKey []byte) (*MemtableEntry, error) {
	if !sstable.bloomFilter.MayExist(targetKey) {
		return nil, ErrKeyNotFound
	}

	offset, err := sstable.sparseIdx.Get(targetKey[0])
	if err != nil {
		return nil, ErrKeyNotFound
	}
	for {
		// bfOffset is where the bloomfilter data starts and where the key-vlogId-vlogOffset data ends
		if offset >= sstable.dataEndOffset {
			return nil, ErrKeyNotFound
		}
		keyLenBuf := make([]byte, 8)
		_, err := sstable.f.ReadAt(keyLenBuf, int64(offset))
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, ErrKeyNotFound
			}
			return nil, err
		}
		offset += 8
		keyLen := binary.LittleEndian.Uint64(keyLenBuf)
		key := make([]byte, keyLen)
		_, err = sstable.f.ReadAt(key, int64(offset))
		if err != nil {
			return nil, err
		}
		offset += keyLen
		switch bytes.Compare(key, targetKey) {
		case 0:
			buf := make([]byte, 16)
			_, err = sstable.f.ReadAt(buf, int64(offset))
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
	id := fmt.Sprintf("%d", time.Now().UnixMicro())
	filename := path.Join(
		"..", "..", "sstables", id)

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return err
	}
	entries := lsm.immutableMemtable.GetEntries()

	bf := bloomfilter.New(h1, h2, h3)
	spIndex := sparseindex.New()

	var offset int64 = 0
	// TODO: consider using a bufio writer
	for _, entry := range entries {
		bf.Insert(entry.Key)
		spIndex.Insert(entry.Key[0], uint64(offset))
		n, err := f.Write(entry.Encode())
		if err != nil {
			return err
		}
		offset += int64(n)
	}
	// write footer
	bfOffset := offset
	bfB := bf.Encode()
	n, err := f.Write(bfB)
	if err != nil {
		return err
	}
	offset += int64(n)

	// write sparse index
	sparseOffset := offset
	spB := spIndex.Encode()
	n, err = f.Write(spB)
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
	if _, err := f.Write(footer); err != nil {
		return err
	}

	lsm.sstablesCache[id] = &SSTable{
		f:             f,
		bloomFilter:   bf,
		sparseIdx:     spIndex,
		dataEndOffset: uint64(bfOffset),
	}
	return f.Sync()
}
