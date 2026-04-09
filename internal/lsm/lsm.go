package lsm

type LSM struct {
	memtable Memtable
	currSize int
	maxSize  int
}

func NewLSM(maxSize int) *LSM {
	return &LSM{
		memtable: NewAVL(),
		maxSize:  maxSize,
	}
}

func (lsm *LSM) Insert(key []byte, id, offset int) error {
	// check if key fits in the lsm
	if lsm.currSize+len(key) > lsm.maxSize {
		// need flush current lsm in disk and create a new one
	}
	if err := lsm.Insert(key, id, offset); err != nil {
		return err
	}
	lsm.currSize += len(key)
	return nil
}

func (lsm *LSM) Get(key []byte) (MemtableEntry, error) {
	return lsm.memtable.Read(key)
}
