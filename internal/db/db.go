package db

import (
	"github.com/GiorgosMarga/wisckey/internal/lsm"
	"github.com/GiorgosMarga/wisckey/internal/vlog"
)

type DB struct {
	lsm  *lsm.LSM
	vlog *vlog.VLog
}

func NewDB() (*DB, error) {
	v, err := vlog.NewVLog()
	if err != nil {
		return nil, err
	}
	return &DB{
		vlog: v,
		lsm:  lsm.NewLSM(4096),
	}, nil
}

func (db *DB) Insert(key, value []byte) error {
	offset, err := db.vlog.Append(key, value)
	if err != nil {
		return err
	}

	return db.lsm.Insert(key, 0, offset)
}
func (db *DB) Read(key []byte) ([]byte, error) {
	entry, err := db.lsm.Get(key)
	if err != nil {
		return nil, err
	}
	return db.vlog.Read(entry.VLogOffset)
}
