package db

import (
	"github.com/GiorgosMarga/wisckey/internal/lsm"
	"github.com/GiorgosMarga/wisckey/internal/vlog"
)

type DB struct {
	lsm         *lsm.LSM
	vlogManager *vlog.Manager
}

func NewDB() *DB {
	return &DB{
		vlogManager: vlog.NewManager(10),
		lsm:         lsm.NewLSM(8192),
	}
}

func (db *DB) Insert(key, value []byte) error {
	vLogId, offset, err := db.vlogManager.Append(key, value)
	if err != nil {
		return err
	}

	return db.lsm.Insert(key, vLogId, offset)
}
func (db *DB) Read(key []byte) ([]byte, error) {
	entry, err := db.lsm.Get(key)
	if err != nil {
		return nil, err
	}
	return db.vlogManager.Read(entry.VLogId, entry.VLogOffset)
}
