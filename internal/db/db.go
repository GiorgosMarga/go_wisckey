package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/GiorgosMarga/wisckey/internal/lsm"
	"github.com/GiorgosMarga/wisckey/internal/vlog"
)

type DB struct {
	lsm         *lsm.LSM
	vlogManager *vlog.Manager
}

func NewDB() *DB {
	if err := backupAndPrepareDir("../../sstables"); err != nil {
		panic(err)
	}

	db := &DB{
		vlogManager: vlog.NewManager(),
		lsm:         lsm.NewLSM(4 * 1024 * 1024),
	}
	if err := db.recoverFromCrash(); err != nil {
		panic(err)
	}
	go db.vlogManager.GC(db.lsm)
	return db
}

func (db *DB) Close() error {
	if err := db.lsm.Close(); err != nil {
		return err
	}

	return db.vlogManager.Close()
}

func (db *DB) recoverFromCrash() error {
	// check if vLogs exist

	vlogs, err := os.ReadDir("../../vlogs")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(vlogs) == 0 {
		return nil
	}

	fmt.Println("Recovering...")

	// need to iterate over len(vlogs) - 1, because the last one is the new one that was created with this NewDB()
	for i := 0; i < len(vlogs)-1; i++ {
		vlogEntry := vlogs[i]
		vlogId, _ := strconv.Atoi(vlogEntry.Name())
		vlog, err := vlog.NewVLog(int64(vlogId))
		if err != nil {
			return err
		}
		entries, err := vlog.Recover()
		if err != nil {
			return err
		}

		for _, entry := range entries {
			if err := db.Insert(entry.Key, entry.Val); err != nil {
				return err
			}
		}

		if err := vlog.Delete(); err != nil {
			return err
		}
	}
	return os.RemoveAll("../../old_sstables")
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

// backupAndPrepareDir renames dir -> old_<dir> and recreates dir
func backupAndPrepareDir(dir string) error {
	// Check if original dir exists
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			// If it doesn't exist, just create it
			return os.MkdirAll(dir, 0755)
		}
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", dir)
	}

	// Build backup name: parent/old_<basename>
	parent := filepath.Dir(dir)
	base := filepath.Base(dir)
	backupDir := filepath.Join(parent, "old_"+base)

	// Remove previous backup if it exists (optional behavior)
	if _, err := os.Stat(backupDir); err == nil {
		if err := os.RemoveAll(backupDir); err != nil {
			return fmt.Errorf("failed to remove existing backup: %w", err)
		}
	}

	// Rename original -> backup
	if err := os.Rename(dir, backupDir); err != nil {
		return fmt.Errorf("failed to rename dir: %w", err)
	}

	// Recreate fresh original directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		// Attempt rollback
		_ = os.Rename(backupDir, dir)
		return fmt.Errorf("failed to recreate dir, rollback attempted: %w", err)
	}

	return nil
}
