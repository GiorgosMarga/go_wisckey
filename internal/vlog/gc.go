package vlog

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/GiorgosMarga/wisckey/internal/lsm"
)

func (m *Manager) GC(l *lsm.LSM) {
	for range time.Tick(5 * time.Second) {
		if m.minVLogId != math.MaxInt64 && m.minVLogId != m.vlogId {
			oldestVlog, exists := m.immutable[m.minVLogId]
			if !exists {
				fmt.Printf("[GC]: vLog with id %d doesnt exist\n", m.minVLogId)
				continue
			}
			if err := oldestVlog.gc(l); err != nil {
				fmt.Println(err)
				continue
			}
			m.mtx.Lock()
			m.minVLogId += 1
			m.mtx.Unlock()
		}
	}

}

func (v *VLog) gc(l *lsm.LSM) error {
	var offset int64 = 0
	for {
		entry, err := v.readEntry(offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		lsmEntry, err := l.Get(entry.Key)
		if err != nil {
			return err
		}

		if lsmEntry.VLogOffset != offset {
			fmt.Println("entry should be deleted")
		}

		offset += 8 + int64(len(entry.Key)+len(entry.Val))
	}
}
