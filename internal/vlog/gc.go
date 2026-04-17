package vlog

import (
	"fmt"
	"sync"
	"time"

	"github.com/GiorgosMarga/wisckey/internal/lsm"
)

func (m *Manager) GC(l *lsm.LSM) {
	wg := sync.WaitGroup{}
	for range time.After(1 * time.Second) {
		fmt.Println("GC running...")
		for _, vlog := range m.immutable {
			wg.Go(func() {
				if err := vlog.gc(l); err != nil {
					fmt.Printf("error cleaning immutable vlog %d: %s\n", vlog.id, err)
				}
			})
		}
		wg.Wait()
		fmt.Println("GC finished...")
	}

}

func (v *VLog) gc(l *lsm.LSM) error {
	for v.tail < v.head {
		entry, err := v.readEntry(v.tail)
		if err != nil {
			return err
		}
		lsmEntry, err := l.Get(entry.key)
		if err != nil {
			return err
		}

		if lsmEntry.VLogOffset != v.tail {
			fmt.Println("entry should be deleted")
		}

		v.tail += 8 + int64(len(entry.key)+len(entry.val))
	}
	fmt.Println("finished ", v.id)
	return nil
}
