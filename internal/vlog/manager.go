package vlog

import (
	"fmt"
	"math"
	"os"
	"sync"
)

type Manager struct {
	maxCapacity int64
	vlogId      int64
	minVLogId   int64 // the vlog id that will be GCed in the GC round
	active      *VLog
	mtx         *sync.Mutex
	immutable   map[int64]*VLog // GC can clear these if all entries are deleted
}

func NewManager() *Manager {
	dir, err := os.ReadDir("../../vlogs")
	if err != nil {
		panic(err)
	}
	// TODO: each vlog should become immutable when it reaches a byte threshold.
	manager := &Manager{
		maxCapacity: 4 * 1024 * 1024, // 4MB
		vlogId:      int64(len(dir)),
		immutable:   make(map[int64]*VLog),
		mtx:         &sync.Mutex{},
		minVLogId:   math.MaxInt64,
	}

	activeVlog, err := NewVLog(manager.vlogId)
	if err != nil {
		panic(err)
	}
	manager.active = activeVlog
	return manager
}

func (m *Manager) Append(key, value []byte) (int64, int64, error) {
	entrySize := len(key) + len(value) + 8 // 2 keysize + 2 valsize + 4 crc
	// the new entry doesnt fit, create a new active segment and make the current
	// immutable
	if m.active.size+int64(entrySize) > m.maxCapacity {
		m.mtx.Lock()
		m.immutable[m.vlogId] = m.active
		m.active, _ = NewVLog(m.vlogId + 1)
		m.minVLogId = min(m.minVLogId, m.vlogId)
		m.vlogId++
		m.mtx.Unlock()
	}

	offset, err := m.active.Append(key, value)
	return m.vlogId, offset, err
}

func (m *Manager) Read(vLogId, offset int64) ([]byte, error) {
	if vLogId == m.vlogId {
		return m.active.Read(offset)
	}
	vlog, exists := m.immutable[vLogId]
	if !exists {
		return nil, fmt.Errorf("invalid vlog id")
	}
	return vlog.Read(offset)
}
