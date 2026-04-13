package vlog

type Manager struct {
	vlogs []*VLog
}

func NewManager(n int) *Manager {
	vlogs := make([]*VLog, 0, n)
	for id := range n {
		vlog, err := NewVLog(int64(id))
		if err != nil {
			panic(err)
		}
		vlogs = append(vlogs, vlog)
	}
	return &Manager{
		vlogs: vlogs,
	}
}

func (m *Manager) getVLogId(key []byte) int64 {
	var id int64 = 0
	for _, b := range key {
		id += int64(b)
	}
	return id % int64(len(m.vlogs))
}
func (m *Manager) Append(key, value []byte) (int64, int64, error) {
	vLogId := m.getVLogId(key)
	vlog := m.vlogs[vLogId]

	offset, err := vlog.Append(key, value)
	return int64(vLogId), offset, err
}
func (m *Manager) Read(vLogId, offset int64) ([]byte,error) {
	vlog := m.vlogs[vLogId]
	return vlog.Read(offset)
}
