package log

import (
	"fmt"
	"os"
	"time"
)

type Log struct {
	f    *os.File
	path string
}

func New() *Log {
	var (
		l   = &Log{}
		err error
	)
	l.path = fmt.Sprintf("../../logs/%d", time.Now().UnixMilli())
	l.f, err = os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	return l
}

func (l *Log) Write(data []byte) error {
	fmt.Println("writing...")
	if _, err := l.f.Write(data); err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *Log) Delete() error {
	if err := l.f.Close(); err != nil {
		return err
	}
	return os.Remove(l.path)
}
