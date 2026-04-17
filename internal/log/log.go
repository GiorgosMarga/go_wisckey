package log

import (
	"fmt"
	"os"
	"time"
)

type Log struct {
	f *os.File
}

func New() *Log {
	f, err := os.OpenFile(fmt.Sprintf("../../logs/", time.Now().UnixMilli()), os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		panic(err)
	}
	return &Log{
		f: f,
	}
}
