package dbs

import (
	"io"
	l "log"
)

var logger *l.Logger

func SetLogWriter(w io.Writer) {
	if logger != nil {
		logger.SetOutput(w)
		return
	}
	logger = l.New(w, "[DBS]", l.Ldate|l.Ltime)
}

func log(args ...interface{}) {
	if logger != nil {
		logger.Println(args...)
	}
}
