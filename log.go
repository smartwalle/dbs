package dbs

import (
	"log"
	"os"
)

func init() {
	SetLogger(log.New(os.Stdout, "", log.LstdFlags|log.Llongfile))
}

type Logger interface {
	SetPrefix(prefix string)
	Prefix() string
	Println(args ...interface{})
	Printf(format string, args ...interface{})
	Output(calldepth int, s string) error
}

var logger Logger

func SetLogger(l Logger) {
	if l != nil && l.Prefix() == "" {
		l.SetPrefix("[dbs] ")
	}
	logger = l
}
