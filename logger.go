package dbs

import (
	"context"
	"log"
	"runtime"
	"time"
)

type Logger interface {
	Trace(ctx context.Context, skip int, begin time.Time, sql string, args []interface{}, rowsAffected int64, err error)
}

func NewLogger() *logger {
	return &logger{}
}

type logger struct {
}

func (logger) Trace(ctx context.Context, skip int, begin time.Time, sql string, args []interface{}, rowsAffected int64, err error) {
	var elapsedTime = time.Since(begin)

	_, file, line, _ := runtime.Caller(skip)

	if err != nil {
		log.Printf("File: %s:%d, SQL: %s, Args: %+v, ElapsedTime: %+v, Error: %+v \n", file, line, sql, args, elapsedTime, err)
	} else {
		log.Printf("File: %s:%d, SQL: %s, Args: %+v, ElapsedTime: %+v, Rows: %d \n", file, line, sql, args, elapsedTime, rowsAffected)
	}
}
