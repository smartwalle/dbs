package logger

import (
	"context"
	"log"
	"runtime"
	"time"
)

func New() *Logger {
	return &Logger{}
}

type Logger struct {
}

func (Logger) Trace(ctx context.Context, depth int, begin time.Time, sql string, args []any, rowsAffected int64, err error) {
	var elapsedTime = time.Since(begin)

	_, file, line, _ := runtime.Caller(depth)

	if err != nil {
		log.Printf("File: %s:%d, SQL: %s, Args: %+v, ElapsedTime: %+v, Error: %+v \n", file, line, sql, args, elapsedTime, err)
	} else {
		log.Printf("File: %s:%d, SQL: %s, Args: %+v, ElapsedTime: %+v, Rows: %d \n", file, line, sql, args, elapsedTime, rowsAffected)
	}
}
