package log

import (
	"context"
	"log"
	"time"
)

func New() *Logger {
	return &Logger{}
}

type Logger struct {
}

func (Logger) Trace(ctx context.Context, begin time.Time, sql string, args []interface{}, rowsAffected int64, err error) {
	var elapsedTime = time.Since(begin)

	if err != nil {
		log.Printf("SQL: %s, Args: %+v, ElapsedTime: %+v, Error: %+v \n", sql, args, elapsedTime, err)
	} else {
		log.Printf("SQL: %s, Args: %+v, ElapsedTime: %+v, Rows: %d \n", sql, args, elapsedTime, rowsAffected)
	}
}
