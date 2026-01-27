package dbs

import (
	"context"
	"time"
)

type Logger interface {
	Trace(ctx context.Context, skip int, begin time.Time, sql string, args []any, rowsAffected int64, err error)
}
