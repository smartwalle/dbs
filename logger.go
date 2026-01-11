package dbs

import (
	"context"
	"github.com/smartwalle/dbs/log"
	"time"
)

var _logger Logger = log.New()

func UseLogger(logger Logger) {
	if logger == nil {
		_logger = logger
	}
}

func GetLogger() Logger {
	return _logger
}

type Logger interface {
	Trace(ctx context.Context, skip int, begin time.Time, sql string, args []interface{}, rowsAffected int64, err error)
}
