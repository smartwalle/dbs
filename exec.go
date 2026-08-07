package dbs

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const (
	kDefaultTraceDepth = 3
)

func Query[T any](ctx context.Context, session Session, query string, args ...any) (dest T, err error) {
	var rowsAffected int
	var logger = session.Logger()
	if logger != nil {
		var beginTime = time.Now()
		defer func() {
			logger.Trace(ctx, kDefaultTraceDepth, beginTime, query, args, int64(rowsAffected), err)
		}()
	}

	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return dest, err
	}
	defer func() {
		_ = rows.Close()
	}()

	if rowsAffected, err = session.Mapper().Decode(rows, &dest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return dest, err
	}
	return dest, nil
}

func Exec(ctx context.Context, session Session, query string, args ...any) (result Result, err error) {
	var logger = session.Logger()
	if logger != nil {
		var beginTime = time.Now()
		defer func() {
			var rowsAffected int64
			if result != nil {
				rowsAffected, _ = result.RowsAffected()
			}
			logger.Trace(ctx, kDefaultTraceDepth, beginTime, query, args, rowsAffected, err)
		}()
	}
	return session.ExecContext(ctx, query, args...)
}

type TxOptions = sql.TxOptions

type IsolationLevel = sql.IsolationLevel

const (
	LevelDefault         = sql.LevelDefault
	LevelReadUncommitted = sql.LevelReadUncommitted
	LevelReadCommitted   = sql.LevelReadCommitted
	LevelWriteCommitted  = sql.LevelWriteCommitted
	LevelRepeatableRead  = sql.LevelRepeatableRead
	LevelSnapshot        = sql.LevelSnapshot
	LevelSerializable    = sql.LevelSerializable
	LevelLinearizable    = sql.LevelLinearizable
)

type Result = sql.Result
