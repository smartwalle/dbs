package dbs

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func Query[T any](ctx context.Context, session Session, query string, args ...interface{}) (dest T, err error) {
	var rowsAffected int
	var beginTime = time.Now()
	defer func() {
		GetLogger().Trace(ctx, beginTime, query, args, int64(rowsAffected), err)
	}()

	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return dest, err
	}
	defer rows.Close()

	if rowsAffected, err = GetMapper().Decode(rows, &dest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return dest, err
	}
	return dest, nil
}

func Exec(ctx context.Context, session Session, query string, args ...interface{}) (result sql.Result, err error) {
	var beginTime = time.Now()
	defer func() {
		var rowsAffected, _ = result.RowsAffected()
		GetLogger().Trace(ctx, beginTime, query, args, rowsAffected, err)
	}()
	return session.ExecContext(ctx, query, args...)
}
