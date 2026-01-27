package dbs

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func Query[T any](ctx context.Context, session Session, query string, args ...any) (dest T, err error) {
	var rowsAffected int
	var logger = session.Logger()
	if logger != nil {
		var beginTime = time.Now()
		defer func() {
			session.Logger().Trace(ctx, 3, beginTime, query, args, int64(rowsAffected), err)
		}()
	}

	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return dest, err
	}
	defer rows.Close()

	if rowsAffected, err = session.Mapper().Decode(rows, &dest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return dest, err
	}
	return dest, nil
}

func Exec(ctx context.Context, session Session, query string, args ...any) (result sql.Result, err error) {
	var logger = session.Logger()
	if logger != nil {
		var beginTime = time.Now()
		defer func() {
			var rowsAffected int64
			if result != nil {
				rowsAffected, _ = result.RowsAffected()
			}
			session.Logger().Trace(ctx, 3, beginTime, query, args, rowsAffected, err)
		}()
	}
	return session.ExecContext(ctx, query, args...)
}
