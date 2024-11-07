package dbs

import (
	"context"
	"database/sql"
)

var gScanner Scanner = NewScanner(kTag)

func UseScanner(s Scanner) {
	if s != nil {
		gScanner = s
	}
}

func Scan[T any](rows *sql.Rows) (dst T, err error) {
	err = gScanner.Scan(rows, &dst)
	return dst, err
}

func Query[T any](ctx context.Context, session Session, query string, args ...interface{}) (dst T, err error) {
	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return dst, err
	}
	defer rows.Close()

	err = gScanner.Scan(rows, &dst)
	return dst, err
}

func Exec(ctx context.Context, session Session, query string, args ...interface{}) (result sql.Result, err error) {
	return session.ExecContext(ctx, query, args...)
}
