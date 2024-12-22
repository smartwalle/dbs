package dbs

import (
	"context"
	"database/sql"
	"errors"
)

var gScanner Scanner = NewScanner(kTag)

func UseScanner(s Scanner) {
	if s != nil {
		gScanner = s
	}
}

func Scan[T any](rows *sql.Rows) (dst T, err error) {
	if err = gScanner.Scan(rows, &dst); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return dst, err
	}
	return dst, nil
}

func Query[T any](ctx context.Context, session Session, query string, args ...interface{}) (dst T, err error) {
	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return dst, err
	}
	defer rows.Close()

	if err = gScanner.Scan(rows, &dst); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return dst, err
	}
	return dst, nil
}

func Exec(ctx context.Context, session Session, query string, args ...interface{}) (result sql.Result, err error) {
	return session.ExecContext(ctx, query, args...)
}

func find(ctx context.Context, session Session, clause SQLClause, dst interface{}) error {
	rows, err := query(ctx, session, clause)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err = gScanner.Scan(rows, dst); err != nil && !errors.Is(err, ErrNoRows) {
		return err
	}
	return nil
}

func query(ctx context.Context, session Session, clause SQLClause) (*sql.Rows, error) {
	sql, args, err := clause.SQL()
	if err != nil {
		return nil, err
	}

	rows, err := session.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func exec(ctx context.Context, session Session, clause SQLClause) (result sql.Result, err error) {
	sql, args, err := clause.SQL()
	if err != nil {
		return nil, err
	}
	return session.ExecContext(ctx, sql, args...)
}
