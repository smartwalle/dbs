package dbs

import (
	"context"
	"database/sql"
)

var mapper = NewMapper(kTag)

func Scan[T any](rows *sql.Rows) (dst T, err error) {
	err = mapper.Decode(rows, &dst)
	return dst, err
}

func Query[T any](ctx context.Context, session Session, query string, args ...interface{}) (dst T, err error) {
	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return dst, err
	}
	defer rows.Close()

	err = mapper.Decode(rows, &dst)
	return dst, err
}

func Exec(ctx context.Context, session Session, query string, args ...interface{}) (result sql.Result, err error) {
	return session.ExecContext(ctx, query, args...)
}

func scan(ctx context.Context, session Session, builder Builder, dst interface{}) error {
	q, args, err := builder.SQL()
	if err != nil {
		return err
	}
	rows, err := session.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return mapper.Decode(rows, dst)
}

func scanRow(ctx context.Context, session Session, builder Builder, dst ...interface{}) error {
	q, args, err := builder.SQL()
	if err != nil {
		return err
	}
	row := session.QueryRowContext(ctx, q, args...)
	return row.Scan(dst...)
}

func query(ctx context.Context, session Session, builder Builder) (*sql.Rows, error) {
	q, args, err := builder.SQL()
	if err != nil {
		return nil, err
	}
	return session.QueryContext(ctx, q, args...)
}

func exec(ctx context.Context, session Session, builder Builder) (sql.Result, error) {
	q, args, err := builder.SQL()
	if err != nil {
		return nil, err
	}
	return session.ExecContext(ctx, q, args...)
}
