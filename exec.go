package dbs

import (
	"context"
	"database/sql"
)

var mapper = NewMapper(kTag)

func Scan(rows *sql.Rows, dst interface{}) (err error) {
	return mapper.Decode(rows, dst)
}

func Query(ctx context.Context, session Session, query string, dst interface{}, args ...interface{}) error {
	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return mapper.Decode(rows, dst)
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
