package dbs

import (
	"context"
	"database/sql"
)

var mapper = NewMapper(kTag)

func Scan(rows *sql.Rows, dst interface{}) (err error) {
	return mapper.Decode(rows, dst)
}

func Query(ctx context.Context, session Session, query string, dst interface{}, args ...interface{}) (err error) {
	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return mapper.Decode(rows, dst)
}

func Exec(ctx context.Context, session Session, query string, args ...interface{}) (result sql.Result, err error) {
	return session.ExecContext(ctx, query, args...)
}

func scanContext(ctx context.Context, session Session, builder Builder, dst interface{}) error {
	query, args, err := builder.SQL()
	if err != nil {
		return err
	}
	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err = mapper.Decode(rows, dst); err != nil {
		return err
	}
	return nil
}

func scanRowContext(ctx context.Context, session Session, builder Builder, dst ...interface{}) error {
	query, args, err := builder.SQL()
	if err != nil {
		return err
	}
	row := session.QueryRowContext(ctx, query, args...)
	return row.Scan(dst...)
}

func queryContext(ctx context.Context, session Session, builder Builder) (*sql.Rows, error) {
	query, args, err := builder.SQL()
	if err != nil {
		return nil, err
	}
	return session.QueryContext(ctx, query, args...)
}

func execContext(ctx context.Context, session Session, builder Builder) (sql.Result, error) {
	query, args, err := builder.SQL()
	if err != nil {
		return nil, err
	}
	return session.ExecContext(ctx, query, args...)
}
