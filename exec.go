package dbs

import (
	"context"
	"database/sql"
	"errors"
)

var gMapper Mapper = NewMapper(kTag)

func UseMapper(s Mapper) {
	if s != nil {
		gMapper = s
	}
}

func GetMapper() Mapper {
	return gMapper
}

func Scan[T any](rows *sql.Rows) (dest T, err error) {
	if err = gMapper.Decode(rows, &dest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return dest, err
	}
	return dest, nil
}

func Query[T any](ctx context.Context, session Session, query string, args ...interface{}) (dest T, err error) {
	rows, err := session.QueryContext(ctx, query, args...)
	if err != nil {
		return dest, err
	}
	defer rows.Close()

	if err = gMapper.Decode(rows, &dest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return dest, err
	}
	return dest, nil
}

func Exec(ctx context.Context, session Session, query string, args ...interface{}) (result sql.Result, err error) {
	return session.ExecContext(ctx, query, args...)
}
