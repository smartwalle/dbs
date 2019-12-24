package dbs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func scanContext(ctx context.Context, s Session, b Builder, dest interface{}) (err error) {
	var tx TX
	var prefix string

	if nTx, ok := s.(TX); ok {
		tx = nTx
		prefix = tx.String() + " " + b.Type()
	} else {
		prefix = b.Type()
	}

	defer func() {
		if err != nil && tx != nil {
			tx.rollback(5)
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return err
	}
	logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Query Failed:", err))
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	if err = Scan(rows, dest); err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
		return err
	}
	return nil
}

func scanRowContext(ctx context.Context, s Session, b Builder, dest ...interface{}) (err error) {
	var tx TX
	var prefix string

	if nTx, ok := s.(TX); ok {
		tx = nTx
		prefix = tx.String() + " " + b.Type()
	} else {
		prefix = b.Type()
	}

	defer func() {
		if err != nil && tx != nil {
			tx.rollback(5)
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return err
	}
	logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Query Failed:", err))
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			err = errors.New("sql: RawBytes isn't allowed on Row.Scan")
			logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
			return err
		}
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
			return err
		}
		logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", sql.ErrNoRows))
		return sql.ErrNoRows
	}
	if err = rows.Scan(dest...); err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Scan Failed:", err))
		return err
	}
	return rows.Close()
}

func queryContext(ctx context.Context, s Session, b Builder) (result *sql.Rows, err error) {
	var tx TX
	var prefix string

	if nTx, ok := s.(TX); ok {
		tx = nTx
		prefix = tx.String() + " " + b.Type()
	} else {
		prefix = b.Type()
	}

	defer func() {
		if err != nil && tx != nil {
			tx.rollback(5)
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return nil, err
	}
	logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	result, err = s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Query Failed:", err))
	}
	return result, err
}

func execContext(ctx context.Context, s Session, b Builder) (result sql.Result, err error) {
	var tx TX
	var prefix string

	if nTx, ok := s.(TX); ok {
		tx = nTx
		prefix = tx.String() + " " + b.Type()
	} else {
		prefix = b.Type()
	}

	defer func() {
		if err != nil && tx != nil {
			tx.rollback(5)
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Build Failed:", err))
		return nil, err
	}

	logger.Output(3, fmt.Sprintln(prefix, "Build Success:", sqlStr, args))
	result, err = s.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln(prefix, "Exec Failed:", err))
	}
	return result, err
}
