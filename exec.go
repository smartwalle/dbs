package dbs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// --------------------------------------------------------------------------------
func scanContext(ctx context.Context, s Session, b Builder, dest interface{}) (err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(b.Type(), "Build Failed:", err))
		return err
	}
	logger.Output(3, fmt.Sprintln(b.Type(), "Build Successfully:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Query Failed:", err))
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	if err = Scan(rows, dest); err != nil {
		logger.Output(3, fmt.Sprintln("Scan Failed:", err))
		return err
	}
	return nil
}

func scanRowContext(ctx context.Context, s Session, b Builder, dest ...interface{}) (err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(b.Type(), "Build Failed:", err))
		return err
	}
	logger.Output(3, fmt.Sprintln(b.Type(), "Build Successfully:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Query Failed:", err))
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			err = errors.New("sql: RawBytes isn't allowed on Row.Scan")
			logger.Output(3, fmt.Sprintln("Scan Failed:", err))
			return err
		}
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			logger.Output(3, fmt.Sprintln("Scan Failed:", err))
			return err
		}
		logger.Output(3, fmt.Sprintln("Scan Failed:", sql.ErrNoRows))
		return sql.ErrNoRows
	}
	if err = rows.Scan(dest...); err != nil {
		logger.Output(3, fmt.Sprintln("Scan Failed:", err))
		return err
	}
	return rows.Close()
}

// --------------------------------------------------------------------------------
func queryContext(ctx context.Context, s Session, b Builder) (result *sql.Rows, err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
				result = nil
			}
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(b.Type(), "Build Failed:", err))
		return nil, err
	}
	logger.Output(3, fmt.Sprintln(b.Type(), "Build Successfully:", sqlStr, args))
	result, err = s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Query Failed:", err))
	}
	return result, err
}

// --------------------------------------------------------------------------------
func execContext(ctx context.Context, s Session, b Builder) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sqlStr, args, err := b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(b.Type(), "Build Failed:", err))
		return nil, err
	}

	logger.Output(3, fmt.Sprintln(b.Type(), "Build Successfully:", sqlStr, args))
	result, err = s.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Exec Failed:", err))
	}
	return result, err
}
