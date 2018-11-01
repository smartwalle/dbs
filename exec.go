package dbs

import (
	"context"
	"database/sql"
	"errors"
)

// --------------------------------------------------------------------------------
type scan struct {
	b Builder
}

func (this *scan) Scan(s Executor, dest interface{}) (err error) {
	return this.ScanContext(context.Background(), s, dest)
}

func (this *scan) ScanContext(ctx context.Context, s Executor, dest interface{}) (err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sql, args, err := this.b.ToSQL()
	if err != nil {
		return err
	}
	rows, err := s.QueryContext(ctx, sql, args...)
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	err = Scan(rows, dest)
	return err
}

func (this *scan) ScanRow(s Executor, dest ...interface{}) (err error) {
	return this.ScanRowContext(context.Background(), s, dest...)
}

func (this *scan) ScanRowContext(ctx context.Context, s Executor, dest ...interface{}) (err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sqlStr, args, err := this.b.ToSQL()
	if err != nil {
		return err
	}
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			err = errors.New("sql: RawBytes isn't allowed on Row.Scan")
			return err
		}
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		err = sql.ErrNoRows
		return err
	}
	if err = rows.Scan(dest...); err != nil {
		return err
	}

	if err := rows.Close(); err != nil {
		return err
	}
	return nil
}

// --------------------------------------------------------------------------------
type query struct {
	b Builder
}

func (this *query) Query(s Executor) (*sql.Rows, error) {
	return this.QueryContext(context.Background(), s)
}

func (this *query) QueryContext(ctx context.Context, s Executor) (result *sql.Rows, err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
				result = nil
			}
		}
	}()

	sql, args, err := this.b.ToSQL()
	if err != nil {
		return nil, err
	}
	result, err = s.QueryContext(ctx, sql, args...)
	return result, err
}

// --------------------------------------------------------------------------------
type exec struct {
	b Builder
}

func (this *exec) Exec(s Executor) (sql.Result, error) {
	return this.ExecContext(context.Background(), s)
}

func (this *exec) ExecContext(ctx context.Context, s Executor) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sql, args, err := this.b.ToSQL()
	if err != nil {
		return nil, err
	}
	result, err = s.ExecContext(ctx, sql, args...)
	return result, err
}
