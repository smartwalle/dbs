package dbs

import (
	"context"
	"database/sql"
	"errors"
)

// --------------------------------------------------------------------------------
type scan struct {
	qFunc func(ctx context.Context, s Executor) (*sql.Rows, error)
}

func (this *scan) Scan(s Executor, result interface{}) (err error) {
	return this.ScanContext(context.Background(), s, result)
}

func (this *scan) ScanContext(ctx context.Context, s Executor, result interface{}) (err error) {
	rows, err := this.qFunc(ctx, s)
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}
	err = Scan(rows, result)
	return err
}

func (this *scan) ScanTx(tx TX, result interface{}) (err error) {
	return this.ScanContextTx(context.Background(), tx, result)
}

func (this *scan) ScanContextTx(ctx context.Context, tx TX, result interface{}) (err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			result = nil
		}
	}()
	err = this.ScanContext(ctx, tx, result)
	return err
}

func (this *scan) ScanRow(s Executor, dest ...interface{}) (err error) {
	return this.ScanRowContext(context.Background(), s, dest...)
}

func (this *scan) ScanRowContext(ctx context.Context, s Executor, dest ...interface{}) (err error) {
	rows, err := this.qFunc(ctx, s)
	if err != nil {
		return err
	}

	defer rows.Close()
	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			return errors.New("sql: RawBytes isn't allowed on Row.Scan")
		}
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err = rows.Scan(dest...); err != nil {
		return err
	}

	if err := rows.Close(); err != nil {
		return err
	}
	return nil
}

func (this *scan) ScanRowTx(tx TX, dest ...interface{}) (err error) {
	return this.ScanRowContextTx(context.Background(), tx, dest...)
}

func (this *scan) ScanRowContextTx(ctx context.Context, tx TX, dest ...interface{}) (err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	err = this.ScanRowContext(ctx, tx, dest...)
	return err
}

// --------------------------------------------------------------------------------
type query struct {
	sFunc func() (string, []interface{}, error)
}

func (this *query) Query(s Executor) (*sql.Rows, error) {
	return this.QueryContext(context.Background(), s)
}

func (this *query) QueryContext(ctx context.Context, s Executor) (*sql.Rows, error) {
	sql, args, err := this.sFunc()
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, sql, args...)
}

func (this *query) QueryTx(tx TX) (rows *sql.Rows, err error) {
	return this.QueryContextTx(context.Background(), tx)
}

func (this *query) QueryContextTx(ctx context.Context, tx TX) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			rows = nil
		}
	}()
	rows, err = this.QueryContext(ctx, tx)
	return rows, err
}

// --------------------------------------------------------------------------------
type exec struct {
	sFunc func() (string, []interface{}, error)
}

func (this *exec) Exec(s Executor) (sql.Result, error) {
	return this.ExecContext(context.Background(), s)
}

func (this *exec) ExecContext(ctx context.Context, s Executor) (sql.Result, error) {
	sql, args, err := this.sFunc()
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, sql, args...)
}

func (this *exec) ExecTx(tx TX) (result sql.Result, err error) {
	return this.ExecContextTx(context.Background(), tx)
}

func (this *exec) ExecContextTx(ctx context.Context, tx TX) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	result, err = this.ExecContext(ctx, tx)
	return result, err
}
