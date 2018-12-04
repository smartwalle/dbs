package dbs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// --------------------------------------------------------------------------------
type scan struct {
	b Builder
}

func (this *scan) Scan(s Executor, dest interface{}) (err error) {
	return this.scanContext(context.Background(), s, dest)
}

func (this *scan) ScanContext(ctx context.Context, s Executor, dest interface{}) (err error) {
	return this.scanContext(ctx, s, dest)
}

func (this *scan) scanContext(ctx context.Context, s Executor, dest interface{}) (err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sqlStr, args, err := this.b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句出错:", err))
		return err
	}
	logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句成功:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Query 出错:", err))
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	if err = Scan(rows, dest); err != nil {
		logger.Output(3, fmt.Sprintln("Scan 出错:", err))
		return err
	}
	return nil
}

func (this *scan) ScanRow(s Executor, dest ...interface{}) (err error) {
	return this.scanRowContext(context.Background(), s, dest...)
}

func (this *scan) ScanRowContext(ctx context.Context, s Executor, dest ...interface{}) (err error) {
	return this.scanRowContext(ctx, s, dest...)
}

func (this *scan) scanRowContext(ctx context.Context, s Executor, dest ...interface{}) (err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sqlStr, args, err := this.b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句出错:", err))
		return err
	}
	logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句成功:", sqlStr, args))
	rows, err := s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Query 出错:", err))
		return err
	}
	if rows != nil {
		defer rows.Close()
	}

	for _, dp := range dest {
		if _, ok := dp.(*sql.RawBytes); ok {
			err = errors.New("sql: RawBytes isn't allowed on Row.Scan")
			logger.Output(3, fmt.Sprintln("Scan 出错:", err))
			return err
		}
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			logger.Output(3, fmt.Sprintln("Scan 出错:", err))
			return err
		}
		logger.Output(3, fmt.Sprintln("Scan 出错:", sql.ErrNoRows))
		return sql.ErrNoRows
	}
	if err = rows.Scan(dest...); err != nil {
		logger.Output(3, fmt.Sprintln("Scan 出错:", err))
		return err
	}
	return rows.Close()
}

// --------------------------------------------------------------------------------
type query struct {
	b Builder
}

func (this *query) Query(s Executor) (*sql.Rows, error) {
	return this.queryContext(context.Background(), s)
}

func (this *query) QueryContext(ctx context.Context, s Executor) (*sql.Rows, error) {
	return this.queryContext(ctx, s)
}

func (this *query) queryContext(ctx context.Context, s Executor) (result *sql.Rows, err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
				result = nil
			}
		}
	}()

	sqlStr, args, err := this.b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句出错:", err))
		return nil, err
	}
	logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句成功:", sqlStr, args))
	result, err = s.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Query 出错:", err))
	}
	return result, err
}

// --------------------------------------------------------------------------------
type exec struct {
	b Builder
}

func (this *exec) Exec(s Executor) (sql.Result, error) {
	return this.execContext(context.Background(), s)
}

func (this *exec) ExecContext(ctx context.Context, s Executor) (result sql.Result, err error) {
	return this.execContext(ctx, s)
}

func (this *exec) execContext(ctx context.Context, s Executor) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			if tx, ok := s.(TX); ok {
				tx.Rollback()
			}
		}
	}()

	sqlStr, args, err := this.b.ToSQL()
	if err != nil {
		logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句出错:", err))
		return nil, err
	}

	logger.Output(3, fmt.Sprintln(this.b.Type(), "构建 SQL 语句成功:", sqlStr, args))
	result, err = s.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		logger.Output(3, fmt.Sprintln("Exec 出错:", err))
	}
	return result, err
}
