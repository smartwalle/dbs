package dbs

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
)

// --------------------------------------------------------------------------------
type Builder interface {
	ToSQL() (string, []interface{}, error)
}

// --------------------------------------------------------------------------------
type RawBuilder struct {
	sql  *bytes.Buffer
	args []interface{}
}

func (this *RawBuilder) Append(sql string, args ...interface{}) *RawBuilder {
	if sql != "" {
		this.sql.WriteString(sql)
		this.sql.WriteString(" ")
	}
	if len(args) > 0 {
		this.args = append(this.args, args...)
	}
	return this
}

func (this *RawBuilder) Format(format string, args ...interface{}) *RawBuilder {
	var v = fmt.Sprintf(format, args...)
	if v != "" {
		this.sql.WriteString(v)
		this.sql.WriteString(" ")
	}
	return this
}

func (this *RawBuilder) Params(args ...interface{}) *RawBuilder {
	if len(args) > 0 {
		this.args = append(this.args, args...)
	}
	return this
}

func (this *RawBuilder) ToSQL() (string, []interface{}, error) {
	var sql = this.sql.String()
	log(sql, this.args)
	return sql, this.args, nil
}

func (this *RawBuilder) AppendToSQL(w io.Writer, args *Args) error {
	io.WriteString(w, this.sql.String())
	args.Append(this.args...)
	return nil
}

// --------------------------------------------------------------------------------
func (this *RawBuilder) Exec(s Executor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.Exec(sql, args...)
}

func (this *RawBuilder) ExecContext(ctx context.Context, s Executor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.ExecContext(ctx, sql, args...)
}

func (this *RawBuilder) ExecTx(tx TX) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	result, err = this.Exec(tx)
	return result, err
}

func (this *RawBuilder) ExecContextTx(ctx context.Context, tx TX) (result sql.Result, err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	result, err = this.ExecContext(ctx, tx)
	return result, err
}

// --------------------------------------------------------------------------------
func (this *RawBuilder) Query(s Executor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.Query(sql, args...)
}

func (this *RawBuilder) QueryContext(ctx context.Context, s Executor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.QueryContext(ctx, sql, args...)
}

func (this *RawBuilder) QueryTx(tx TX) (rows *sql.Rows, err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			rows = nil
		}
	}()
	rows, err = this.Query(tx)
	return rows, err
}

func (this *RawBuilder) QueryContextTx(ctx context.Context, tx TX) (rows *sql.Rows, err error) {
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
func (this *RawBuilder) Scan(s Executor, result interface{}) (err error) {
	rows, err := this.Query(s)
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}
	err = Scan(rows, result)
	return err
}

func (this *RawBuilder) ScanContext(ctx context.Context, s Executor, result interface{}) (err error) {
	rows, err := this.QueryContext(ctx, s)
	if err != nil {
		return err
	}
	if rows != nil {
		defer rows.Close()
	}
	err = Scan(rows, result)
	return err
}

func (this *RawBuilder) ScanTx(tx TX, result interface{}) (err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			result = nil
		}
	}()
	err = this.Scan(tx, result)
	return err
}

func (this *RawBuilder) ScanContextTx(ctx context.Context, tx TX, result interface{}) (err error) {
	defer func() {
		if err != nil {
			tx.Rollback()
			result = nil
		}
	}()
	err = this.ScanContext(ctx, tx, result)
	return err
}

// --------------------------------------------------------------------------------
func NewBuilder(sql string, args ...interface{}) *RawBuilder {
	var b = &RawBuilder{}
	b.sql = &bytes.Buffer{}
	b.Append(sql, args...)
	return b
}
