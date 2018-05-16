package dbs

import (
	"bytes"
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

func NewBuilder() *RawBuilder {
	var b = &RawBuilder{}
	b.sql = &bytes.Buffer{}
	return b
}

func (this *RawBuilder) Append(sql string, args ...interface{}) *RawBuilder {
	this.sql.WriteString(sql)
	this.sql.WriteString(" ")
	this.args = append(this.args, args...)
	return this
}

func (this *RawBuilder) Format(format string, args ...interface{}) *RawBuilder {
	var v = fmt.Sprintf(format, args...)
	this.sql.WriteString(v)
	this.sql.WriteString(" ")
	return this
}

func (this *RawBuilder) Params(args ...interface{}) *RawBuilder {
	this.args = append(this.args, args...)
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

func (this *RawBuilder) Exec(s SQLExecutor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.Exec(sql, args...)
}

func (this *RawBuilder) ExecRaw(s SQLExecutor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.ExecRaw(sql, args...)
}

func (this *RawBuilder) Query(s SQLExecutor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.Query(sql, args...)
}

func (this *RawBuilder) QueryRaw(s SQLExecutor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.QueryRaw(sql, args...)
}

func (this *RawBuilder) Scan(s SQLExecutor, result interface{}) (err error) {
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