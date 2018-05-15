package dbs

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
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

func (this *RawBuilder) Query(s SQLExecutor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.Query(sql, args...)
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

// --------------------------------------------------------------------------------
type LockBuilder struct {
	tables statements
}

func NewLockBuilder() *LockBuilder {
	var b = &LockBuilder{}
	return b
}

func (this *LockBuilder) LockTable(lockType, table string, args ...string) *LockBuilder {
	var ts []string
	ts = append(ts, fmt.Sprintf("`%s`", table))
	if len(args) > 0 {
		ts = append(ts, args...)
	}
	ts = append(ts, lockType)
	this.tables = append(this.tables, NewStatement(strings.Join(ts, " ")))
	return this
}

func (this *LockBuilder) WriteLock(table string, args ...string) *LockBuilder {
	return this.LockTable("WRITE", table, args...)
}

func (this *LockBuilder) ReadLock(table string, args ...string) *LockBuilder {
	return this.LockTable("READ", table, args...)
}

func (this *LockBuilder) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	if err := this.AppendToSQL(sqlBuffer, args); err != nil {
		return "", nil, err
	}
	sql := sqlBuffer.String()
	log(sql, args.values)
	return sql, args.values, nil
}

func (this *LockBuilder) AppendToSQL(w io.Writer, args *Args) error {
	if len(this.tables) == 0 {
		return nil
	}
	io.WriteString(w, "LOCK TABLES ")
	if err := this.tables.AppendToSQL(w, ", ", args); err != nil {
		return err
	}
	io.WriteString(w, ";")

	return nil
}

func (this *LockBuilder) Exec(s SQLExecutor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return s.Exec(sql, args...)
}

func WriteLock(table string, args ...string) *LockBuilder {
	var b = NewLockBuilder()
	b.WriteLock(table, args...)
	return b
}

func ReadLock(table string, args ...string) *LockBuilder {
	var b = NewLockBuilder()
	b.ReadLock(table, args...)
	return b
}

func UnlockTable() *RawBuilder {
	var b = NewBuilder()
	b.Append("UNLOCK TABLES;")
	return b
}