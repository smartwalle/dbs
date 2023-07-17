package dbs

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	kRawBuilder = "RawBuilder"
)

type Builder interface {
	Type() string
	SQL() (string, []interface{}, error)
}

type builder struct {
	d Dialect
}

func (this *builder) UseDialect(d Dialect) {
	this.d = d
}

func (this *builder) GetDialect() Dialect {
	return this.d
}

func (this *builder) quote(s string) string {
	if strings.Index(s, ".") != -1 {
		var newStrs []string
		for _, s := range strings.Split(s, ".") {
			newStrs = append(newStrs, this.d.Quote(s))
		}
		return strings.Join(newStrs, ".")
	}
	return this.d.Quote(s)
}

func (this *builder) parseVal(sql string) (string, error) {
	return this.d.ParseVal(sql)
}

// RawBuilder 原始 SQL 语句构造器，不会自动添加任何的关键字，主要是为了便于 SQL 语句及参数的管理。
type RawBuilder struct {
	builder
	sql  *bytes.Buffer
	args []interface{}
}

func (this *RawBuilder) Type() string {
	return kRawBuilder
}

func (this *RawBuilder) UseDialect(d Dialect) *RawBuilder {
	this.builder.UseDialect(d)
	return this
}

func (this *RawBuilder) Append(sql string, args ...interface{}) *RawBuilder {
	if sql != "" {
		if this.sql.Len() > 0 {
			this.sql.WriteString(" ")
		}
		this.sql.WriteString(sql)
	}
	if len(args) > 0 {
		this.args = append(this.args, args...)
	}
	return this
}

func (this *RawBuilder) Format(format string, args ...interface{}) *RawBuilder {
	var v = fmt.Sprintf(format, args...)
	if v != "" {
		if this.sql.Len() > 0 {
			this.sql.WriteString(" ")
		}
		this.sql.WriteString(v)
	}
	return this
}

func (this *RawBuilder) Params(args ...interface{}) *RawBuilder {
	if len(args) > 0 {
		this.args = append(this.args, args...)
	}
	return this
}

func (this *RawBuilder) SQL() (string, []interface{}, error) {
	var sql = this.sql.String()
	sql, err := this.parseVal(sql)
	if err != nil {
		return "", nil, err
	}
	return sql, this.args, nil
}

func (this *RawBuilder) Write(w Writer) error {
	w.WriteString(this.sql.String())
	w.WriteArgs(this.args...)
	return nil
}

func (this *RawBuilder) reset() {
	this.sql.Reset()
	this.args = this.args[:0]
}

func (this *RawBuilder) quote(s string) string {
	if strings.Index(s, ".") != -1 {
		var newStrs []string
		for _, s := range strings.Split(s, ".") {
			newStrs = append(newStrs, this.d.Quote(s))
		}
		return strings.Join(newStrs, ".")
	}
	return this.d.Quote(s)
}

func (this *RawBuilder) parseVal(sql string) (string, error) {
	return this.d.ParseVal(sql)
}

func (this *RawBuilder) Scan(s Session, dst interface{}) (err error) {
	return scanContext(context.Background(), s, this, dst)
}

func (this *RawBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scanContext(ctx, s, this, dst)
}

func (this *RawBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRowContext(context.Background(), s, this, dst...)
}

func (this *RawBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRowContext(ctx, s, this, dst...)
}

func (this *RawBuilder) Query(s Session) (*sql.Rows, error) {
	return queryContext(context.Background(), s, this)
}

func (this *RawBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return queryContext(ctx, s, this)
}

func (this *RawBuilder) Exec(s Session) (sql.Result, error) {
	return execContext(context.Background(), s, this)
}

func (this *RawBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return execContext(ctx, s, this)
}

func NewBuilder(sql string, args ...interface{}) *RawBuilder {
	var b = &RawBuilder{}
	b.d = gDialect
	b.sql = &bytes.Buffer{}
	b.args = make([]interface{}, 0, 8)
	b.Append(sql, args...)
	return b
}
