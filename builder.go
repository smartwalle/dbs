package dbs

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
)

const (
	kRawBuilder = "RawBuilder"
)

type Builder interface {
	Type() string
	SQL() (string, []interface{}, error)
}

type builder struct {
	placeholder Placeholder
}

func (b *builder) UsePlaceholder(p Placeholder) {
	b.placeholder = p
}

func (b *builder) GetPlaceholder() Placeholder {
	return b.placeholder
}

func (b *builder) quote(s string) string {
	//if strings.Index(s, ".") != -1 {
	//	var newStrs []string
	//	for _, s := range strings.Split(s, ".") {
	//		newStrs = append(newStrs, b.placeholder.Quote(s))
	//	}
	//	return strings.Join(newStrs, ".")
	//}
	//return b.placeholder.Quote(s)
	return s
}

func (b *builder) replace(sql string) (string, error) {
	return b.placeholder.Replace(sql)
}

// RawBuilder 原始 SQL 语句构造器，不会自动添加任何的关键字，主要是为了便于 SQL 语句及参数的管理。
type RawBuilder struct {
	builder
	sql  *bytes.Buffer
	args []interface{}
}

func (rb *RawBuilder) Type() string {
	return kRawBuilder
}

func (rb *RawBuilder) UsePlaceholder(p Placeholder) *RawBuilder {
	rb.builder.UsePlaceholder(p)
	return rb
}

func (rb *RawBuilder) Append(sql string, args ...interface{}) *RawBuilder {
	if sql != "" {
		if rb.sql.Len() > 0 {
			rb.sql.WriteString(" ")
		}
		rb.sql.WriteString(sql)
	}
	if len(args) > 0 {
		rb.args = append(rb.args, args...)
	}
	return rb
}

func (rb *RawBuilder) Format(format string, args ...interface{}) *RawBuilder {
	var v = fmt.Sprintf(format, args...)
	if v != "" {
		if rb.sql.Len() > 0 {
			rb.sql.WriteString(" ")
		}
		rb.sql.WriteString(v)
	}
	return rb
}

func (rb *RawBuilder) Params(args ...interface{}) *RawBuilder {
	if len(args) > 0 {
		rb.args = append(rb.args, args...)
	}
	return rb
}

func (rb *RawBuilder) SQL() (string, []interface{}, error) {
	var sql = rb.sql.String()
	sql, err := rb.replace(sql)
	if err != nil {
		return "", nil, err
	}
	return sql, rb.args, nil
}

func (rb *RawBuilder) Write(w Writer) error {
	w.WriteString(rb.sql.String())
	w.WriteArgs(rb.args...)
	return nil
}

func (rb *RawBuilder) reset() {
	rb.sql.Reset()
	rb.args = rb.args[:0]
}

func (rb *RawBuilder) Scan(s Session, dst interface{}) (err error) {
	return scanContext(context.Background(), s, rb, dst)
}

func (rb *RawBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scanContext(ctx, s, rb, dst)
}

func (rb *RawBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRowContext(context.Background(), s, rb, dst...)
}

func (rb *RawBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRowContext(ctx, s, rb, dst...)
}

func (rb *RawBuilder) Query(s Session) (*sql.Rows, error) {
	return queryContext(context.Background(), s, rb)
}

func (rb *RawBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return queryContext(ctx, s, rb)
}

func (rb *RawBuilder) Exec(s Session) (sql.Result, error) {
	return execContext(context.Background(), s, rb)
}

func (rb *RawBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return execContext(ctx, s, rb)
}

func NewBuilder(sql string, args ...interface{}) *RawBuilder {
	var b = &RawBuilder{}
	b.placeholder = gPlaceholder
	b.sql = &bytes.Buffer{}
	b.args = make([]interface{}, 0, 8)
	b.Append(sql, args...)
	return b
}
