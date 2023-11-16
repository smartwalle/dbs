package dbs

import (
	"bytes"
	"context"
	"database/sql"
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

func (b *builder) replace(clause string) string {
	return b.placeholder.Replace(clause)
}

// RawBuilder 原始 SQL 语句构造器，不会自动添加任何的关键字，主要是为了便于 SQL 语句及参数的管理。
type RawBuilder struct {
	builder
	buf  *bytes.Buffer
	args []interface{}
}

func (rb *RawBuilder) Type() string {
	return kRawBuilder
}

func (rb *RawBuilder) UsePlaceholder(p Placeholder) *RawBuilder {
	rb.builder.UsePlaceholder(p)
	return rb
}

func (rb *RawBuilder) Append(clause string, args ...interface{}) *RawBuilder {
	if clause != "" {
		if rb.buf.Len() > 0 {
			rb.buf.WriteString(" ")
		}
		rb.buf.WriteString(clause)
	}
	if len(args) > 0 {
		rb.args = append(rb.args, args...)
	}
	return rb
}

func (rb *RawBuilder) Appends(clauses ...string) *RawBuilder {
	for _, clause := range clauses {
		if clause != "" {
			if rb.buf.Len() > 0 {
				rb.buf.WriteString(" ")
			}
			rb.buf.WriteString(clause)
		}
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
	return rb.replace(rb.buf.String()), rb.args, nil
}

func (rb *RawBuilder) Write(w Writer) error {
	w.WriteString(rb.buf.String())
	w.WriteArgs(rb.args...)
	return nil
}

func (rb *RawBuilder) reset() {
	rb.buf.Reset()
	rb.args = rb.args[:0]
}

func (rb *RawBuilder) Exec(s Session) (sql.Result, error) {
	return exec(context.Background(), s, rb)
}

func (rb *RawBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return exec(ctx, s, rb)
}

func (rb *RawBuilder) Scan(s Session, dst interface{}) (err error) {
	return scan(context.Background(), s, rb, dst)
}

func (rb *RawBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scan(ctx, s, rb, dst)
}

func (rb *RawBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRow(context.Background(), s, rb, dst...)
}

func (rb *RawBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRow(ctx, s, rb, dst...)
}

func (rb *RawBuilder) Query(s Session) (*sql.Rows, error) {
	return query(context.Background(), s, rb)
}

func (rb *RawBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return query(ctx, s, rb)
}

func NewBuilder(clause string, args ...interface{}) *RawBuilder {
	var b = &RawBuilder{}
	b.placeholder = gPlaceholder
	b.buf = &bytes.Buffer{}
	b.args = make([]interface{}, 0, 8)
	b.Append(clause, args...)
	return b
}
