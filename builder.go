package dbs

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// --------------------------------------------------------------------------------
const (
	kRawBuilder = "RawBuilder"
)

// --------------------------------------------------------------------------------
type Builder interface {
	Type() string
	ToSQL() (string, []interface{}, error)
}

// --------------------------------------------------------------------------------
type RawBuilder struct {
	d    dialect
	sql  *bytes.Buffer
	args []interface{}
}

func (this *RawBuilder) Type() string {
	return kRawBuilder
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
	sql, err := this.parseVal(sql)
	if err != nil {
		return "", nil, err
	}
	return sql, this.args, nil
}

func (this *RawBuilder) WriteToSQL(w Writer) error {
	w.WriteString(this.sql.String())
	w.WriteArgs(this.args...)
	return nil
}

// --------------------------------------------------------------------------------
func (this *RawBuilder) UseDialect(d dialect) {
	this.d = d
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

// --------------------------------------------------------------------------------
func (this *RawBuilder) Scan(s Session, dest interface{}) (err error) {
	return scanContext(context.Background(), s, this, dest)
}

func (this *RawBuilder) ScanContext(ctx context.Context, s Session, dest interface{}) (err error) {
	return scanContext(ctx, s, this, dest)
}

func (this *RawBuilder) ScanRow(s Session, dest ...interface{}) (err error) {
	return scanRowContext(context.Background(), s, this, dest...)
}

func (this *RawBuilder) ScanRowContext(ctx context.Context, s Session, dest ...interface{}) (err error) {
	return scanRowContext(ctx, s, this, dest...)
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

// --------------------------------------------------------------------------------
func NewBuilder(sql string, args ...interface{}) *RawBuilder {
	var b = &RawBuilder{}
	b.d = gDialect
	b.sql = &bytes.Buffer{}
	b.Append(sql, args...)
	return b
}
