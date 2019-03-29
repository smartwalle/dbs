package dbs

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// --------------------------------------------------------------------------------
type builder struct {
	d dialect
}

func (this *builder) UseDialect(d dialect) {
	this.d = d
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

func newBuilder() *builder {
	var b = &builder{}
	b.d = gDialect
	return b
}

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
	*builder
	*query
	*exec
	*scan
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

func (this *RawBuilder) AppendToSQL(w io.Writer, args *Args) error {
	io.WriteString(w, this.sql.String())
	args.Append(this.args...)
	return nil
}

// --------------------------------------------------------------------------------
func NewBuilder(sql string, args ...interface{}) *RawBuilder {
	var b = &RawBuilder{}
	b.builder = newBuilder()
	b.query = &query{b: b}
	b.exec = &exec{b: b}
	b.scan = &scan{b: b}
	b.sql = &bytes.Buffer{}
	b.Append(sql, args...)
	return b
}
