package dbs

import (
	"bytes"
	"fmt"
	"io"
)

// --------------------------------------------------------------------------------
type Builder interface {
	ToSQL() (string, []interface{}, error)
}

// --------------------------------------------------------------------------------
type RawBuilder struct {
	*query
	*exec
	*scan
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
	sql, err := Placeholder.Replace(sql)
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
	b.query = &query{sFunc: b.ToSQL}
	b.exec = &exec{sFunc: b.ToSQL}
	b.scan = &scan{qFunc: b.QueryContext}
	b.sql = &bytes.Buffer{}
	b.Append(sql, args...)
	return b
}
