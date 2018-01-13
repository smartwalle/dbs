package dbs

import (
	"bytes"
	"io"
)

// --------------------------------------------------------------------------------
type Args struct {
	args []interface{}
}

func (this *Args) Append(args ...interface{}) {
	this.args = append(this.args, args...)
}

// --------------------------------------------------------------------------------
type Clauser interface {
	AppendToSQL(w io.Writer, sep string, args *Args)
	ToSQL(sep string) (string, []interface{})
}

// --------------------------------------------------------------------------------
type Clause struct {
	sql  string
	args []interface{}
}

func NewClause(sql string, args ...interface{}) Clauser {
	var c = &Clause{}
	c.sql = sql
	c.args = args
	return c
}

func (this *Clause) AppendToSQL(w io.Writer, sep string, args *Args) {
	if len(this.sql) > 0 {
		io.WriteString(w, this.sql)
	}
	if len(this.args) > 0 && args != nil {
		args.Append(this.args...)
	}
}

func (this *Clause) ToSQL(sep string) (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	this.AppendToSQL(sqlBuffer, sep, nil)
	return sqlBuffer.String(), this.args
}

// --------------------------------------------------------------------------------
type Clauses []Clauser

func (this Clauses) AppendToSQL(w io.Writer, sep string, args *Args) {
	for i, c := range this {
		if i != 0 {
			io.WriteString(w, sep)
		}
		c.AppendToSQL(w, sep, args)
	}
}

func (this Clauses) ToSQL(sep string) (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = &Args{}
	this.AppendToSQL(sqlBuffer, sep, args)
	return sqlBuffer.String(), args.args
}

// --------------------------------------------------------------------------------
type Set struct {
	column string
	value  interface{}
}

func NewSet(column string, value interface{}) Clauser {
	return &Set{column, value}
}

func (this *Set) AppendToSQL(w io.Writer, sep string, args *Args) {
	io.WriteString(w, this.column)
	io.WriteString(w, "=")
	switch tv := this.value.(type) {
	case Clauser:
		tv.AppendToSQL(w, "", args)
	default:
		io.WriteString(w, "?")
		if this.value != nil && args != nil {
			args.Append(this.value)
		}
	}
}

func (this *Set) ToSQL(sep string) (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = &Args{}
	this.AppendToSQL(sqlBuffer, " ", args)
	return sqlBuffer.String(), args.args
}

// --------------------------------------------------------------------------------
type Sets []Clauser

func (this Sets) AppendToSQL(w io.Writer, sep string, args *Args) {
	for i, c := range this {
		if i != 0 {
			io.WriteString(w, sep)
		}
		c.AppendToSQL(w, "", args)
	}
}

func (this Sets) ToSQL(sep string) (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = &Args{}
	this.AppendToSQL(sqlBuffer, sep, args)
	return sqlBuffer.String(), args.args
}