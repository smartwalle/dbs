package dbs

import (
	"bytes"
	"io"
	"reflect"
	"strings"
)

// --------------------------------------------------------------------------------
type Args struct {
	values []interface{}
}

func (this *Args) Append(args ...interface{}) {
	this.values = append(this.values, args...)
}

func newArgs() *Args {
	return &Args{}
}

// --------------------------------------------------------------------------------
type Statement interface {
	AppendToSQL(w io.Writer, sep string, args *Args)
	ToSQL() (string, []interface{})
	Valid() bool
}

// --------------------------------------------------------------------------------
type statement struct {
	sql  string
	args []interface{}
}

func NewStatement(sql string, args ...interface{}) *statement {
	var st = &statement{}
	st.sql = sql
	st.args = args
	return st
}

func SQL(sql string, args ...interface{}) *statement {
	return NewStatement(sql, args...)
}

func (this *statement) AppendToSQL(w io.Writer, sep string, args *Args) {
	if len(this.sql) > 0 {
		io.WriteString(w, this.sql)
	}
	if len(this.args) > 0 && args != nil {
		args.Append(this.args...)
	}
}

func (this *statement) ToSQL() (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	this.AppendToSQL(sqlBuffer, "", nil)
	return sqlBuffer.String(), this.args
}

func (this *statement) Valid() bool {
	if len(this.sql) > 0 || len(this.args) > 0 {
		return true
	}
	return false
}

// --------------------------------------------------------------------------------
type statements []Statement

func (this statements) AppendToSQL(w io.Writer, sep string, args *Args) {
	for i, c := range this {
		if i != 0 {
			io.WriteString(w, sep)
		}
		c.AppendToSQL(w, sep, args)
	}
}

func (this statements) ToSQL() (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	this.AppendToSQL(sqlBuffer, "", args)
	return sqlBuffer.String(), args.values
}

func (this statements) Valid() bool {
	return true
}

// --------------------------------------------------------------------------------
type Clause struct {
	sql  string
	args Statement
}

func NewClause(sql string, s Statement) *Clause {
	var c = &Clause{}
	c.sql = sql
	c.args = s
	return c
}

func (this *Clause) AppendToSQL(w io.Writer, sep string, args *Args) {
	if len(this.sql) > 0 {
		io.WriteString(w, this.sql)
	}
	if this.args != nil {
		this.args.AppendToSQL(w, sep, args)
	}
}

func (this *Clause) ToSQL() (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	this.AppendToSQL(sqlBuffer, "", args)
	return sqlBuffer.String(), args.values
}

func (this *Clause) Valid() bool {
	return true
}

// --------------------------------------------------------------------------------
type set struct {
	column string
	value  interface{}
}

func newSet(column string, value interface{}) *set {
	return &set{column, value}
}

func (this *set) AppendToSQL(w io.Writer, sep string, args *Args) {
	io.WriteString(w, this.column)
	io.WriteString(w, "=")
	switch tv := this.value.(type) {
	case Statement:
		tv.AppendToSQL(w, "", args)
	default:
		io.WriteString(w, "?")
		if this.value != nil && args != nil {
			args.Append(this.value)
		}
	}
}

func (this *set) ToSQL() (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	this.AppendToSQL(sqlBuffer, " ", args)
	return sqlBuffer.String(), args.values
}

func (this *set) Valid() bool {
	return true
}

// --------------------------------------------------------------------------------
type sets []Statement

func (this sets) AppendToSQL(w io.Writer, sep string, args *Args) {
	for i, c := range this {
		if i != 0 {
			io.WriteString(w, sep)
		}
		c.AppendToSQL(w, "", args)
	}
}

func (this sets) ToSQL() (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	this.AppendToSQL(sqlBuffer, ", ", args)
	return sqlBuffer.String(), args.values
}

func (this *sets) Valid() bool {
	return true
}

// --------------------------------------------------------------------------------
type where struct {
	sql      string
	prefix   string
	args     []interface{}
	children []Statement
}

func (this *where) AppendToSQL(w io.Writer, sep string, args *Args) {
	var hasSQL = len(this.sql) > 0
	var hasChildren = len(this.children) > 0
	var hasParen = len(this.children) > 1

	if len(this.args) > 0 {
		args.Append(this.args...)
	}

	if hasSQL || hasChildren {
		if hasParen {
			io.WriteString(w, "(")
		}
		io.WriteString(w, this.sql)
		for i, e := range this.children {
			if i != 0 || hasSQL {
				io.WriteString(w, this.prefix)
			}
			e.AppendToSQL(w, sep, args)
		}
		if hasParen {
			io.WriteString(w, ")")
		}
	}
}

func (this *where) ToSQL() (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	this.AppendToSQL(sqlBuffer, " ", args)
	return sqlBuffer.String(), args.values
}

func (this *where) Append(sts ...Statement) *where {
	for _, c := range sts {
		if c != nil {
			this.children = append(this.children, c)
		}
	}
	return this
}

func (this *where) Valid() bool {
	if len(this.sql) > 0 || len(this.children) > 0 {
		return true
	}
	return false
}

// --------------------------------------------------------------------------------
func AND(sts ...Statement) *where {
	var w = &where{}
	w.children = sts
	w.prefix = " AND "
	return w
}

func OR(sts ...Statement) *where {
	var w = &where{}
	w.children = sts
	w.prefix = " OR "
	return w
}

func IN(sql string, args interface{}) Statement {
	if len(sql) == 0 {
		return nil
	}

	var pType = reflect.TypeOf(args)
	var pValue = reflect.ValueOf(args)
	var params []interface{}

	if pType.Kind() == reflect.Array || pType.Kind() == reflect.Slice {
		var l = pValue.Len()
		params = make([]interface{}, l)
		for i := 0; i < l; i++ {
			params[i] = pValue.Index(i).Interface()
		}
	}

	if len(params) > 0 {
		sql = sql + " IN (" + strings.Repeat(", ?", len(params))[2:] + ")"
	}

	var st = &statement{}
	st.sql = sql
	st.args = params
	return st
}
