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

func NC(sql string, args ...interface{}) Clauser {
	return NewClause(sql, args...)
}

func C(sql string, args ...interface{}) Clauser {
	return NewClause(sql, args...)
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

// --------------------------------------------------------------------------------
type where struct {
	sql      string
	prefix   string
	args     []interface{}
	children []Clauser
}

func NewWhere() *where {
	var w = &where{}
	return w
}

func (this *where) AppendToSQL(w io.Writer, sep string, args *Args) {
	var hasSQL = len(this.sql) > 0
	var hasChildren = len(this.children) > 0

	if len(this.args) > 0 {
		args.Append(this.args...)
	}

	if hasSQL || hasChildren {
		if hasChildren {
			io.WriteString(w, "(")
		}

		io.WriteString(w, this.sql)

		if hasSQL && len(this.prefix) > 0 && len(this.children) == 1 {
			io.WriteString(w, " ")
			io.WriteString(w, this.prefix)
			io.WriteString(w, " ")
		}

		for i, e := range this.children {
			if i != 0 {
				if len(this.prefix) > 0 {
					io.WriteString(w, " ")
					io.WriteString(w, this.prefix)
					io.WriteString(w, " ")
				}
			}
			e.AppendToSQL(w, sep, args)
		}

		if hasChildren {
			io.WriteString(w, ")")
		}
	}
}

func (this *where) ToSQL(sep string) (string, []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	var args = &Args{}
	this.AppendToSQL(sqlBuffer, " ", args)
	return sqlBuffer.String(), args.args
}

func (this *where) Append(cs ...Clauser) *where {
	for _, c := range cs {
		if c != nil {
			this.children = append(this.children, c)
		}
	}
	return this
}

//func (this *where) And(cs ...*where) *where {
//	var w = NewWhere("", nil)
//	w.children = cs
//	return this
//
//	//var w *where
//	//for i, c := range cs {
//	//	//if i == 0 {
//	//		switch t := c.(type) {
//	//		case string:
//	//			if i == 0 {
//	//				w = NewWhere(t, nil)
//	//				w.prefix = "AND"
//	//				this.children = append(this.children, w)
//	//			} else {
//	//				w.args = append(w.args, t)
//	//			}
//	//		case *where:
//	//			t.prefix = "AND"
//	//			this.children = append(this.children, t)
//	//		case Clauser:
//	//			this.children = append(this.children, t)
//	//		default:
//	//			w.args = append(w.args, t)
//	//		}
//	//	//}
//	//}
//	//return this
//}
//
//func (this *where) Or(sql string, args ...interface{}) *where {
//	var w = NewWhere(sql, args...)
//	w.prefix = "OR"
//	this.children = append(this.children, w)
//	return this
//}

// --------------------------------------------------------------------------------
func And(cs ...Clauser) *where {
	var w = &where{}
	w.children = cs
	w.prefix = "AND"
	return w
}

func Or(cs ...Clauser) *where {
	var w = &where{}
	w.children = cs
	w.prefix = "OR"
	return w
}
//
//func Not(cs ...Clauser) *where {
//	var w = &where{}
//	w.children = cs
//	w.prefix = "NOT"
//	return w
//}
