package dbs

import (
	"bytes"
	"io"
	"reflect"
	"strings"
)

const (
	k_WHERE_AND = "AND"
	k_WHERE_OR  = "OR"
	k_WHERE_NOT = "NOT"
	k_WHERE_IN  = "IN"
)

// --------------------------------------------------------------------------------
type Clause interface {
	appendToSQL(w io.Writer, sep string, args []interface{}) []interface{}
	Append(c ...Clause) Clause
	AppendStmt(sql string, args ...interface{}) Clause
	ToSQL() (sql string, args []interface{})
}

// --------------------------------------------------------------------------------
type rawClause struct {
	sql  interface{}
	args []interface{}
}

func (this *rawClause) ToSQL() (sql string, args []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	args = this.appendToSQL(sqlBuffer, "", nil)
	return sqlBuffer.String(), args
}

func (this *rawClause) appendToSQL(w io.Writer, sep string, args []interface{}) []interface{} {
	switch t := this.sql.(type) {
	case Clause:
		args = t.appendToSQL(w, sep, args)
	case string:
		io.WriteString(w, t)
		args = append(args, this.args...)
	case nil:
	default:
	}
	return args
}

func (this *rawClause) Append(c ...Clause) Clause {
	return this
}

func (this *rawClause) AppendStmt(sql string, args ...interface{}) Clause {
	return this
}

// --------------------------------------------------------------------------------
type logicClause struct {
	sql      string
	args     []interface{}
	logic    string
	children []Clause
}

func (this *logicClause) ToSQL() (sql string, args []interface{}) {
	var sqlBuffer = &bytes.Buffer{}
	args = this.appendToSQL(sqlBuffer, "", nil)
	return sqlBuffer.String(), args
}

func (this *logicClause) appendToSQL(w io.Writer, sep string, args []interface{}) []interface{} {
	var hasSQL = len(this.sql) > 0
	var hasChildren = len(this.children) > 0

	if hasChildren {
		io.WriteString(w, "(")
	}

	if hasSQL {
		if this.logic == k_WHERE_NOT {
			appendLogic(w, this.logic, "", " ")
		}
		io.WriteString(w, this.sql)
		if hasChildren && this.logic != k_WHERE_NOT {
			appendLogic(w, this.logic, " ", " ")
		}
	}
	if len(this.args) > 0 {
		args = append(args, this.args...)
	}

	for i, e := range this.children {
		if i != 0 {
			appendLogic(w, this.logic, " ", " ")
		}
		args = e.appendToSQL(w, sep, args)
	}

	if hasChildren {
		io.WriteString(w, ")")
	}
	return args
}

func (this *logicClause) Append(cs ...Clause) Clause {
	for _, c := range cs {
		if c != nil {
			this.children = append(this.children, c)
		}
	}
	return this
}

func (this *logicClause) AppendStmt(sql string, args ...interface{}) Clause {
	this.Append(C(sql, args...))
	return this
}

// --------------------------------------------------------------------------------
func appendLogic(w io.Writer, logic, prefix, suffix string) {
	var l = strings.TrimSpace(logic)
	if len(l) > 0 {
		io.WriteString(w, prefix)
		io.WriteString(w, l)
		io.WriteString(w, suffix)
	}
}

// --------------------------------------------------------------------------------
func C(sql interface{}, args ...interface{}) Clause {
	return &rawClause{sql, args}
}

func AND(c ...Clause) Clause {
	var w = &logicClause{}
	w.Append(c...)
	w.logic = k_WHERE_AND
	return w
}

func OR(c ...Clause) Clause {
	var w = &logicClause{}
	w.Append(c...)
	w.logic = k_WHERE_OR
	return w
}

func NOT(sql string, args ...interface{}) Clause {
	var w = &logicClause{}
	w.sql = sql
	w.args = args
	w.logic = k_WHERE_NOT
	return w
}

func IN(sql string, args interface{}) Clause {
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

	var w = &logicClause{}
	w.sql = sql
	w.args = params
	return w
}
