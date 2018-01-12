package dbs


import (
	"io"
	"strings"
	"bytes"
)

const (
	k_WHERE_AND = "AND"
	k_WHERE_OR  = "OR"
	k_WHERE_NOT = "NOT"
)

// --------------------------------------------------------------------------------
type clause struct {
	sql      string
	args     []interface{}
	logic    string
	children []*clause
}

func (this *clause) appendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	var hasSQL = len(this.sql) > 0
	var hasChildren = len(this.children) > 0
	var err error

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
		args, err = e.appendToSQL(w, sep, args)
		if err != nil {
			return nil, err
		}
	}

	if hasChildren {
		io.WriteString(w, ")")
	}
	return args, err
}

func (this *clause) appendChildren(c ...*clause) {
	this.children = append(this.children, c...)
}

func (this *clause) ToSQL() (sql string, args []interface{}, err error) {
	var sqlBuffer = &bytes.Buffer{}
	args, err = this.appendToSQL(sqlBuffer, "", nil)
	return sqlBuffer.String(), args, err
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
func C(sql string, args ...interface{}) *clause {
	var w = &clause{}
	w.sql = sql
	w.args = args
	return w
}

func AND(c ...*clause) *clause {
	var w = &clause{}
	w.appendChildren(c...)
	w.logic = k_WHERE_AND
	return w
}

func OR(c ...*clause) *clause {
	var w = &clause{}
	w.appendChildren(c...)
	w.logic = k_WHERE_OR
	return w
}

func NOT(sql string, args ...interface{}) *clause {
	var w = &clause{}
	w.sql = sql
	w.args = args
	w.logic = k_WHERE_NOT
	return w
}
