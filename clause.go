package dbs

import (
	"io"
	"strings"
	"bytes"
	"reflect"
)

const (
	k_WHERE_AND = "AND"
	k_WHERE_OR  = "OR"
	k_WHERE_NOT = "NOT"
	k_WHERE_IN  = "IN"
)

// --------------------------------------------------------------------------------
type Clause interface {
	AppendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error)
	Append(c ...Clause)
	ToSQL() (sql string, args []interface{}, err error)
}

type clause struct {
	sql      string
	args     []interface{}
	logic    string
	children []Clause
}

func (this *clause) AppendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
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
		args, err = e.AppendToSQL(w, sep, args)
		if err != nil {
			return nil, err
		}
	}

	if hasChildren {
		io.WriteString(w, ")")
	}
	return args, err
}

func (this *clause) Append(cs ...Clause) {
	for _, c := range cs {
		if c != nil {
			this.children = append(this.children, c)
		}
	}
}

func (this *clause) ToSQL() (sql string, args []interface{}, err error) {
	var sqlBuffer = &bytes.Buffer{}
	args, err = this.AppendToSQL(sqlBuffer, "", nil)
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
type rawSQL struct {
	sql interface{}
	args []interface{}
}

func SQL(sql interface{}, args ...interface{}) Clause {
	return &rawSQL{sql, args}
}

func (this *rawSQL) ToSQL() (sql string, args []interface{}, err error) {
	var sqlBuffer = &bytes.Buffer{}
	args, err = this.AppendToSQL(sqlBuffer, "", nil)
	return sqlBuffer.String(), args, err
}

func (this *rawSQL) AppendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	var err error
	switch t := this.sql.(type) {
	case Clause:
		args, err = t.AppendToSQL(w, sep, args)
		if err != nil {
			return nil, err
		}
	case string:
		io.WriteString(w, t)
		args = append(args, this.args...)
	case nil:
	default:
	}
	return args, nil
}

func (this *rawSQL) Append(c ...Clause) {
}

// --------------------------------------------------------------------------------
func AND(c ...Clause) Clause {
	var w = &clause{}
	w.Append(c...)
	w.logic = k_WHERE_AND
	return w
}

func OR(c ...Clause) Clause {
	var w = &clause{}
	w.Append(c...)
	w.logic = k_WHERE_OR
	return w
}

func NOT(sql string, args ...interface{}) Clause {
	var w = &clause{}
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

	var w = &clause{}
	w.sql = sql
	w.args = params
	return w
}