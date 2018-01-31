package dbs

import (
	"bytes"
	"io"
	"strings"
	"reflect"
	"fmt"
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
	AppendToSQL(w io.Writer, args *Args) error
	ToSQL() (string, []interface{}, error)
}

// --------------------------------------------------------------------------------
type statement struct {
	sql  interface{}
	args []interface{}
}

func NewStatement(sql interface{}, args ...interface{}) *statement {
	var s = &statement{}
	s.sql = sql
	s.args = args
	return s
}

func SQL(sql string, args ...interface{}) *statement {
	return NewStatement(sql, args...)
}

func (this *statement) AppendToSQL(w io.Writer, args *Args) error {
	switch ts := this.sql.(type) {
	case Statement:
		if err := ts.AppendToSQL(w, args); err != nil {
			return err
		}
	case string:
		if _, err := io.WriteString(w, ts); err != nil {
			return err
		}
	default:
	}

	for _, arg := range this.args {
		switch ta := arg.(type) {
		case Statement:
			if _, err := io.WriteString(w, "("); err != nil {
				return err
			}
			if err := ta.AppendToSQL(w, args); err != nil {
				return err
			}
			if _, err := io.WriteString(w, ")"); err != nil {
				return err
			}
		default:
			if args != nil {
				args.Append(ta)
			}
		}
	}
	return nil
}

func (this *statement) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type aliasStmt struct {
	sql   interface{}
	alias string
}

func Alias(sql interface{}, alias string) *aliasStmt {
	var s = &aliasStmt{}
	s.sql = sql
	s.alias = alias
	return s
}

func (this *aliasStmt) AppendToSQL(w io.Writer, args *Args) error {
	switch ts := this.sql.(type) {
	case Statement:
		if _, err := io.WriteString(w, "("); err != nil {
			return err
		}
		if err := ts.AppendToSQL(w, args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, ")"); err != nil {
			return err
		}
	case string:
		if _, err := io.WriteString(w, ts); err != nil {
			return err
		}
	default:
	}

	if _, err := io.WriteString(w, " AS "); err != nil {
		return err
	}
	if _, err := io.WriteString(w, this.alias); err != nil {
		return err
	}
	return nil
}

func (this *aliasStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type whenStmt struct {
	when Statement
	then Statement
}
type caseStmt struct {
	whatPart Statement
	whenPart []whenStmt
	elsePart Statement
}

func Case(what ...interface{}) *caseStmt {
	var c = &caseStmt{}
	switch len(what) {
	case 0:
	case 1:
		c.what(what[0])
	default:
		c.what(parseStmt(what[0]))
	}
	return c
}

func (this *caseStmt) AppendToSQL(w io.Writer, args *Args) error {
	if _, err := io.WriteString(w, "CASE "); err != nil {
		return err
	}
	if this.whatPart != nil {
		if err := this.whatPart.AppendToSQL(w, args); err != nil {
			return err
		}
	}

	for _, wp := range this.whenPart {
		if _, err := io.WriteString(w, " WHEN "); err != nil {
			return err
		}
		if err := wp.when.AppendToSQL(w, args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, " THEN "); err != nil {
			return err
		}
		if err := wp.then.AppendToSQL(w, args); err != nil {
			return err
		}
	}

	if this.elsePart != nil {
		if _, err := io.WriteString(w, " ELSE "); err != nil {
			return err
		}
		if err := this.elsePart.AppendToSQL(w, args); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(w, " END"); err != nil {
		return err
	}
	return nil
}

func (this *caseStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
}

func (this *caseStmt) what(what interface{}) *caseStmt {
	this.whatPart = parseStmt(what)
	return this
}

func (this *caseStmt) When(when, then interface{}) *caseStmt {
	this.whenPart = append(this.whenPart, whenStmt{parseStmt(when), parseStmt(then)})
	return this
}

func (this *caseStmt) Else(sql interface{}) *caseStmt {
	this.elsePart = parseStmt(sql)
	return this
}

// --------------------------------------------------------------------------------
type setStmt struct {
	column string
	value  interface{}
}

func newSet(column string, value interface{}) *setStmt {
	return &setStmt{column, value}
}

func (this *setStmt) AppendToSQL(w io.Writer, args *Args) error {
	io.WriteString(w, this.column)
	io.WriteString(w, "=")
	switch tv := this.value.(type) {
	case Statement:
		if err := tv.AppendToSQL(w, args); err != nil {
			return err
		}
	default:
		io.WriteString(w, "?")
		if this.value != nil && args != nil {
			args.Append(this.value)
		}
	}
	return nil
}

func (this *setStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type setStmts []*setStmt

func (this setStmts) AppendToSQL(w io.Writer, sep string, args *Args) error {
	for i, c := range this {
		if i != 0 {
			if _, err := io.WriteString(w, sep); err != nil {
				return err
			}
		}
		if err := c.AppendToSQL(w, args); err != nil {
			return err
		}
	}
	return nil
}

func (this setStmts) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, ", ", args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type statements []Statement

func (this statements) AppendToSQL(w io.Writer, sep string, args *Args) error {
	for i, stmt := range this {
		if i != 0 {
			if _, err := io.WriteString(w, sep); err != nil {
				return err
			}
		}
		if err := stmt.AppendToSQL(w, args); err != nil {
			return err
		}
	}
	return nil
}

func (this statements) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, ", ", args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type whereStmt struct {
	stmts statements
	sep   string
}

func (this *whereStmt) AppendToSQL(w io.Writer, args *Args) error {
	for i, stmt := range this.stmts {
		if i != 0 {
			if _, err := io.WriteString(w, this.sep); err != nil {
				return err
			}
		}

		switch st := stmt.(type) {
		case *whereStmt:
			if _, err := io.WriteString(w, "("); err != nil {
				return err
			}
			if err := st.AppendToSQL(w, args); err != nil {
				return err
			}
			if _, err := io.WriteString(w, ")"); err != nil {
				return err
			}
		default:
			if err := st.AppendToSQL(w, args); err != nil {
				return err
			}
		}

	}
	return nil
}

func (this *whereStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
}

func (this *whereStmt) Appends(stmts ...Statement) *whereStmt {
	this.stmts = append(this.stmts, stmts...)
	return this
}

func (this *whereStmt) Append(sql interface{}, args ...interface{}) *whereStmt {
	var s = parseStmt(sql, args...)
	if s != nil {
		this.stmts = append(this.stmts, s)
	}
	return this
}

func AND(stmts ...Statement) *whereStmt {
	var s = &whereStmt{}
	s.stmts = stmts
	s.sep = " AND "
	return s
}

func OR(stmts ...Statement) *whereStmt {
	var s = &whereStmt{}
	s.stmts = stmts
	s.sep = " OR "
	return s
}

// --------------------------------------------------------------------------------
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
		sql = fmt.Sprintf("%s IN (%s)", sql, strings.Repeat(", ?", len(params))[2:])
	}

	var st = &statement{}
	st.sql = sql
	st.args = params
	return st
}

// --------------------------------------------------------------------------------
func parseStmt(sql interface{}, args ...interface{}) Statement {
	switch s := sql.(type) {
	case string:
		if strings.TrimSpace(s) == "" {
			return nil
		}
		return NewStatement(s, args...)
	case Statement:
		return s
	default:
		return nil
	}
	return nil
}
