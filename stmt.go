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
	AppendToSQL(w io.Writer, sep string, args *Args) error
	ToSQL() (string, []interface{}, error)
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

func (this *statement) AppendToSQL(w io.Writer, sep string, args *Args) error {
	if len(this.sql) > 0 {
		if _, err := io.WriteString(w, this.sql); err != nil {
			return err
		}
	}
	if len(this.args) > 0 && args != nil {
		args.Append(this.args...)
	}
	return nil
}

func (this *statement) Append(sql string, args ...interface{}) *statement {
	this.sql = this.sql + " " + sql
	this.args = append(this.args, args...)
	return this
}

func (this *statement) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	err := this.AppendToSQL(sqlBuffer, "", nil)
	return sqlBuffer.String(), this.args, err
}

// --------------------------------------------------------------------------------
type statements []Statement

func (this statements) AppendToSQL(w io.Writer, sep string, args *Args) error {
	for i, c := range this {
		if i != 0 {
			if _, err := io.WriteString(w, sep); err != nil {
				return err
			}
		}
		if err := c.AppendToSQL(w, sep, args); err != nil {
			return err
		}
	}
	return nil
}

func (this statements) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, "", args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type clauseStmt struct {
	sql  string
	args Statement
}

func Clause(sql string, s Statement) *clauseStmt {
	var c = &clauseStmt{}
	c.sql = sql
	c.args = s
	return c
}

func (this *clauseStmt) AppendToSQL(w io.Writer, sep string, args *Args) error {
	if len(this.sql) > 0 {
		if _, err := io.WriteString(w, this.sql); err != nil {
			return err
		}
	}
	if this.args != nil {
		if err := this.args.AppendToSQL(w, sep, args); err != nil {
			return err
		}
	}
	return nil
}

func (this *clauseStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, "", args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type setStmt struct {
	column string
	value  interface{}
}

func newSet(column string, value interface{}) *setStmt {
	return &setStmt{column, value}
}

func (this *setStmt) AppendToSQL(w io.Writer, sep string, args *Args) error {
	io.WriteString(w, this.column)
	io.WriteString(w, "=")
	switch tv := this.value.(type) {
	case Statement:
		if err := tv.AppendToSQL(w, "", args); err != nil {
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
	err := this.AppendToSQL(sqlBuffer, " ", args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type setStmts []Statement

func (this setStmts) AppendToSQL(w io.Writer, sep string, args *Args) error {
	for i, c := range this {
		if i != 0 {
			if _, err := io.WriteString(w, sep); err != nil {
				return err
			}
		}
		if err := c.AppendToSQL(w, "", args); err != nil {
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
type whereStmt struct {
	sql      string
	prefix   string
	args     []interface{}
	children []Statement
}

func (this *whereStmt) AppendToSQL(w io.Writer, sep string, args *Args) error {
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
		if _, err := io.WriteString(w, this.sql); err != nil {
			return err
		}
		for i, e := range this.children {
			if i != 0 || hasSQL {
				io.WriteString(w, this.prefix)
			}
			if err := e.AppendToSQL(w, sep, args); err != nil {
				return err
			}
		}
		if hasParen {
			io.WriteString(w, ")")
		}
	}
	return nil
}

func (this *whereStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, " ", args)
	return sqlBuffer.String(), args.values, err
}

func (this *whereStmt) Append(sql string, args ...interface{}) *whereStmt {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.Appends(stmt)
	}
	return this
}

func (this *whereStmt) Appends(sts ...Statement) *whereStmt {
	for _, c := range sts {
		if c != nil {
			this.children = append(this.children, c)
		}
	}
	return this
}

// --------------------------------------------------------------------------------
func parseStmt(sql interface{}, args ...interface{}) Statement {
	switch s := sql.(type) {
	case string:
		if len(strings.TrimSpace(s)) == 0 {
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

// --------------------------------------------------------------------------------
func AND(sts ...Statement) *whereStmt {
	var w = &whereStmt{}
	w.children = sts
	w.prefix = " AND "
	return w
}

func OR(sts ...Statement) *whereStmt {
	var w = &whereStmt{}
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
		c.what(parseStmt(what[0], what[1:]...))
	}
	return c
}

func (this *caseStmt) AppendToSQL(w io.Writer, sep string, args *Args) error {
	io.WriteString(w, "CASE ")
	if this.whatPart != nil {
		this.whatPart.AppendToSQL(w, " ", args)
	}

	for _, wp := range this.whenPart {
		io.WriteString(w, " WHEN ")
		wp.when.AppendToSQL(w, " ", args)
		io.WriteString(w, " THEN ")
		wp.then.AppendToSQL(w, " ", args)
	}

	if this.elsePart != nil {
		io.WriteString(w, " ELSE ")
		this.elsePart.AppendToSQL(w, " ", args)
	}

	io.WriteString(w, " END")
	return nil
}

func (this *caseStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, " ", args)
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
type aliasStmt struct {
	sql Statement
	alias string
}

func Alias(sql interface{}, alias string) *aliasStmt {
	var s = &aliasStmt{}
	s.sql = parseStmt(sql)
	s.alias = alias
	return s
}

func (this *aliasStmt) AppendToSQL(w io.Writer, sep string, args *Args) error {
	if this.sql != nil {
		io.WriteString(w, "(")
		this.sql.AppendToSQL(w, "", args)
		io.WriteString(w, ")")
	}
	io.WriteString(w, " AS ")
	io.WriteString(w, this.alias)
	return nil
}

func (this *aliasStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, " ", args)
	return sqlBuffer.String(), args.values, err
}
