package dbs

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

func placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat(", ?", count)[2:]
}

type SQLValue interface {
	SQLValue() string
}

type Statement interface {
	Write(w Writer) error
	SQL() (string, []interface{}, error)
}

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

func (this *statement) Write(w Writer) error {
	switch ts := this.sql.(type) {
	case Statement:
		if err := ts.Write(w); err != nil {
			return err
		}
	case string:
		if _, err := w.WriteString(ts); err != nil {
			return err
		}
	default:
	}

	for _, arg := range this.args {
		switch ta := arg.(type) {
		case Statement:
			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if err := ta.Write(w); err != nil {
				return err
			}
			if _, err := w.WriteString(")"); err != nil {
				return err
			}
		default:
			w.WriteArgs(ta)
		}
	}
	return nil
}

func (this *statement) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

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

func (this *aliasStmt) Write(w Writer) error {
	switch ts := this.sql.(type) {
	case Statement:
		if _, err := w.WriteString("("); err != nil {
			return err
		}
		if err := ts.Write(w); err != nil {
			return err
		}
		if _, err := w.WriteString(")"); err != nil {
			return err
		}
	case string:
		if _, err := w.WriteString(ts); err != nil {
			return err
		}
	default:
	}

	if _, err := w.WriteString(" AS "); err != nil {
		return err
	}
	if _, err := w.WriteString(this.alias); err != nil {
		return err
	}
	return nil
}

func (this *aliasStmt) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type whenStmt struct {
	when Statement
	then Statement
	args []interface{}
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
	//case 1:
	//	c.what(what[0])
	default:
		c.what(what[0])
	}
	return c
}

func (this *caseStmt) Write(w Writer) error {
	if _, err := w.WriteString("CASE"); err != nil {
		return err
	}
	if this.whatPart != nil {
		if _, err := w.WriteString(" "); err != nil {
			return err
		}
		if err := this.whatPart.Write(w); err != nil {
			return err
		}
	}

	for _, wp := range this.whenPart {
		if _, err := w.WriteString(" WHEN "); err != nil {
			return err
		}
		if err := wp.when.Write(w); err != nil {
			return err
		}
		if _, err := w.WriteString(" THEN "); err != nil {
			return err
		}
		if err := wp.then.Write(w); err != nil {
			return err
		}
		w.WriteArgs(wp.args...)
	}

	if this.elsePart != nil {
		if _, err := w.WriteString(" ELSE "); err != nil {
			return err
		}
		if err := this.elsePart.Write(w); err != nil {
			return err
		}
	}

	if _, err := w.WriteString(" END"); err != nil {
		return err
	}
	return nil
}

func (this *caseStmt) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (this *caseStmt) what(what interface{}) *caseStmt {
	this.whatPart = parseStmt(what)
	return this
}

func (this *caseStmt) When(when, then interface{}, args ...interface{}) *caseStmt {
	this.whenPart = append(this.whenPart, whenStmt{parseStmt(when), parseStmt(then), args})
	return this
}

func (this *caseStmt) Else(sql interface{}, args ...interface{}) *caseStmt {
	this.elsePart = parseStmt(sql, args...)
	return this
}

type setStmt struct {
	column string
	value  interface{}
}

func newSet(column string, value interface{}) *setStmt {
	return &setStmt{column, value}
}

func (this *setStmt) Write(w Writer) error {
	w.WriteString(this.column)
	w.WriteString("=")
	switch tv := this.value.(type) {
	case Statement:
		if err := tv.Write(w); err != nil {
			return err
		}
	default:
		w.WriteString("?")
		w.WriteArgs(this.value)
	}
	return nil
}

func (this *setStmt) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type setStmts []*setStmt

func (this setStmts) Write(w Writer, sep string) error {
	for i, c := range this {
		if i != 0 {
			if _, err := w.WriteString(sep); err != nil {
				return err
			}
		}
		if err := c.Write(w); err != nil {
			return err
		}
	}
	return nil
}

func (this setStmts) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf, ", ")
	return sqlBuf.String(), sqlBuf.Values(), err
}

type statements []Statement

func (this statements) Write(w Writer, sep string) error {
	for i, stmt := range this {
		if i != 0 {
			if _, err := w.WriteString(sep); err != nil {
				return err
			}
		}
		switch st := stmt.(type) {
		case *whereStmt:
			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if err := st.Write(w); err != nil {
				return err
			}
			if _, err := w.WriteString(")"); err != nil {
				return err
			}
		default:
			if err := st.Write(w); err != nil {
				return err
			}
		}
		//if err := stmt.Write(w, args); err != nil {
		//	return err
		//}
	}
	return nil
}

func (this statements) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf, ", ")
	return sqlBuf.String(), sqlBuf.Values(), err
}

type whereStmt struct {
	stmts statements
	sep   string
}

func (this *whereStmt) Write(w Writer) error {
	for i, stmt := range this.stmts {
		if i != 0 {
			if _, err := w.WriteString(this.sep); err != nil {
				return err
			}
		}

		switch st := stmt.(type) {
		case *whereStmt:
			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if err := st.Write(w); err != nil {
				return err
			}
			if _, err := w.WriteString(")"); err != nil {
				return err
			}
		default:
			if err := st.Write(w); err != nil {
				return err
			}
		}

	}
	return nil
}

func (this *whereStmt) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
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

func in(sql, exp string, args interface{}) Statement {
	if len(sql) == 0 {
		return nil
	}

	var params []interface{}
	if args == nil {
		sql = fmt.Sprintf("%s %s (%s)", sql, exp, placeholders(len(params)))
	} else {
		var pValue = reflect.ValueOf(args)
		var pKind = pValue.Kind()

		if pKind == reflect.Array || pKind == reflect.Slice {
			var l = pValue.Len()
			params = make([]interface{}, l)
			for i := 0; i < l; i++ {
				params[i] = pValue.Index(i).Interface()
			}
			sql = fmt.Sprintf("%s %s (%s)", sql, exp, placeholders(len(params)))
		} else {
			switch args.(type) {
			case Statement:
				sql = fmt.Sprintf("%s %s ", sql, exp)
				params = append(params, args)
			}
		}
	}

	var s = &statement{}
	s.sql = sql
	s.args = params
	return s
}

func IN(sql string, args interface{}) Statement {
	return in(sql, "IN", args)
}

func NotIn(sql string, args interface{}) Statement {
	return in(sql, "NOT IN", args)
}

func parseStmt(sql interface{}, args ...interface{}) Statement {
	switch s := sql.(type) {
	case string:
		return NewStatement(s, args...)
	case Statement:
		return s
	default:
		return NewStatement(fmt.Sprintf("%v", sql), args...)
	}
	return nil
}

var isMap = map[bool]string{true: "IS", false: "IS NOT"}
var inMap = map[bool]string{true: "IN", false: "NOT IN"}
var eqMap = map[bool]string{true: "=", false: "<>"}

type Eq map[string]interface{}

func (this Eq) write(eq bool, w Writer) error {
	var index = 0
	for key, value := range this {
		if key == "" {
			continue
		}

		var stmt = ""
		if value == nil {
			stmt = fmt.Sprintf("%s %s NULL", key, isMap[eq])
		} else {
			var pValue = reflect.ValueOf(value)
			var pKind = pValue.Kind()
			if pKind == reflect.Array || pKind == reflect.Slice {
				if pValue.Len() > 0 {
					for i := 0; i < pValue.Len(); i++ {
						w.WriteArgs(pValue.Index(i).Interface())
					}
				}
				stmt = fmt.Sprintf("%s %s (%s)", key, inMap[eq], placeholders(pValue.Len()))
			} else {
				switch v := value.(type) {
				case Statement:
					sql, arg, err := v.SQL()
					if err != nil {
						return err
					}
					stmt = fmt.Sprintf("%s %s %s", key, eqMap[eq], sql)
					w.WriteArgs(arg...)
				default:
					stmt = fmt.Sprintf("%s %s ?", key, eqMap[eq])
					w.WriteArgs(value)
				}
			}
		}

		if stmt != "" {
			if index != 0 {
				if _, err := w.WriteString(" AND "); err != nil {
					return err
				}
			}

			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if _, err := w.WriteString(stmt); err != nil {
				return err
			}
			if _, err := w.WriteString(")"); err != nil {
				return err
			}
			index += 1
		}
	}
	return nil
}

func (this Eq) Write(w Writer) error {
	return this.write(true, w)
}

func (this Eq) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type NotEq Eq

func (this NotEq) Write(w Writer) error {
	return Eq(this).write(false, w)
}

func (this NotEq) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func like(sql, exp string, a ...interface{}) Statement {
	var buf = &bytes.Buffer{}
	fmt.Fprintf(buf, "%s %s ?", sql, exp)

	var s = &statement{}
	s.sql = buf.String()
	s.args = append(s.args, fmt.Sprint(a...))
	return s
}

func Like(sql string, args ...interface{}) Statement {
	return like(sql, "LIKE", args...)
}

func NotLike(sql string, args ...interface{}) Statement {
	return like(sql, "NOT LIKE", args...)
}
