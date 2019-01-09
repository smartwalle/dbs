package dbs

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// --------------------------------------------------------------------------------
func placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat(", ?", count)[2:]
}

// --------------------------------------------------------------------------------
type SQLValue interface {
	SQLValue() string
}

// --------------------------------------------------------------------------------
type Args struct {
	values []interface{}
}

func (this *Args) Append(args ...interface{}) {
	//this.values = append(this.values, args...)
	for _, v := range args {
		switch vt := v.(type) {
		case driver.Valuer:
			v, _ := vt.Value()
			this.values = append(this.values, v)
		case SQLValue:
			this.values = append(this.values, vt.SQLValue())
		default:
			this.values = append(this.values, v)
		}
	}
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
		args.Append(wp.args...)
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

func (this *caseStmt) When(when, then interface{}, args ...interface{}) *caseStmt {
	this.whenPart = append(this.whenPart, whenStmt{parseStmt(when), parseStmt(then), args})
	return this
}

func (this *caseStmt) Else(sql interface{}, args ...interface{}) *caseStmt {
	this.elsePart = parseStmt(sql, args...)
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
		if args != nil {
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
		//if err := stmt.AppendToSQL(w, args); err != nil {
		//	return err
		//}
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

// --------------------------------------------------------------------------------
func IN(sql string, args interface{}) Statement {
	return in(sql, "IN", args)
}

// --------------------------------------------------------------------------------
func NotIn(sql string, args interface{}) Statement {
	return in(sql, "NOT IN", args)
}

// --------------------------------------------------------------------------------
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

// --------------------------------------------------------------------------------
var isMap = map[bool]string{true: "IS", false: "IS NOT"}
var inMap = map[bool]string{true: "IN", false: "NOT IN"}
var eqMap = map[bool]string{true: "=", false: "<>"}

type Eq map[string]interface{}

func (this Eq) appendToSQL(eq bool, w io.Writer, args *Args) error {
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
						args.Append(pValue.Index(i).Interface())
					}
				}
				stmt = fmt.Sprintf("%s %s (%s)", key, inMap[eq], placeholders(pValue.Len()))
			} else {
				switch v := value.(type) {
				case Statement:
					sql, arg, err := v.ToSQL()
					if err != nil {
						return err
					}
					stmt = fmt.Sprintf("%s %s %s", key, eqMap[eq], sql)
					args.Append(arg...)
				default:
					stmt = fmt.Sprintf("%s %s ?", key, eqMap[eq])
					args.Append(value)
				}
			}
		}

		if stmt != "" {
			if index != 0 {
				if _, err := io.WriteString(w, " AND "); err != nil {
					return err
				}
			}

			if _, err := io.WriteString(w, "("); err != nil {
				return err
			}
			if _, err := io.WriteString(w, stmt); err != nil {
				return err
			}
			if _, err := io.WriteString(w, ")"); err != nil {
				return err
			}
			index += 1
		}
	}
	return nil
}

func (this Eq) AppendToSQL(w io.Writer, args *Args) error {
	return this.appendToSQL(true, w, args)
}

func (this Eq) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
type NotEq Eq

func (this NotEq) AppendToSQL(w io.Writer, args *Args) error {
	return Eq(this).appendToSQL(false, w, args)
}

func (this NotEq) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
}

// --------------------------------------------------------------------------------
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
