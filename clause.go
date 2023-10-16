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

type SQLClause interface {
	Write(w Writer) error
	SQL() (string, []interface{}, error)
}

type Clause struct {
	sql  interface{}
	args []interface{}
}

func NewClause(sql interface{}, args ...interface{}) *Clause {
	var s = &Clause{}
	s.sql = sql
	s.args = args
	return s
}

func SQL(sql string, args ...interface{}) *Clause {
	return NewClause(sql, args...)
}

func (this *Clause) Write(w Writer) error {
	switch ts := this.sql.(type) {
	case SQLClause:
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
		case SQLClause:
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

func (this *Clause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type pureClause string

func (this pureClause) Write(w Writer) error {
	_, err := w.WriteString(string(this))
	return err
}

func (this pureClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type aliasClause struct {
	sql   interface{}
	alias string
}

func Alias(sql interface{}, alias string) *aliasClause {
	var s = &aliasClause{}
	s.sql = sql
	s.alias = alias
	return s
}

func (this *aliasClause) Write(w Writer) error {
	switch ts := this.sql.(type) {
	case SQLClause:
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

func (this *aliasClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type whenClause struct {
	when SQLClause
	then SQLClause
	args []interface{}
}

type caseClause struct {
	whatClause SQLClause
	whenClause []whenClause
	elseClause SQLClause
}

func Case(what ...interface{}) *caseClause {
	var c = &caseClause{}
	switch len(what) {
	case 0:
	default:
		c.what(what[0])
	}
	return c
}

func (this *caseClause) Write(w Writer) error {
	if _, err := w.WriteString("CASE"); err != nil {
		return err
	}
	if this.whatClause != nil {
		if _, err := w.WriteString(" "); err != nil {
			return err
		}
		if err := this.whatClause.Write(w); err != nil {
			return err
		}
	}

	for _, wp := range this.whenClause {
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

	if this.elseClause != nil {
		if _, err := w.WriteString(" ELSE "); err != nil {
			return err
		}
		if err := this.elseClause.Write(w); err != nil {
			return err
		}
	}

	if _, err := w.WriteString(" END"); err != nil {
		return err
	}
	return nil
}

func (this *caseClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (this *caseClause) what(what interface{}) *caseClause {
	this.whatClause = parseClause(what)
	return this
}

func (this *caseClause) When(when, then interface{}, args ...interface{}) *caseClause {
	this.whenClause = append(this.whenClause, whenClause{parseClause(when), parseClause(then), args})
	return this
}

func (this *caseClause) Else(sql interface{}, args ...interface{}) *caseClause {
	this.elseClause = parseClause(sql, args...)
	return this
}

type setClause struct {
	column string
	value  interface{}
}

func newSet(column string, value interface{}) *setClause {
	return &setClause{column, value}
}

func (this *setClause) Write(w Writer) error {
	w.WriteString(this.column)
	w.WriteString("=")
	switch tv := this.value.(type) {
	case SQLClause:
		if err := tv.Write(w); err != nil {
			return err
		}
	default:
		w.WriteString("?")
		w.WriteArgs(this.value)
	}
	return nil
}

func (this *setClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type setClauses []*setClause

func (this setClauses) Write(w Writer, sep string) error {
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

func (this setClauses) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf, ", ")
	return sqlBuf.String(), sqlBuf.Values(), err
}

type Clauses []SQLClause

func (this Clauses) Write(w Writer, sep string) error {
	for i, clause := range this {
		if i != 0 {
			if _, err := w.WriteString(sep); err != nil {
				return err
			}
		}
		switch st := clause.(type) {
		case *whereClause:
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
		//if err := cache.Write(w, args); err != nil {
		//	return err
		//}
	}
	return nil
}

func (this Clauses) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf, ", ")
	return sqlBuf.String(), sqlBuf.Values(), err
}

type whereClause struct {
	clauses Clauses
	sep     string
}

func (this *whereClause) Write(w Writer) error {
	for i, clause := range this.clauses {
		if i != 0 {
			if _, err := w.WriteString(this.sep); err != nil {
				return err
			}
		}

		switch st := clause.(type) {
		case *whereClause:
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

func (this *whereClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (this *whereClause) Appends(clauses ...SQLClause) *whereClause {
	this.clauses = append(this.clauses, clauses...)
	return this
}

func (this *whereClause) Append(sql interface{}, args ...interface{}) *whereClause {
	var s = parseClause(sql, args...)
	if s != nil {
		this.clauses = append(this.clauses, s)
	}
	return this
}

func AND(clauses ...SQLClause) *whereClause {
	var s = &whereClause{}
	s.clauses = clauses
	s.sep = " AND "
	return s
}

func OR(clauses ...SQLClause) *whereClause {
	var s = &whereClause{}
	s.clauses = clauses
	s.sep = " OR "
	return s
}

func in(sql, exp string, args interface{}) SQLClause {
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
			case SQLClause:
				sql = fmt.Sprintf("%s %s ", sql, exp)
				params = append(params, args)
			}
		}
	}

	var s = &Clause{}
	s.sql = sql
	s.args = params
	return s
}

func IN(sql string, args interface{}) SQLClause {
	return in(sql, "IN", args)
}

func NotIn(sql string, args interface{}) SQLClause {
	return in(sql, "NOT IN", args)
}

func parseClause(sql interface{}, args ...interface{}) SQLClause {
	switch s := sql.(type) {
	case string:
		return NewClause(s, args...)
	case SQLClause:
		return s
	default:
		return NewClause(fmt.Sprintf("%v", sql), args...)
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

		var clause = ""
		if value == nil {
			clause = fmt.Sprintf("%s %s NULL", key, isMap[eq])
		} else {
			var pValue = reflect.ValueOf(value)
			var pKind = pValue.Kind()
			if pKind == reflect.Array || pKind == reflect.Slice {
				if pValue.Len() > 0 {
					for i := 0; i < pValue.Len(); i++ {
						w.WriteArgs(pValue.Index(i).Interface())
					}
				}
				clause = fmt.Sprintf("%s %s (%s)", key, inMap[eq], placeholders(pValue.Len()))
			} else {
				switch v := value.(type) {
				case SQLClause:
					sql, arg, err := v.SQL()
					if err != nil {
						return err
					}
					clause = fmt.Sprintf("%s %s %s", key, eqMap[eq], sql)
					w.WriteArgs(arg...)
				default:
					clause = fmt.Sprintf("%s %s ?", key, eqMap[eq])
					w.WriteArgs(value)
				}
			}
		}

		if clause != "" {
			if index != 0 {
				if _, err := w.WriteString(" AND "); err != nil {
					return err
				}
			}

			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if _, err := w.WriteString(clause); err != nil {
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

func like(sql, exp string, a ...interface{}) SQLClause {
	var buf = &bytes.Buffer{}
	fmt.Fprintf(buf, "%s %s ?", sql, exp)

	var s = &Clause{}
	s.sql = buf.String()
	s.args = append(s.args, fmt.Sprint(a...))
	return s
}

func Like(sql string, args ...interface{}) SQLClause {
	return like(sql, "LIKE", args...)
}

func NotLike(sql string, args ...interface{}) SQLClause {
	return like(sql, "NOT LIKE", args...)
}
