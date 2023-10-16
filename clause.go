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
	return &Clause{sql: sql, args: args}
}

func SQL(sql string, args ...interface{}) *Clause {
	return NewClause(sql, args...)
}

func (clause *Clause) Write(w Writer) error {
	switch sqlType := clause.sql.(type) {
	case SQLClause:
		if err := sqlType.Write(w); err != nil {
			return err
		}
	case string:
		if _, err := w.WriteString(sqlType); err != nil {
			return err
		}
	default:
	}

	for _, arg := range clause.args {
		switch argType := arg.(type) {
		case SQLClause:
			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if err := argType.Write(w); err != nil {
				return err
			}
			if _, err := w.WriteString(")"); err != nil {
				return err
			}
		default:
			w.WriteArgs(argType)
		}
	}
	return nil
}

func (clause *Clause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type pureClause string

func (clause pureClause) Write(w Writer) error {
	_, err := w.WriteString(string(clause))
	return err
}

func (clause pureClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type aliasClause struct {
	sql   interface{}
	alias string
}

func Alias(sql interface{}, alias string) *aliasClause {
	return &aliasClause{sql: sql, alias: alias}
}

func (clause *aliasClause) Write(w Writer) error {
	switch ts := clause.sql.(type) {
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
	if _, err := w.WriteString(clause.alias); err != nil {
		return err
	}
	return nil
}

func (clause *aliasClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type whenClause struct {
	when SQLClause
	then SQLClause
	args []interface{}
}

type caseClause struct {
	whatClause  SQLClause
	whenClauses []whenClause
	elseClause  SQLClause
}

func Case(what ...interface{}) *caseClause {
	var clause = &caseClause{}
	switch len(what) {
	case 0:
	default:
		clause.what(what[0])
	}
	return clause
}

func (clause *caseClause) Write(w Writer) error {
	if _, err := w.WriteString("CASE"); err != nil {
		return err
	}
	if clause.whatClause != nil {
		if _, err := w.WriteString(" "); err != nil {
			return err
		}
		if err := clause.whatClause.Write(w); err != nil {
			return err
		}
	}

	for _, when := range clause.whenClauses {
		if _, err := w.WriteString(" WHEN "); err != nil {
			return err
		}
		if err := when.when.Write(w); err != nil {
			return err
		}
		if _, err := w.WriteString(" THEN "); err != nil {
			return err
		}
		if err := when.then.Write(w); err != nil {
			return err
		}
		w.WriteArgs(when.args...)
	}

	if clause.elseClause != nil {
		if _, err := w.WriteString(" ELSE "); err != nil {
			return err
		}
		if err := clause.elseClause.Write(w); err != nil {
			return err
		}
	}

	if _, err := w.WriteString(" END"); err != nil {
		return err
	}
	return nil
}

func (clause *caseClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (clause *caseClause) what(what interface{}) *caseClause {
	clause.whatClause = parseClause(what)
	return clause
}

func (clause *caseClause) When(when, then interface{}, args ...interface{}) *caseClause {
	clause.whenClauses = append(clause.whenClauses, whenClause{parseClause(when), parseClause(then), args})
	return clause
}

func (clause *caseClause) Else(sql interface{}, args ...interface{}) *caseClause {
	clause.elseClause = parseClause(sql, args...)
	return clause
}

type setClause struct {
	column string
	value  interface{}
}

func newSet(column string, value interface{}) *setClause {
	return &setClause{column, value}
}

func (clause *setClause) Write(w Writer) error {
	w.WriteString(clause.column)
	w.WriteString("=")
	switch tv := clause.value.(type) {
	case SQLClause:
		if err := tv.Write(w); err != nil {
			return err
		}
	default:
		w.WriteString("?")
		w.WriteArgs(clause.value)
	}
	return nil
}

func (clause *setClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type setClauses []*setClause

func (clause setClauses) Write(w Writer, sep string) error {
	for i, c := range clause {
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

func (clause setClauses) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf, ", ")
	return sqlBuf.String(), sqlBuf.Values(), err
}

type Clauses []SQLClause

func (clauses Clauses) Write(w Writer, sep string) error {
	for i, clause := range clauses {
		if i != 0 {
			if _, err := w.WriteString(sep); err != nil {
				return err
			}
		}
		switch clauseType := clause.(type) {
		case *whereClause:
			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if err := clauseType.Write(w); err != nil {
				return err
			}
			if _, err := w.WriteString(")"); err != nil {
				return err
			}
		default:
			if err := clauseType.Write(w); err != nil {
				return err
			}
		}
		//if err := cache.Write(w, args); err != nil {
		//	return err
		//}
	}
	return nil
}

func (clauses Clauses) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clauses.Write(sqlBuf, ", ")
	return sqlBuf.String(), sqlBuf.Values(), err
}

type whereClause struct {
	clauses Clauses
	sep     string
}

func (clause *whereClause) Write(w Writer) error {
	for i, ele := range clause.clauses {
		if i != 0 {
			if _, err := w.WriteString(clause.sep); err != nil {
				return err
			}
		}

		switch clauseType := ele.(type) {
		case *whereClause:
			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if err := clauseType.Write(w); err != nil {
				return err
			}
			if _, err := w.WriteString(")"); err != nil {
				return err
			}
		default:
			if err := clauseType.Write(w); err != nil {
				return err
			}
		}

	}
	return nil
}

func (clause *whereClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (clause *whereClause) Appends(clauses ...SQLClause) *whereClause {
	clause.clauses = append(clause.clauses, clauses...)
	return clause
}

func (clause *whereClause) Append(sql interface{}, args ...interface{}) *whereClause {
	var nClause = parseClause(sql, args...)
	if nClause != nil {
		clause.clauses = append(clause.clauses, nClause)
	}
	return clause
}

func AND(clauses ...SQLClause) *whereClause {
	return &whereClause{clauses: clauses, sep: " AND "}
}

func OR(clauses ...SQLClause) *whereClause {
	return &whereClause{clauses: clauses, sep: " OR "}
}

func in(sql, exp string, args interface{}) SQLClause {
	if len(sql) == 0 {
		return nil
	}

	var nArgs []interface{}
	if args == nil {
		sql = fmt.Sprintf("%s %s (%s)", sql, exp, placeholders(len(nArgs)))
	} else {
		var pValue = reflect.ValueOf(args)
		var pKind = pValue.Kind()

		if pKind == reflect.Array || pKind == reflect.Slice {
			var l = pValue.Len()
			nArgs = make([]interface{}, l)
			for i := 0; i < l; i++ {
				nArgs[i] = pValue.Index(i).Interface()
			}
			sql = fmt.Sprintf("%s %s (%s)", sql, exp, placeholders(len(nArgs)))
		} else {
			switch args.(type) {
			case SQLClause:
				sql = fmt.Sprintf("%s %s ", sql, exp)
				nArgs = append(nArgs, args)
			}
		}
	}

	return &Clause{sql: sql, args: nArgs}
}

func IN(sql string, args interface{}) SQLClause {
	return in(sql, "IN", args)
}

func NotIn(sql string, args interface{}) SQLClause {
	return in(sql, "NOT IN", args)
}

func parseClause(sql interface{}, args ...interface{}) SQLClause {
	switch sqlType := sql.(type) {
	case string:
		return NewClause(sqlType, args...)
	case SQLClause:
		return sqlType
	default:
		return NewClause(fmt.Sprintf("%v", sql), args...)
	}
	return nil
}

var isMap = map[bool]string{true: "IS", false: "IS NOT"}
var inMap = map[bool]string{true: "IN", false: "NOT IN"}
var eqMap = map[bool]string{true: "=", false: "<>"}

type Eq map[string]interface{}

func (clause Eq) write(eq bool, w Writer) error {
	var index = 0
	for key, value := range clause {
		if key == "" {
			continue
		}

		var nClause = ""
		if value == nil {
			nClause = fmt.Sprintf("%s %s NULL", key, isMap[eq])
		} else {
			var pValue = reflect.ValueOf(value)
			var pKind = pValue.Kind()
			if pKind == reflect.Array || pKind == reflect.Slice {
				if pValue.Len() > 0 {
					for i := 0; i < pValue.Len(); i++ {
						w.WriteArgs(pValue.Index(i).Interface())
					}
				}
				nClause = fmt.Sprintf("%s %s (%s)", key, inMap[eq], placeholders(pValue.Len()))
			} else {
				switch v := value.(type) {
				case SQLClause:
					sql, arg, err := v.SQL()
					if err != nil {
						return err
					}
					nClause = fmt.Sprintf("%s %s %s", key, eqMap[eq], sql)
					w.WriteArgs(arg...)
				default:
					nClause = fmt.Sprintf("%s %s ?", key, eqMap[eq])
					w.WriteArgs(value)
				}
			}
		}

		if nClause != "" {
			if index != 0 {
				if _, err := w.WriteString(" AND "); err != nil {
					return err
				}
			}

			if _, err := w.WriteString("("); err != nil {
				return err
			}
			if _, err := w.WriteString(nClause); err != nil {
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

func (clause Eq) Write(w Writer) error {
	return clause.write(true, w)
}

func (clause Eq) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

type NotEq Eq

func (clause NotEq) Write(w Writer) error {
	return Eq(clause).write(false, w)
}

func (clause NotEq) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func like(sql, exp string, a ...interface{}) SQLClause {
	var buf = &bytes.Buffer{}
	fmt.Fprintf(buf, "%s %s ?", sql, exp)

	var clause = &Clause{}
	clause.sql = buf.String()
	clause.args = append(clause.args, fmt.Sprint(a...))
	return clause
}

func Like(sql string, args ...interface{}) SQLClause {
	return like(sql, "LIKE", args...)
}

func NotLike(sql string, args ...interface{}) SQLClause {
	return like(sql, "NOT LIKE", args...)
}
