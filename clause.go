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
	clause interface{}
	args   []interface{}
}

func NewClause(clause interface{}, args ...interface{}) *Clause {
	return &Clause{clause: clause, args: args}
}

func SQL(clause string, args ...interface{}) *Clause {
	return NewClause(clause, args...)
}

func (clause *Clause) Write(w Writer) error {
	switch clauseType := clause.clause.(type) {
	case SQLClause:
		if err := clauseType.Write(w); err != nil {
			return err
		}
	case string:
		if _, err := w.WriteString(clauseType); err != nil {
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
	var buf = getBuffer()
	defer buf.Release()

	err := clause.Write(buf)
	return buf.String(), buf.Values(), err
}

type pureClause string

func (clause pureClause) Write(w Writer) error {
	_, err := w.WriteString(string(clause))
	return err
}

func (clause pureClause) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	err := clause.Write(buf)
	return buf.String(), buf.Values(), err
}

type aliasClause struct {
	clause interface{}
	alias  string
}

func Alias(clause interface{}, alias string) *aliasClause {
	return &aliasClause{clause: clause, alias: alias}
}

func (clause *aliasClause) Write(w Writer) error {
	switch ts := clause.clause.(type) {
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
	var buf = getBuffer()
	defer buf.Release()

	err := clause.Write(buf)
	return buf.String(), buf.Values(), err
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

func (cc *caseClause) Write(w Writer) error {
	if _, err := w.WriteString("CASE"); err != nil {
		return err
	}
	if cc.whatClause != nil {
		if _, err := w.WriteString(" "); err != nil {
			return err
		}
		if err := cc.whatClause.Write(w); err != nil {
			return err
		}
	}

	for _, when := range cc.whenClauses {
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

	if cc.elseClause != nil {
		if _, err := w.WriteString(" ELSE "); err != nil {
			return err
		}
		if err := cc.elseClause.Write(w); err != nil {
			return err
		}
	}

	if _, err := w.WriteString(" END"); err != nil {
		return err
	}
	return nil
}

func (cc *caseClause) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	err := cc.Write(buf)
	return buf.String(), buf.Values(), err
}

func (cc *caseClause) what(what interface{}) *caseClause {
	cc.whatClause = parseClause(what)
	return cc
}

func (cc *caseClause) When(when, then interface{}, args ...interface{}) *caseClause {
	cc.whenClauses = append(cc.whenClauses, whenClause{parseClause(when), parseClause(then), args})
	return cc
}

func (cc *caseClause) Else(clause interface{}, args ...interface{}) *caseClause {
	cc.elseClause = parseClause(clause, args...)
	return cc
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
	var buf = getBuffer()
	defer buf.Release()

	err := clause.Write(buf)
	return buf.String(), buf.Values(), err
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
	var buf = getBuffer()
	defer buf.Release()

	err := clause.Write(buf, ", ")
	return buf.String(), buf.Values(), err
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
	var buf = getBuffer()
	defer buf.Release()

	err := clauses.Write(buf, ", ")
	return buf.String(), buf.Values(), err
}

type whereClause struct {
	clauses Clauses
	sep     string
}

func (where *whereClause) Write(w Writer) error {
	for i, ele := range where.clauses {
		if i != 0 {
			if _, err := w.WriteString(where.sep); err != nil {
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

func (where *whereClause) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	err := where.Write(buf)
	return buf.String(), buf.Values(), err
}

func (where *whereClause) Appends(clauses ...SQLClause) *whereClause {
	where.clauses = append(where.clauses, clauses...)
	return where
}

func (where *whereClause) Append(clause interface{}, args ...interface{}) *whereClause {
	var nClause = parseClause(clause, args...)
	if nClause != nil {
		where.clauses = append(where.clauses, nClause)
	}
	return where
}

func AND(clauses ...SQLClause) *whereClause {
	return &whereClause{clauses: clauses, sep: " AND "}
}

func OR(clauses ...SQLClause) *whereClause {
	return &whereClause{clauses: clauses, sep: " OR "}
}

func in(clause, exp string, args interface{}) SQLClause {
	if len(clause) == 0 {
		return nil
	}

	var nArgs []interface{}
	if args == nil {
		clause = clause + exp + "(" + placeholders(len(nArgs)) + ")"
	} else {
		var pValue = reflect.ValueOf(args)
		var pKind = pValue.Kind()

		if pKind == reflect.Array || pKind == reflect.Slice {
			var l = pValue.Len()
			nArgs = make([]interface{}, l)
			for i := 0; i < l; i++ {
				nArgs[i] = pValue.Index(i).Interface()
			}
			clause = clause + exp + "(" + placeholders(len(nArgs)) + ")"
		} else {
			switch args.(type) {
			case SQLClause:
				clause = clause + exp
				nArgs = append(nArgs, args)
			}
		}
	}

	return &Clause{clause: clause, args: nArgs}
}

func IN(clause string, args interface{}) SQLClause {
	return in(clause, " IN ", args)
}

func NotIn(clause string, args interface{}) SQLClause {
	return in(clause, " NOT IN ", args)
}

func parseClause(clause interface{}, args ...interface{}) SQLClause {
	switch clauseType := clause.(type) {
	case string:
		return NewClause(clauseType, args...)
	case SQLClause:
		return clauseType
	default:
		return NewClause(fmt.Sprintf("%v", clause), args...)
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

		if index != 0 {
			if _, err := w.WriteString(" AND "); err != nil {
				return err
			}
		}

		if _, err := w.WriteString("("); err != nil {
			return err
		}

		if value == nil {
			if _, err := w.WriteString(key); err != nil {
				return err
			}
			if _, err := w.WriteString(" "); err != nil {
				return err
			}
			if _, err := w.WriteString(isMap[eq]); err != nil {
				return err
			}
			if _, err := w.WriteString(" NULL"); err != nil {
				return err
			}
		} else {
			var pValue = reflect.ValueOf(value)
			var pKind = pValue.Kind()
			if pKind == reflect.Array || pKind == reflect.Slice {
				for i := 0; i < pValue.Len(); i++ {
					w.WriteArgs(pValue.Index(i).Interface())
				}
				if _, err := w.WriteString(key); err != nil {
					return err
				}
				if _, err := w.WriteString(" "); err != nil {
					return err
				}
				if _, err := w.WriteString(inMap[eq]); err != nil {
					return err
				}
				if _, err := w.WriteString(" ("); err != nil {
					return err
				}
				if _, err := w.WriteString(placeholders(pValue.Len())); err != nil {
					return err
				}
				if _, err := w.WriteString(")"); err != nil {
					return err
				}
			} else {
				switch v := value.(type) {
				case SQLClause:
					if _, err := w.WriteString(key); err != nil {
						return err
					}
					if _, err := w.WriteString(" "); err != nil {
						return err
					}
					if _, err := w.WriteString(eqMap[eq]); err != nil {
						return err
					}
					if _, err := w.WriteString(" "); err != nil {
						return err
					}
					if err := v.Write(w); err != nil {
						return err
					}
				default:
					if _, err := w.WriteString(key); err != nil {
						return err
					}
					if _, err := w.WriteString(" "); err != nil {
						return err
					}
					if _, err := w.WriteString(eqMap[eq]); err != nil {
						return err
					}
					if _, err := w.WriteString(" ?"); err != nil {
						return err
					}
					w.WriteArgs(value)
				}
			}
		}
		if _, err := w.WriteString(")"); err != nil {
			return err
		}

		index += 1
	}
	return nil
}

func (clause Eq) Write(w Writer) error {
	return clause.write(true, w)
}

func (clause Eq) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	err := clause.Write(buf)
	return buf.String(), buf.Values(), err
}

type NotEq Eq

func (clause NotEq) Write(w Writer) error {
	return Eq(clause).write(false, w)
}

func (clause NotEq) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	err := clause.Write(buf)
	return buf.String(), buf.Values(), err
}

func like(clause, exp string, args ...string) SQLClause {
	var buf = &bytes.Buffer{}
	buf.WriteString(clause)
	buf.WriteString(exp)
	buf.WriteString("?")

	var nClause = &Clause{}
	nClause.clause = buf.String()
	nClause.args = append(nClause.args, strings.Join(args, ""))
	return nClause
}

func Like(clause string, args ...string) SQLClause {
	return like(clause, " LIKE ", args...)
}

func NotLike(clause string, args ...string) SQLClause {
	return like(clause, " NOT LIKE ", args...)
}
