package dbs

import (
	"database/sql/driver"
	"reflect"
	"strings"
)

type SQLClause interface {
	Write(w Writer) error

	SQL() (string, []interface{}, error)
}

type Clause struct {
	sql  interface{}
	args []interface{}
}

func NewClause(sql interface{}, args ...interface{}) Clause {
	return Clause{sql: sql, args: args}
}

func SQL(sql interface{}, args ...interface{}) Clause {
	return NewClause(sql, args...)
}

func (c Clause) Clone() Clause {
	var nc = Clause{}
	if raw, ok := c.sql.(SQLClause); ok {
		nc.sql = clone(raw)
	} else {
		nc.sql = c.sql
	}
	if len(c.args) > 0 {
		nc.args = make([]interface{}, len(c.args))
		for i, arg := range c.args {
			switch raw := arg.(type) {
			case SQLClause:
				nc.args[i] = clone(raw)
			default:
				nc.args[i] = arg
			}
		}
	}
	return nc
}

func (c Clause) Write(w Writer) (err error) {
	var args = c.args
	switch raw := c.sql.(type) {
	case SQLClause:
		if err = raw.Write(w); err != nil {
			return err
		}
	case string:
		if args, err = buildClause(w, raw, args); err != nil {
			return err
		}
	default:
	}

	if len(args) > 0 {
		// 1 - 返回错误
		//return errors.New("参数数量错误")

		// 2 - 将多余的参数直接追加到参数列表中
		for _, arg := range args {
			if err = w.WriteArgument(FlagArgument, arg); err != nil {
				return err
			}
		}

		// 3 - 将多余的参数进行处理并追加到参数列表中
		//for _, arg := range args[offset:] {
		//	switch raw := arg.(type) {
		//	case SQLClause:
		//		//if err = w.WriteByte('('); err != nil {
		//		//	return err
		//		//}
		//		if err = raw.Write(w); err != nil {
		//			return err
		//		}
		//		//if err = w.WriteByte(')'); err != nil {
		//		//	return err
		//		//}
		//	default:
		//		w.WriteArguments(raw)
		//	}
		//}
		//
		// 4 - 直接丢弃
	}
	return nil
}

func (c Clause) SQL() (string, []interface{}, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	if err := c.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func buildArgument(w Writer, arg interface{}) (err error) {
	switch raw := arg.(type) {
	case SQLClause:
		if err = raw.Write(w); err != nil {
			return err
		}
	case driver.Valuer:
		var value driver.Value
		if value, err = raw.Value(); err != nil {
			return err
		}
		if err = w.WriteArgument(FlagPlaceholder|FlagArgument, value); err != nil {
			return err
		}
	default:
		var value = reflect.ValueOf(raw)
		var kind = value.Kind()
		if kind == reflect.Slice || kind == reflect.Array {
			for idx := 0; idx < value.Len(); idx++ {
				if idx != 0 {
					if err = w.WriteByte(','); err != nil {
						return err
					}
				}
				if err = w.WriteArgument(FlagPlaceholder|FlagArgument, value.Index(idx).Interface()); err != nil {
					return err
				}
			}
		} else {
			if err = w.WriteArgument(FlagPlaceholder|FlagArgument, raw); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildClause(w Writer, sql string, args []interface{}) ([]interface{}, error) {
	var err error

	for len(sql) > 0 {
		var pos = strings.IndexByte(sql, '?')
		if pos == -1 {
			break
		}
		if _, err = w.WriteString(sql[:pos]); err != nil {
			return nil, err
		}

		if len(args) > 0 {
			if err = buildArgument(w, args[0]); err != nil {
				return nil, err
			}
			args = args[1:]
		} else {
			if err = w.WriteArgument(FlagPlaceholder, nil); err != nil {
				return nil, err
			}
		}

		sql = sql[pos+1:]
	}

	if len(sql) > 0 {
		if _, err = w.WriteString(sql); err != nil {
			return nil, err
		}
	}
	return args, nil
}

type Clauses struct {
	sep     byte
	clauses []SQLClause
}

func NewClauses(sep byte, clauses ...SQLClause) *Clauses {
	return &Clauses{sep: sep, clauses: clauses}
}

func (cs *Clauses) Clone() *Clauses {
	if cs == nil {
		return nil
	}
	var ncs = &Clauses{}
	ncs.sep = cs.sep
	if len(cs.clauses) > 0 {
		ncs.clauses = make([]SQLClause, len(cs.clauses))
		for i, c := range cs.clauses {
			ncs.clauses[i] = clone(c)
		}
	}
	return ncs
}

func (cs *Clauses) Write(w Writer) (err error) {
	for idx, clause := range cs.clauses {
		if idx != 0 {
			if err = w.WriteByte(cs.sep); err != nil {
				return err
			}
		}
		if err = clause.Write(w); err != nil {
			return err
		}
	}
	return nil
}

func (cs *Clauses) SQL() (string, []interface{}, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	if err := cs.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (cs *Clauses) valid() bool {
	return cs != nil && len(cs.clauses) > 0
}

func (cs *Clauses) reset() {
	if cs != nil {
		cs.clauses = cs.clauses[:0]
	}
}

func (cs *Clauses) Append(sql interface{}, args ...interface{}) *Clauses {
	if raw, ok := sql.(SQLClause); ok {
		cs.clauses = append(cs.clauses, raw)
	} else {
		cs.clauses = append(cs.clauses, SQL(sql, args...))
	}
	return cs
}

type Conds struct {
	ignoreBracket bool
	sep           string
	clauses       []SQLClause
}

func NewConds(sep string, clauses ...SQLClause) *Conds {
	return &Conds{sep: sep, clauses: clauses}
}

func (cs *Conds) Clone() *Conds {
	if cs == nil {
		return nil
	}
	var ncs = &Conds{}
	ncs.ignoreBracket = cs.ignoreBracket
	ncs.sep = cs.sep
	if len(cs.clauses) > 0 {
		ncs.clauses = make([]SQLClause, len(cs.clauses))
		for i, c := range cs.clauses {
			ncs.clauses[i] = clone(c)
		}
	}
	return ncs
}

func (cs *Conds) Write(w Writer) (err error) {
	var n = len(cs.clauses)
	if n == 0 {
		return nil
	}

	if n > 1 && !cs.ignoreBracket {
		if err = w.WriteByte('('); err != nil {
			return err
		}
	}
	for idx, clause := range cs.clauses {
		if idx != 0 {
			if _, err = w.WriteString(cs.sep); err != nil {
				return err
			}
		}
		if err = clause.Write(w); err != nil {
			return err
		}
	}
	if n > 1 && !cs.ignoreBracket {
		if err = w.WriteByte(')'); err != nil {
			return err
		}
	}
	return nil
}

func (cs *Conds) SQL() (string, []interface{}, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	if err := cs.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (cs *Conds) valid() bool {
	return cs != nil && len(cs.clauses) > 0
}

func (cs *Conds) reset() {
	if cs != nil {
		cs.clauses = cs.clauses[:0]
	}
}

func (cs *Conds) Append(sql interface{}, args ...interface{}) *Conds {
	if raw, ok := sql.(SQLClause); ok {
		cs.clauses = append(cs.clauses, raw)
	} else {
		cs.clauses = append(cs.clauses, SQL(sql, args...))
	}
	return cs
}

func AND(clauses ...SQLClause) *Conds {
	return NewConds(" AND ", clauses...)
}

func OR(clauses ...SQLClause) *Conds {
	return NewConds(" OR ", clauses...)
}

type Set struct {
	column string
	value  interface{}
}

func NewSet(column string, value interface{}) Set {
	return Set{column: column, value: value}
}

func (sc Set) Clone() Set {
	var nsc = Set{}
	nsc.column = sc.column
	if raw, ok := sc.value.(SQLClause); ok {
		nsc.value = clone(raw)
	} else {
		nsc.value = sc.value
	}
	return nsc
}

func (sc Set) Write(w Writer) (err error) {
	if _, err = w.WriteString(sc.column); err != nil {
		return err
	}
	if err = w.WriteByte('='); err != nil {
		return err
	}

	switch raw := sc.value.(type) {
	case SQLClause:
		if err = raw.Write(w); err != nil {
			return err
		}
	default:
		if err = w.WriteArgument(FlagPlaceholder|FlagArgument, raw); err != nil {
			return err
		}
	}
	return nil
}

func (sc Set) SQL() (string, []interface{}, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	if err := sc.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

type Parts []string

func (ps Parts) Clone() Parts {
	var ncps = make([]string, len(ps))
	copy(ncps, ps)
	return ncps
}

func (ps Parts) Write(w Writer) (err error) {
	for idx, s := range ps {
		if len(s) == 0 {
			continue
		}

		if idx != 0 {
			if err = w.WriteByte(','); err != nil {
				return err
			}
		}
		if _, err = w.WriteString(s); err != nil {
			return err
		}
	}
	return nil
}

func (ps Parts) SQL() (string, []interface{}, error) {
	var buffer = NewBuffer()
	defer buffer.Release()

	if err := ps.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func clone(clause SQLClause) SQLClause {
	if clause == nil {
		return nil
	}
	switch raw := clause.(type) {
	case *Clauses:
		return raw.Clone()
	case *Conds:
		return raw.Clone()
	case Clause:
		return raw.Clone()
	case Set:
		return raw.Clone()
	case Parts:
		return raw.Clone()
	default:
		return clause
	}
}
