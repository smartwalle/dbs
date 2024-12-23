package dbs

import (
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

func (c Clause) Write(w Writer) (err error) {
	var offset = 0
	switch raw := c.sql.(type) {
	case SQLClause:
		if err = raw.Write(w); err != nil {
			return err
		}
	case string:
		var sql = raw
		var args = c.args

		for len(sql) > 0 {
			var pos = strings.Index(sql, "?")
			if pos == -1 {
				break
			}
			if _, err = w.WriteString(sql[:pos]); err != nil {
				return err
			}

			if len(args) > 0 {
				switch arg := args[0].(type) {
				case SQLClause:
					if err = arg.Write(w); err != nil {
						return err
					}
				default:
					var argValue = reflect.ValueOf(args[0])
					var argKind = argValue.Kind()
					if argKind == reflect.Slice || argKind == reflect.Array {
						for idx := 0; idx < argValue.Len(); idx++ {
							if idx != 0 {
								if _, err = w.WriteString(", "); err != nil {
									return err
								}
							}
							if err = w.WritePlaceholder(); err != nil {
								return err
							}
							w.WriteArguments(argValue.Index(idx).Interface())
						}
					} else {
						if err = w.WritePlaceholder(); err != nil {
							return err
						}
						w.WriteArguments(args[0])
					}
				}
				args = args[1:]
			} else {
				if err = w.WritePlaceholder(); err != nil {
					return err
				}
			}

			sql = sql[pos+1:]
			offset++
		}

		if len(sql) > 0 {
			if _, err = w.WriteString(sql); err != nil {
				return err
			}
		}
	default:
	}

	if offset < len(c.args) {
		// 1 - 返回错误
		//return errors.New("参数数量错误")

		// 2 - 将多余的参数直接追加到参数列表中
		w.WriteArguments(c.args[offset:]...)

		// 3 - 将多余的参数进行处理并追加到参数列表中
		//for _, arg := range c.args[offset:] {
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
	}
	return nil
}

func (c Clause) SQL() (string, []interface{}, error) {
	var buffer = getBuffer()
	defer putBuffer(buffer)

	if err := c.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

type Clauses struct {
	sep     string
	clauses []SQLClause
}

func NewClauses(sep string, clauses ...SQLClause) *Clauses {
	return &Clauses{sep: sep, clauses: clauses}
}

func (cs *Clauses) Write(w Writer) (err error) {
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
	return nil
}

func (cs *Clauses) SQL() (string, []interface{}, error) {
	var buffer = getBuffer()
	defer putBuffer(buffer)

	if err := cs.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (cs *Clauses) valid() bool {
	return cs != nil && len(cs.clauses) > 0
}

func (cs *Clauses) Append(sql interface{}, args ...interface{}) *Clauses {
	cs.clauses = append(cs.clauses, SQL(sql, args...))
	return cs
}
