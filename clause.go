package dbs

import (
	"strings"
)

type SQLClause interface {
	Write(w Writer) error

	SQL(p Placeholder) (string, []interface{}, error)
}

type Clause struct {
	Expr interface{}
	Args []interface{}
}

func NewClause(expr interface{}, args ...interface{}) Clause {
	return Clause{Expr: expr, Args: args}
}

func SQL(expr interface{}, args ...interface{}) Clause {
	return NewClause(expr, args...)
}

func (c Clause) Write(w Writer) (err error) {
	var offset = 0
	switch raw := c.Expr.(type) {
	case SQLClause:
		if err = raw.Write(w); err != nil {
			return err
		}
	case string:
		var expr = raw
		var args = c.Args

		for len(expr) > 0 {
			var pos = strings.Index(expr, "?")
			if pos == -1 {
				break
			}
			if _, err = w.WriteString(expr[:pos]); err != nil {
				return err
			}

			if len(args) > 0 {
				switch arg := args[0].(type) {
				case SQLClause:
					if err = arg.Write(w); err != nil {
						return err
					}
				default:
					if err = w.WritePlaceholder(); err != nil {
						return err
					}
					w.WriteArguments(args[0])
				}
				args = args[1:]
			} else {
				if err = w.WritePlaceholder(); err != nil {
					return err
				}
			}

			expr = expr[pos+1:]
			offset++
		}
	default:
	}

	if offset < len(c.Args) {
		// 1 - 返回错误
		//return errors.New("参数数量错误")

		// 2 - 将多余的参数直接追加到参数列表中
		w.WriteArguments(c.Args[offset:]...)

		// 3 - 将多余的参数进行处理并追加到参数列表中
		//for _, arg := range c.Args[offset:] {
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

func (c Clause) SQL(p Placeholder) (string, []interface{}, error) {
	var buffer = getBuffer(p)
	defer putBuffer(buffer)

	if err := c.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}
