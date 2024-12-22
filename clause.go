package dbs

import "strings"

type SQLClause interface {
	Write(w Writer) error

	SQL() (string, []interface{}, error)
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
	var idx = 0
	switch raw := c.Expr.(type) {
	case SQLClause:
		if err = raw.Write(w); err != nil {
			return err
		}
	case string:
		var expr = raw
		var args = c.Args

		for len(args) > 0 && len(expr) > 0 {
			var pos = strings.Index(expr, "?")
			if pos == -1 {
				break
			}
			switch arg := args[0].(type) {
			case SQLClause:
				w.WriteString(expr[:pos])
				if err = arg.Write(w); err != nil {
					return err
				}
			default:
				w.WriteString(expr[:pos+1])
				w.WriteArgs(args[0])
			}
			args = args[1:]
			expr = expr[pos+1:]
			idx++
		}
		w.WriteString(expr)
	default:
	}

	if idx < len(c.Args) {
		for _, arg := range c.Args[idx:] {
			switch raw := arg.(type) {
			case SQLClause:
				//if err = w.WriteByte('('); err != nil {
				//	return err
				//}
				if err = raw.Write(w); err != nil {
					return err
				}
				//if err = w.WriteByte(')'); err != nil {
				//	return err
				//}
			default:
				w.WriteArgs(raw)
			}
		}
	}
	return nil
}

func (c Clause) SQL() (string, []interface{}, error) {
	var buffer = getBuffer()
	defer putBuffer(buffer)

	if err := c.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Args(), nil
}
