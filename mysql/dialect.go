package mysql

import "github.com/smartwalle/dbs"

var _dialect = &dialect{}

func Dialect() dbs.Dialect {
	return _dialect
}

type dialect struct {
}

func (q *dialect) WritePlaceholder(w dbs.Writer, _ int) (err error) {
	if err = w.WriteByte('?'); err != nil {
		return err
	}
	return nil
}
