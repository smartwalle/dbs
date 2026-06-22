package mysql

import "github.com/smartwalle/dbs"

var _dialect = &dialect{}

const (
	kPlaceholder = '?'
)

func Dialect() dbs.Dialect {
	return _dialect
}

type dialect struct {
}

func (d *dialect) WritePlaceholder(w dbs.Writer, _ int) (err error) {
	if err = w.WriteByte(kPlaceholder); err != nil {
		return err
	}
	return nil
}
