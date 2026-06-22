package postgres

import (
	"strconv"

	"github.com/smartwalle/dbs"
)

var _dialect = &dialect{}

const (
	kPlaceholder = '$'
)

func Dialect() dbs.Dialect {
	return _dialect
}

type dialect struct {
}

func (d *dialect) WritePlaceholder(w dbs.Writer, idx int) (err error) {
	if err = w.WriteByte(kPlaceholder); err != nil {
		return err
	}
	if _, err = w.WriteString(strconv.Itoa(idx)); err != nil {
		return err
	}
	return nil
}
