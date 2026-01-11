package dbs

type Dialect interface {
	WritePlaceholder(w Writer, idx int) error
}

var defaultDialect = &dialect{}

var _dialect Dialect = defaultDialect

func UseDialect(dialect Dialect) {
	if dialect == nil {
		dialect = defaultDialect
	}
	_dialect = dialect
}

func GetDialect() Dialect {
	return _dialect
}

func DefaultDialect() Dialect {
	return defaultDialect
}

type dialect struct {
}

func (q *dialect) WritePlaceholder(w Writer, _ int) (err error) {
	if err = w.WriteByte('?'); err != nil {
		return err
	}
	return nil
}
