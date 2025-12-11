package dbs

type Dialect interface {
	WritePlaceholder(w Writer, idx int) error
}

var defaultDialect = &dialect{}

var globalDialect Dialect = defaultDialect

func UseDialect(dialect Dialect) {
	if dialect == nil {
		dialect = defaultDialect
	}
	globalDialect = dialect
}

func GlobalDialect() Dialect {
	return globalDialect
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
