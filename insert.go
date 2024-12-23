package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

type InsertBuilder struct {
	placeholder Placeholder
	session     Session
	prefixes    *Clauses
	options     *Clauses
	columns     []string
	table       string
	values      [][]interface{}
	suffixes    *Clauses
}

func NewInsertBuilder() *InsertBuilder {
	var ib = &InsertBuilder{}
	ib.placeholder = GlobalPlaceholder()
	return ib
}

func (ib *InsertBuilder) UsePlaceholder(p Placeholder) *InsertBuilder {
	ib.placeholder = p
	return ib
}

func (ib *InsertBuilder) UseSession(s Session) *InsertBuilder {
	ib.session = s
	return ib
}

func (ib *InsertBuilder) Prefix(sql interface{}, args ...interface{}) *InsertBuilder {
	if ib.prefixes == nil {
		ib.prefixes = NewClauses(" ")
	}
	ib.prefixes.Append(sql, args...)
	return ib
}

func (ib *InsertBuilder) Option(sql interface{}, args ...interface{}) *InsertBuilder {
	if ib.options == nil {
		ib.options = NewClauses(" ")
	}
	ib.options.Append(sql, args...)
	return ib
}

func (ib *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	ib.columns = append(ib.columns, columns...)
	return ib
}

func (ib *InsertBuilder) Table(table string) *InsertBuilder {
	ib.table = table
	return ib
}

func (ib *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	ib.values = append(ib.values, values)
	return ib
}

func (ib *InsertBuilder) Suffix(sql interface{}, args ...interface{}) *InsertBuilder {
	if ib.suffixes == nil {
		ib.suffixes = NewClauses(" ")
	}
	ib.suffixes.Append(sql, args...)
	return ib
}

func (ib *InsertBuilder) Write(w Writer) (err error) {
	if len(ib.table) == 0 {
		return errors.New("dbs: insert clause must specify a table")
	}
	if len(ib.values) == 0 {
		return errors.New("dbs: insert clause must have at least one set of values")
	}

	if ib.prefixes.valid() {
		if err = ib.prefixes.Write(w); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("INSERT "); err != nil {
		return err
	}

	if ib.options.valid() {
		if err = ib.options.Write(w); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("INTO "); err != nil {
		return err
	}
	if _, err = w.WriteString(ib.table); err != nil {
		return err
	}
	w.WriteString(" ")

	if len(ib.columns) > 0 {
		if _, err = w.WriteString("("); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(ib.columns, ", ")); err != nil {
			return err
		}
		if _, err = w.WriteString(")"); err != nil {
			return err
		}
	}

	if len(ib.values) > 0 {
		if _, err = w.WriteString(" VALUES "); err != nil {
			return err
		}
		for row, values := range ib.values {
			w.WriteString("(")
			for i, value := range values {
				switch vt := value.(type) {
				case SQLClause:
					if err = vt.Write(w); err != nil {
						return err
					}
				default:
					w.WritePlaceholder()
					w.WriteArguments(value)
				}
				if i < len(values)-1 {
					w.WriteString(", ")
				}
			}
			w.WriteString(")")

			if row < len(ib.values)-1 {
				w.WriteString(", ")
			}
		}
	}

	if ib.suffixes.valid() {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = ib.suffixes.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func (ib *InsertBuilder) SQL() (string, []interface{}, error) {
	var buffer = getBuffer()
	buffer.UsePlaceholder(ib.placeholder)

	defer putBuffer(buffer)

	if err := ib.Write(buffer); err != nil {
		return "", nil, err
	}
	return buffer.String(), buffer.Arguments(), nil
}

func (ib *InsertBuilder) Scan(ctx context.Context, dst interface{}) error {
	return scan(ctx, ib.session, ib, dst)
}

func (ib *InsertBuilder) Query(ctx context.Context) (*sql.Rows, error) {
	return query(ctx, ib.session, ib)
}

func (ib *InsertBuilder) Exec(ctx context.Context) (sql.Result, error) {
	return exec(ctx, ib.session, ib)
}
