package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const (
	kInsertBuilder = "InsertBuilder"
)

type InsertBuilder struct {
	builder
	prefixes Clauses
	options  Clauses
	columns  []string
	table    string
	values   [][]interface{}
	suffixes Clauses
	sb       *SelectBuilder
}

func (ib *InsertBuilder) Type() string {
	return kInsertBuilder
}

func (ib *InsertBuilder) UsePlaceholder(p Placeholder) *InsertBuilder {
	ib.builder.UsePlaceholder(p)
	if ib.sb != nil {
		ib.sb.UsePlaceholder(ib.placeholder)
	}
	return ib
}

func (ib *InsertBuilder) Prefix(clause string, args ...interface{}) *InsertBuilder {
	ib.prefixes = append(ib.prefixes, NewClause(clause, args...))
	return ib
}

func (ib *InsertBuilder) Options(options ...string) *InsertBuilder {
	for _, opt := range options {
		ib.options = append(ib.options, NewClause(opt))
	}
	return ib
}

func (ib *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	ib.columns = append(ib.columns, columns...)
	return ib
}

func (ib *InsertBuilder) Column(column string) *InsertBuilder {
	ib.columns = append(ib.columns, column)
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

func (ib *InsertBuilder) Suffix(clause interface{}, args ...interface{}) *InsertBuilder {
	var nClause = parseClause(clause, args...)
	if nClause != nil {
		ib.suffixes = append(ib.suffixes, nClause)
	}
	return ib
}

func (ib *InsertBuilder) SET(column string, value interface{}) *InsertBuilder {
	ib.columns = append(ib.columns, column)
	if len(ib.values) == 0 {
		ib.values = append(ib.values, make([]interface{}, 0))
	}
	ib.values[0] = append(ib.values[0], value)
	return ib
}

func (ib *InsertBuilder) Select(sb *SelectBuilder) *InsertBuilder {
	ib.sb = sb
	if ib.sb != nil {
		ib.sb.UsePlaceholder(ib.placeholder)
	}
	return ib
}

func (ib *InsertBuilder) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	if err := ib.Write(buf); err != nil {
		return "", nil, err
	}
	return ib.replace(buf.String()), buf.Values(), nil
}

func (ib *InsertBuilder) Write(w Writer) (err error) {
	if len(ib.table) == 0 {
		return errors.New("dbs: Insert clause must specify a table")
	}
	if len(ib.values) == 0 && ib.sb == nil {
		return errors.New("dbs: Insert clause must have at least one set of values")
	}

	if len(ib.prefixes) > 0 {
		if err = ib.prefixes.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("INSERT "); err != nil {
		return err
	}

	if len(ib.options) > 0 {
		if err = ib.options.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("INTO "); err != nil {
		return err
	}
	if _, err = w.WriteString(ib.quote(ib.table)); err != nil {
		return err
	}
	w.WriteString(" ")

	if len(ib.columns) > 0 {
		if _, err = w.WriteString("("); err != nil {
			return err
		}
		var ncs = make([]string, 0, len(ib.columns))
		for _, c := range ib.columns {
			ncs = append(ncs, ib.quote(c))
		}
		if _, err = w.WriteString(strings.Join(ncs, ", ")); err != nil {
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

		for index, elements := range ib.values {
			w.WriteString("(")
			for i, ele := range elements {
				switch vt := ele.(type) {
				case SQLClause:
					if err = vt.Write(w); err != nil {
						return err
					}
				default:
					w.WriteString("?")
					w.WriteArgs(ele)
				}
				if i < len(elements)-1 {
					w.WriteString(", ")
				}
			}
			w.WriteString(")")

			if index < len(ib.values)-1 {
				w.WriteString(", ")
			}
		}
	} else if ib.sb != nil {
		if _, err = w.WriteString(" ("); err != nil {
			return err
		}
		if err = ib.sb.Write(w); err != nil {
			return err
		}
		if _, err = w.WriteString(")"); err != nil {
			return err
		}
	}

	if len(ib.suffixes) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = ib.suffixes.Write(w, " "); err != nil {
			return err
		}
	}
	return nil
}

func (ib *InsertBuilder) reset() {
	ib.values = ib.values[:0]
}

func (ib *InsertBuilder) Exec(s Session) (sql.Result, error) {
	return exec(context.Background(), s, ib)
}

func (ib *InsertBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return exec(ctx, s, ib)
}

func (ib *InsertBuilder) Scan(s Session, dst interface{}) (err error) {
	return scan(context.Background(), s, ib, dst)
}

func (ib *InsertBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scan(ctx, s, ib, dst)
}

func (ib *InsertBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRow(context.Background(), s, ib, dst...)
}

func (ib *InsertBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRow(ctx, s, ib, dst...)
}

func (ib *InsertBuilder) Query(s Session) (*sql.Rows, error) {
	return query(context.Background(), s, ib)
}

func (ib *InsertBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return query(ctx, s, ib)
}

func NewInsertBuilder() *InsertBuilder {
	var ib = &InsertBuilder{}
	ib.placeholder = gPlaceholder
	return ib
}

func Insert(columns ...string) *InsertBuilder {
	var ib = NewInsertBuilder()
	ib.Columns(columns...)
	return ib
}
