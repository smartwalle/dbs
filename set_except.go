package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const (
	kExceptBuilder = "ExceptBuilder"
)

type ExceptBuilder struct {
	builder
	clauses  Clauses
	orderBys []string
	limit    SQLClause
	offset   SQLClause
}

func (eb *ExceptBuilder) Type() string {
	return kExceptBuilder
}

func (eb *ExceptBuilder) UsePlaceholder(p Placeholder) *ExceptBuilder {
	eb.builder.UsePlaceholder(p)
	return eb
}

func (eb *ExceptBuilder) Except(clauses ...SQLClause) *ExceptBuilder {
	eb.addClauses(" EXCEPT ", clauses...)
	return eb
}

func (eb *ExceptBuilder) ExceptAll(clauses ...SQLClause) *ExceptBuilder {
	eb.addClauses(" EXCEPT ALL ", clauses...)
	return eb
}

func (eb *ExceptBuilder) addClauses(prefix string, clauses ...SQLClause) {
	var first = len(eb.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			eb.clauses = append(eb.clauses, NewClause("", clause))
		} else {
			eb.clauses = append(eb.clauses, NewClause(prefix, clause))
		}
	}
}

func (eb *ExceptBuilder) OrderBy(clause ...string) *ExceptBuilder {
	eb.orderBys = append(eb.orderBys, clause...)
	return eb
}

func (eb *ExceptBuilder) Limit(limit int64) *ExceptBuilder {
	eb.limit = NewClause(" LIMIT ?", limit)
	return eb
}

func (eb *ExceptBuilder) Offset(offset int64) *ExceptBuilder {
	eb.offset = NewClause(" OFFSET ?", offset)
	return eb
}

func (eb *ExceptBuilder) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	if err := eb.Write(buf); err != nil {
		return "", nil, err
	}
	return eb.replace(buf.String()), buf.Values(), nil
}

func (eb *ExceptBuilder) Write(w Writer) (err error) {
	if len(eb.clauses) < 2 {
		return errors.New("dbs: EXCEPT clause must have at least two clause")
	}

	for _, clause := range eb.clauses {
		if err = clause.Write(w); err != nil {
			return err
		}
	}

	if len(eb.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(eb.orderBys, ", ")); err != nil {
			return err
		}
	}

	if eb.limit != nil {
		if err = eb.limit.Write(w); err != nil {
			return err
		}
	}

	if eb.offset != nil {
		if err = eb.offset.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func (eb *ExceptBuilder) Scan(s Session, dst interface{}) (err error) {
	return scan(context.Background(), s, eb, dst)
}

func (eb *ExceptBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scan(ctx, s, eb, dst)
}

func (eb *ExceptBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRow(context.Background(), s, eb, dst...)
}

func (eb *ExceptBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRow(ctx, s, eb, dst...)
}

func (eb *ExceptBuilder) Query(s Session) (*sql.Rows, error) {
	return query(context.Background(), s, eb)
}

func (eb *ExceptBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return query(ctx, s, eb)
}

func NewExceptBuilder() *ExceptBuilder {
	var sb = &ExceptBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}
