package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const (
	kIntersectBuilder = "IntersectBuilder"
)

type IntersectBuilder struct {
	builder
	clauses  []SQLClause
	orderBys []string
	limit    SQLClause
	offset   SQLClause
}

func (ib *IntersectBuilder) Type() string {
	return kIntersectBuilder
}

func (ib *IntersectBuilder) UsePlaceholder(p Placeholder) *IntersectBuilder {
	ib.builder.UsePlaceholder(p)
	return ib
}

func (ib *IntersectBuilder) Intersect(clauses ...SQLClause) *IntersectBuilder {
	ib.addClauses(" INTERSECT ", clauses...)
	return ib
}

func (ib *IntersectBuilder) IntersectAll(clauses ...SQLClause) *IntersectBuilder {
	ib.addClauses(" INTERSECT ALL ", clauses...)
	return ib
}

func (ib *IntersectBuilder) addClauses(prefix string, clauses ...SQLClause) {
	var first = len(ib.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			ib.clauses = append(ib.clauses, NewClause("", clause))
		} else {
			ib.clauses = append(ib.clauses, NewClause(prefix, clause))
		}
	}
}

func (ib *IntersectBuilder) OrderBy(clause ...string) *IntersectBuilder {
	ib.orderBys = append(ib.orderBys, clause...)
	return ib
}

func (ib *IntersectBuilder) Limit(limit int64) *IntersectBuilder {
	ib.limit = NewClause(" LIMIT ?", limit)
	return ib
}

func (ib *IntersectBuilder) Offset(offset int64) *IntersectBuilder {
	ib.offset = NewClause(" OFFSET ?", offset)
	return ib
}

func (ib *IntersectBuilder) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	if err := ib.Write(buf); err != nil {
		return "", nil, err
	}

	clause, err := ib.replace(buf.String())
	if err != nil {
		return "", nil, err
	}
	return clause, buf.Values(), nil
}

func (ib *IntersectBuilder) Write(w Writer) (err error) {
	if len(ib.clauses) < 2 {
		return errors.New("dbs: INTERSECT clause must have at least two clause")
	}

	for _, clause := range ib.clauses {
		if err = clause.Write(w); err != nil {
			return err
		}
	}

	if len(ib.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(ib.orderBys, ", ")); err != nil {
			return err
		}
	}

	if ib.limit != nil {
		if err = ib.limit.Write(w); err != nil {
			return err
		}
	}

	if ib.offset != nil {
		if err = ib.offset.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func (ib *IntersectBuilder) Scan(s Session, dst interface{}) (err error) {
	return scan(context.Background(), s, ib, dst)
}

func (ib *IntersectBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scan(ctx, s, ib, dst)
}

func (ib *IntersectBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRow(context.Background(), s, ib, dst...)
}

func (ib *IntersectBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRow(ctx, s, ib, dst...)
}

func (ib *IntersectBuilder) Query(s Session) (*sql.Rows, error) {
	return query(context.Background(), s, ib)
}

func (ib *IntersectBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return query(ctx, s, ib)
}

func NewIntersectBuilder() *IntersectBuilder {
	var sb = &IntersectBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}
