package dbs

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const (
	kUnionBuilder = "UnionBuilder"
)

type UnionBuilder struct {
	builder
	clauses  []SQLClause
	orderBys []string
	limit    SQLClause
	offset   SQLClause
}

func (ub *UnionBuilder) Type() string {
	return kUnionBuilder
}

func (ub *UnionBuilder) UsePlaceholder(p Placeholder) *UnionBuilder {
	ub.builder.UsePlaceholder(p)
	return ub
}

func (ub *UnionBuilder) Union(clauses ...SQLClause) *UnionBuilder {
	ub.addClauses(" UNION ", clauses...)
	return ub
}

func (ub *UnionBuilder) UnionAll(clauses ...SQLClause) *UnionBuilder {
	ub.addClauses(" UNION ALL ", clauses...)
	return ub
}

func (ub *UnionBuilder) addClauses(prefix string, clauses ...SQLClause) {
	var first = len(ub.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			ub.clauses = append(ub.clauses, NewClause("", clause))
		} else {
			ub.clauses = append(ub.clauses, NewClause(prefix, clause))
		}
	}
}

func (ub *UnionBuilder) OrderBy(clause ...string) *UnionBuilder {
	ub.orderBys = append(ub.orderBys, clause...)
	return ub
}

func (ub *UnionBuilder) Limit(limit int64) *UnionBuilder {
	ub.limit = NewClause(" LIMIT ?", limit)
	return ub
}

func (ub *UnionBuilder) Offset(offset int64) *UnionBuilder {
	ub.offset = NewClause(" OFFSET ?", offset)
	return ub
}

func (ub *UnionBuilder) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	if err := ub.Write(buf); err != nil {
		return "", nil, err
	}

	clause, err := ub.replace(buf.String())
	if err != nil {
		return "", nil, err
	}
	return clause, buf.Values(), nil
}

func (ub *UnionBuilder) Write(w Writer) (err error) {
	if len(ub.clauses) < 2 {
		return errors.New("dbs: UNION clause must have at least two clause")
	}

	for _, clause := range ub.clauses {
		if err = clause.Write(w); err != nil {
			return err
		}
	}

	if len(ub.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(ub.orderBys, ", ")); err != nil {
			return err
		}
	}

	if ub.limit != nil {
		if err = ub.limit.Write(w); err != nil {
			return err
		}
	}

	if ub.offset != nil {
		if err = ub.offset.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func (ub *UnionBuilder) Scan(s Session, dst interface{}) (err error) {
	return scan(context.Background(), s, ub, dst)
}

func (ub *UnionBuilder) ScanContext(ctx context.Context, s Session, dst interface{}) (err error) {
	return scan(ctx, s, ub, dst)
}

func (ub *UnionBuilder) ScanRow(s Session, dst ...interface{}) (err error) {
	return scanRow(context.Background(), s, ub, dst...)
}

func (ub *UnionBuilder) ScanRowContext(ctx context.Context, s Session, dst ...interface{}) (err error) {
	return scanRow(ctx, s, ub, dst...)
}

func (ub *UnionBuilder) Query(s Session) (*sql.Rows, error) {
	return query(context.Background(), s, ub)
}

func (ub *UnionBuilder) QueryContext(ctx context.Context, s Session) (*sql.Rows, error) {
	return query(ctx, s, ub)
}

func NewUnionBuilder() *UnionBuilder {
	var sb = &UnionBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}
