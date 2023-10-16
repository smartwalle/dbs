package dbs

import (
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
	var first = len(eb.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			eb.clauses = append(eb.clauses, NewClause("", clause))
		} else {
			eb.clauses = append(eb.clauses, NewClause(" EXCEPT ", clause))
		}
	}
	return eb
}

func (eb *ExceptBuilder) ExceptAll(clauses ...SQLClause) *ExceptBuilder {
	var first = len(eb.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			eb.clauses = append(eb.clauses, NewClause("", clause))
		} else {
			eb.clauses = append(eb.clauses, NewClause(" EXCEPT ALL ", clause))
		}
	}
	return eb
}

func (eb *ExceptBuilder) OrderBy(sql ...string) *ExceptBuilder {
	eb.orderBys = append(eb.orderBys, sql...)
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
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	if err := eb.Write(sqlBuf); err != nil {
		return "", nil, err
	}

	sql, err := eb.replace(sqlBuf.String())
	if err != nil {
		return "", nil, err
	}
	return sql, sqlBuf.Values(), nil
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

func NewExceptBuilder() *ExceptBuilder {
	var sb = &ExceptBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}
