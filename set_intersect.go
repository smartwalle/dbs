package dbs

import (
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

func (this *IntersectBuilder) Type() string {
	return kIntersectBuilder
}

func (this *IntersectBuilder) UsePlaceholder(p Placeholder) *IntersectBuilder {
	this.builder.UsePlaceholder(p)
	return this
}

func (this *IntersectBuilder) Intersect(clauses ...SQLClause) *IntersectBuilder {
	var first = len(this.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			this.clauses = append(this.clauses, NewClause("", clause))
		} else {
			this.clauses = append(this.clauses, NewClause(" INTERSECT ", clause))
		}
	}
	return this
}

func (this *IntersectBuilder) IntersectAll(clauses ...SQLClause) *IntersectBuilder {
	var first = len(this.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			this.clauses = append(this.clauses, NewClause("", clause))
		} else {
			this.clauses = append(this.clauses, NewClause(" INTERSECT ALL ", clause))
		}
	}
	return this
}

func (this *IntersectBuilder) OrderBy(sql ...string) *IntersectBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *IntersectBuilder) Limit(limit int64) *IntersectBuilder {
	this.limit = NewClause(" LIMIT ?", limit)
	return this
}

func (this *IntersectBuilder) Offset(offset int64) *IntersectBuilder {
	this.offset = NewClause(" OFFSET ?", offset)
	return this
}

func (this *IntersectBuilder) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	if err := this.Write(sqlBuf); err != nil {
		return "", nil, err
	}

	sql, err := this.replace(sqlBuf.String())
	if err != nil {
		return "", nil, err
	}
	return sql, sqlBuf.Values(), nil
}

func (this *IntersectBuilder) Write(w Writer) (err error) {
	if len(this.clauses) < 2 {
		return errors.New("dbs: INTERSECT clause must have at least two clause")
	}

	for _, clause := range this.clauses {
		if err = clause.Write(w); err != nil {
			return err
		}
	}

	if len(this.orderBys) > 0 {
		if _, err = w.WriteString(" ORDER BY "); err != nil {
			return err
		}
		if _, err = w.WriteString(strings.Join(this.orderBys, ", ")); err != nil {
			return err
		}
	}

	if this.limit != nil {
		if err = this.limit.Write(w); err != nil {
			return err
		}
	}

	if this.offset != nil {
		if err = this.offset.Write(w); err != nil {
			return err
		}
	}

	return nil
}

func NewIntersectBuilder() *IntersectBuilder {
	var sb = &IntersectBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}
