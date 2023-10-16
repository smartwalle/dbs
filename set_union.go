package dbs

import (
	"errors"
	"strings"
)

const (
	kUnionBuilder = "UnionBuilder"
)

type UnionBuilder struct {
	builder
	all      bool
	clauses  Clauses
	orderBys []string
	limit    SQLClause
	offset   SQLClause
}

func (this *UnionBuilder) Type() string {
	return kUnionBuilder
}

func (this *UnionBuilder) UsePlaceholder(p Placeholder) *UnionBuilder {
	this.builder.UsePlaceholder(p)
	return this
}

func (this *UnionBuilder) Clone() *UnionBuilder {
	var sb = *this
	return &sb
}

func (this *UnionBuilder) Union(clause ...SQLClause) *UnionBuilder {
	this.all = false
	this.clauses = clause
	return this
}

func (this *UnionBuilder) UnionAll(clause ...SQLClause) *UnionBuilder {
	this.all = true
	this.clauses = clause
	return this
}

func (this *UnionBuilder) OrderBy(sql ...string) *UnionBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *UnionBuilder) Limit(limit int64) *UnionBuilder {
	this.limit = NewClause(" LIMIT ?", limit)
	return this
}

func (this *UnionBuilder) Offset(offset int64) *UnionBuilder {
	this.offset = NewClause(" OFFSET ?", offset)
	return this
}

func (this *UnionBuilder) SQL() (string, []interface{}, error) {
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

func (this *UnionBuilder) Write(w Writer) (err error) {
	if len(this.clauses) < 2 {
		return errors.New("dbs: UNION clause must have at least two clause")
	}

	for i, clause := range this.clauses {
		if i > 0 {
			if this.all {
				w.WriteString(" UNION ALL ")
			} else {
				w.WriteString(" UNION ")
			}
		}
		w.WriteString("(")
		clause.Write(w)
		w.WriteString(")")
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

func NewUnionBuilder() *UnionBuilder {
	var sb = &UnionBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}
