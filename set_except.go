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

func (this *ExceptBuilder) Type() string {
	return kExceptBuilder
}

func (this *ExceptBuilder) UsePlaceholder(p Placeholder) *ExceptBuilder {
	this.builder.UsePlaceholder(p)
	return this
}

func (this *ExceptBuilder) Except(clauses ...SQLClause) *ExceptBuilder {
	var first = len(this.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			this.clauses = append(this.clauses, NewClause("", clause))
		} else {
			this.clauses = append(this.clauses, NewClause(" EXCEPT ", clause))
		}
	}
	return this
}

func (this *ExceptBuilder) ExceptAll(clauses ...SQLClause) *ExceptBuilder {
	var first = len(this.clauses) == 0
	for i, clause := range clauses {
		if i == 0 && first {
			this.clauses = append(this.clauses, NewClause("", clause))
		} else {
			this.clauses = append(this.clauses, NewClause(" EXCEPT ALL ", clause))
		}
	}
	return this
}

func (this *ExceptBuilder) OrderBy(sql ...string) *ExceptBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *ExceptBuilder) Limit(limit int64) *ExceptBuilder {
	this.limit = NewClause(" LIMIT ?", limit)
	return this
}

func (this *ExceptBuilder) Offset(offset int64) *ExceptBuilder {
	this.offset = NewClause(" OFFSET ?", offset)
	return this
}

func (this *ExceptBuilder) SQL() (string, []interface{}, error) {
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

func (this *ExceptBuilder) Write(w Writer) (err error) {
	if len(this.clauses) < 2 {
		return errors.New("dbs: EXCEPT clause must have at least two clause")
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

func NewExceptBuilder() *ExceptBuilder {
	var sb = &ExceptBuilder{}
	sb.placeholder = gPlaceholder
	return sb
}
