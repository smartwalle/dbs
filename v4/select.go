package dbs

import (
	"io"
	"bytes"
	"errors"
	"strings"
	"fmt"
)

type SelectBuilder struct {
	prefixes statements
	options  statements
	columns  statements
	from     statements
	joins    statements
	wheres   statements
	groupBys []string
	havings  statements
	orderBys []string
	limit    Statement
	offset   Statement
	suffixes statements
}

func (this *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	this.prefixes = append(this.prefixes, NewStatement(sql, args...))
	return this
}

func (this *SelectBuilder) Options(options ...string) *SelectBuilder {
	for _, c := range options {
		this.options = append(this.options, NewStatement(c))
	}
	return this
}

func (this *SelectBuilder) Selects(columns ...string) *SelectBuilder {
	for _, c := range columns {
		this.columns = append(this.columns, NewStatement(c))
	}
	return this
}

func (this *SelectBuilder) Select(column interface{}, args ...interface{}) *SelectBuilder {
	var stmt = parseStmt(column, args...)
	if stmt != nil {
		this.columns = append(this.columns, stmt)
	}
	return this
}

func (this *SelectBuilder) From(table string, args ...string) *SelectBuilder {
	var ts []string
	ts = append(ts, fmt.Sprintf("`%s`", table))
	ts = append(ts, args...)
	this.from = append(this.from, NewStatement(strings.Join(ts, " ")))
	return this
}

func (this *SelectBuilder) Join(join, table, suffix string, args ...interface{}) *SelectBuilder {
	return this.join(join, table, suffix, args...)
}

func (this *SelectBuilder) RightJoin(table, suffix string, args ...interface{}) *SelectBuilder {
	return this.join("RIGHT JOIN", table, suffix, args...)
}

func (this *SelectBuilder) LeftJoin(table, suffix string, args ...interface{}) *SelectBuilder {
	return this.join("LEFT JOIN", table, suffix, args...)
}

func (this *SelectBuilder) join(join, table, suffix string, args ...interface{}) *SelectBuilder {
	var sql = []string{join, fmt.Sprintf("`%s`", table), suffix}
	this.joins = append(this.joins, NewStatement(strings.Join(sql, " "), args...))
	return this
}

func (this *SelectBuilder) Where(sql interface{}, args ...interface{}) *SelectBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.wheres = append(this.wheres, stmt)
	}
	return this
}

func (this *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	this.groupBys = append(this.groupBys, groupBys...)
	return this
}

func (this *SelectBuilder) Having(sql string, args ...interface{}) *SelectBuilder {
	this.havings = append(this.havings, NewStatement(sql, args...))
	return this
}

func (this *SelectBuilder) OrderBy(sql ...string) *SelectBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	this.limit = NewStatement(fmt.Sprintf(" LIMIT %d", limit))
	return this
}

func (this *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	this.offset = NewStatement(fmt.Sprintf(" OFFSET %d", offset))
	return this
}

func (this *SelectBuilder) Suffix(sql string, args ...interface{}) *SelectBuilder {
	this.suffixes = append(this.suffixes, NewStatement(sql, args...))
	return this
}

func (this *SelectBuilder) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, "", args)
	return sqlBuffer.String(), args.values, err
}

func (this *SelectBuilder) AppendToSQL(w io.Writer, sep string, args *Args) error {
	if len(this.columns) == 0 {
		return errors.New("SELECT statements must have at least on result column")
	}

	if len(this.prefixes) > 0 {
		this.prefixes.AppendToSQL(w, " ", args)
		io.WriteString(w, " ")
	}

	io.WriteString(w, "SELECT ")

	if len(this.options) > 0 {
		this.options.AppendToSQL(w, " ", args)
		io.WriteString(w, " ")
	}

	if len(this.columns) > 0 {
		this.columns.AppendToSQL(w, ", ", args)
	}

	if len(this.from) > 0 {
		io.WriteString(w, " FROM ")
		this.from.AppendToSQL(w, ", ", args)
	}

	if len(this.joins) > 0 {
		io.WriteString(w, " ")
		this.joins.AppendToSQL(w, " ", args)
	}

	if len(this.wheres) > 0 {
		io.WriteString(w, " WHERE ")
		this.wheres.AppendToSQL(w, " AND ", args)
	}

	if len(this.groupBys) > 0 {
		io.WriteString(w, " GROUP BY ")
		io.WriteString(w, strings.Join(this.groupBys, ", "))
	}

	if len(this.havings) > 0 {
		io.WriteString(w, " HAVING ")
		this.havings.AppendToSQL(w, " AND ", args)
	}

	if len(this.orderBys) > 0 {
		io.WriteString(w, " ORDER BY ")
		io.WriteString(w, strings.Join(this.orderBys, ", "))
	}

	if this.limit != nil {
		this.limit.AppendToSQL(w, args)
	}

	if this.offset != nil {
		this.offset.AppendToSQL(w, args)
	}

	if len(this.suffixes) > 0 {
		io.WriteString(w, " ")
		this.suffixes.AppendToSQL(w, " ", args)
	}

	return nil
}

func NewSelectBuilder() *SelectBuilder {
	return &SelectBuilder{}
}
