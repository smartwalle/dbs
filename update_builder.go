package dba

import (
	"strings"
	"strconv"
	"bytes"
)

type UpdateBuilder struct {
	prefixes     expressions
	options      expressions
	tables       []string
	sets         expressions
	wheres       whereExpressions
	orderBys     expressions
	limit        uint64
	updateLimit  bool
}

func (this *UpdateBuilder) Prefix(sql string, args ...interface{}) *UpdateBuilder {
	this.prefixes = append(this.prefixes, Expression(sql, args...))
	return this
}

func (this *UpdateBuilder) Options(options ...string) *UpdateBuilder {
	for _, c := range options {
		this.options = append(this.options, Expression(c))
	}
	return this
}

func (this *UpdateBuilder) Table(from ...string) *UpdateBuilder {
	this.tables = from
	return this
}

func (this *UpdateBuilder) SET(set string, args ...interface{}) *UpdateBuilder {
	this.sets = append(this.sets, Expression(set, args...))
	return this
}

func (this *UpdateBuilder) Where(sql string, args ...interface{}) *UpdateBuilder {
	this.wheres = append(this.wheres, WhereExpression(sql, args...))
	return this
}

func (this *UpdateBuilder) OrderBy(sql string, args ...interface{}) *UpdateBuilder {
	this.orderBys = append(this.orderBys, Expression(sql, args...))
	return this
}

func (this *UpdateBuilder) Limit(limit uint64) *UpdateBuilder {
	this.limit = limit
	this.updateLimit = true
	return this
}

func (this *UpdateBuilder) ToSQL() (sql string, args []interface{}, err error) {
	var sqlBuffer = &bytes.Buffer{}
	if len(this.prefixes) > 0 {
		args, _ = this.prefixes.appendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("UPDATE ")

	if len(this.options) > 0 {
		args, _ = this.options.appendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	if len(this.tables) > 0 {
		sqlBuffer.WriteString(strings.Join(this.tables, ", "))
	}

	sqlBuffer.WriteString(" SET ")

	if len(this.sets) > 0 {
		args, _ = this.sets.appendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.wheres) > 0 {
		sqlBuffer.WriteString(" WHERE ")
		args, _ = this.wheres.appendToSQL(sqlBuffer, " ", args)
	}

	if len(this.orderBys) > 0 {
		sqlBuffer.WriteString(" ORDER BY ")
		args, _ = this.orderBys.appendToSQL(sqlBuffer, ", ", args)
	}

	if this.updateLimit {
		sqlBuffer.WriteString(" LIMIT ")
		sqlBuffer.WriteString(strconv.FormatUint(this.limit, 10))
	}

	sql = sqlBuffer.String()

	return sql, args, err
}

func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{}
}
