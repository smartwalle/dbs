package dbs

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/smartwalle/errors"
	"strings"
)

type UpdateBuilder struct {
	prefixes statements
	options  statements
	tables   statements
	joins    statements
	columns  sets
	where    Statement
	orderBys []string
	limit    Statement
	offset   Statement
	suffixes statements
}

func (this *UpdateBuilder) Prefix(sql string, args ...interface{}) *UpdateBuilder {
	this.prefixes = append(this.prefixes, NewStatement(sql, args...))
	return this
}

func (this *UpdateBuilder) Options(options ...string) *UpdateBuilder {
	for _, c := range options {
		this.options = append(this.options, NewStatement(c))
	}
	return this
}

func (this *UpdateBuilder) Table(table string, args ...string) *UpdateBuilder {
	var ts []string
	ts = append(ts, fmt.Sprintf("`%s`", table))
	ts = append(ts, args...)
	this.tables = append(this.tables, NewStatement(strings.Join(ts, " ")))
	return this
}

func (this *UpdateBuilder) Join(join, table, suffix string, args ...interface{}) *UpdateBuilder {
	return this.join(join, table, suffix, args...)
}

func (this *UpdateBuilder) RightJoin(table, suffix string, args ...interface{}) *UpdateBuilder {
	return this.join("RIGHT JOIN", table, suffix, args...)
}

func (this *UpdateBuilder) LeftJoin(table, suffix string, args ...interface{}) *UpdateBuilder {
	return this.join("LEFT JOIN", table, suffix, args...)
}

func (this *UpdateBuilder) join(join, table, suffix string, args ...interface{}) *UpdateBuilder {
	var sql = []string{join, fmt.Sprintf("`%s`", table), suffix}
	this.joins = append(this.joins, NewStatement(strings.Join(sql, " "), args...))
	return this
}

func (this *UpdateBuilder) SET(column string, value interface{}) *UpdateBuilder {
	this.columns = append(this.columns, newSet(column, value))
	return this
}

func (this *UpdateBuilder) SetMap(data map[string]interface{}) *UpdateBuilder {
	for k, v := range data {
		this.SET(k, v)
	}
	return this
}

func (this *UpdateBuilder) Where(sql Statement) *UpdateBuilder {
	this.where = sql
	return this
}

func (this *UpdateBuilder) OrderBy(sql ...string) *UpdateBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *UpdateBuilder) Limit(limit uint64) *UpdateBuilder {
	this.limit = NewStatement(fmt.Sprintf(" LIMIT %d", limit))
	return this
}

func (this *UpdateBuilder) Offset(offset uint64) *UpdateBuilder {
	this.offset = NewStatement(fmt.Sprintf(" OFFSET %d", offset))
	return this
}

func (this *UpdateBuilder) Suffix(sql string, args ...interface{}) *UpdateBuilder {
	this.suffixes = append(this.suffixes, NewStatement(sql, args...))
	return this
}

func (this *UpdateBuilder) ToSQL() (string, []interface{}, error) {
	if len(this.tables) == 0 {
		return "", nil, errors.New("update statements must specify a table")
	}
	if len(this.columns) == 0 {
		return "", nil, errors.New("update statements must have at least one Set")
	}

	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	var err error

	if len(this.prefixes) > 0 {
		this.prefixes.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("UPDATE ")

	if len(this.options) > 0 {
		this.options.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	if len(this.tables) > 0 {
		this.tables.AppendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.joins) > 0 {
		sqlBuffer.WriteString(" ")
		this.joins.AppendToSQL(sqlBuffer, " ", args)
	}

	sqlBuffer.WriteString(" SET ")

	if len(this.columns) > 0 {
		this.columns.AppendToSQL(sqlBuffer, ", ", args)
	}

	if this.where == nil || this.where.Valid() == false {
		return "", nil, errors.New("update statements must have WHERE condition")
	} else {
		sqlBuffer.WriteString(" WHERE ")
		this.where.AppendToSQL(sqlBuffer, " ", args)
	}

	if len(this.orderBys) > 0 {
		sqlBuffer.WriteString(" ORDER BY ")
		sqlBuffer.WriteString(strings.Join(this.orderBys, ", "))
	}

	if this.limit != nil {
		this.limit.AppendToSQL(sqlBuffer, "", args)
	}

	if this.offset != nil {
		this.offset.AppendToSQL(sqlBuffer, "", args)
	}

	if len(this.suffixes) > 0 {
		sqlBuffer.WriteString(" ")
		this.suffixes.AppendToSQL(sqlBuffer, " ", args)
	}

	return sqlBuffer.String(), args.values, err
}

func (this *UpdateBuilder) Exec(s SQLExecutor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return Exec(s, sql, args...)
}

func NewUpdateBuilder() *UpdateBuilder {
	return &UpdateBuilder{}
}
