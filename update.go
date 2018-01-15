package dbs

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
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
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, "", args)
	return sqlBuffer.String(), args.values, err
}

func (this *UpdateBuilder) AppendToSQL(w io.Writer, sep string, args *Args) error {
	if len(this.tables) == 0 {
		return errors.New("update statements must specify a table")
	}
	if len(this.columns) == 0 {
		return errors.New("update statements must have at least one Set")
	}

	if len(this.prefixes) > 0 {
		this.prefixes.AppendToSQL(w, " ", args)
		io.WriteString(w, " ")
	}

	io.WriteString(w, "UPDATE ")

	if len(this.options) > 0 {
		this.options.AppendToSQL(w, " ", args)
		io.WriteString(w, " ")
	}

	if len(this.tables) > 0 {
		this.tables.AppendToSQL(w, ", ", args)
	}

	if len(this.joins) > 0 {
		io.WriteString(w, " ")
		this.joins.AppendToSQL(w, " ", args)
	}

	io.WriteString(w, " SET ")

	if len(this.columns) > 0 {
		this.columns.AppendToSQL(w, ", ", args)
	}

	if this.where == nil || this.where.Valid() == false {
		return errors.New("update statements must have WHERE condition")
	} else {
		io.WriteString(w, " WHERE ")
		this.where.AppendToSQL(w, " ", args)
	}

	if len(this.orderBys) > 0 {
		io.WriteString(w, " ORDER BY ")
		io.WriteString(w, strings.Join(this.orderBys, ", "))
	}

	if this.limit != nil {
		this.limit.AppendToSQL(w, "", args)
	}

	if this.offset != nil {
		this.offset.AppendToSQL(w, "", args)
	}

	if len(this.suffixes) > 0 {
		io.WriteString(w, " ")
		this.suffixes.AppendToSQL(w, " ", args)
	}

	return nil
}

func (this *UpdateBuilder) Valid() bool {
	return true
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
