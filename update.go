package dbs

import (
	"strings"
	"strconv"
	"bytes"
	"github.com/smartwalle/errors"
	"fmt"
	"database/sql"
)

type UpdateBuilder struct {
	prefixes     rawClauses
	options      rawClauses
	tables       rawClauses
	joins        []string
	joinsArg     []interface{}
	columns      Clause
	wheres       whereExpressions
	wheres2      Clause
	orderBys     []string
	limit        uint64
	updateLimit  bool
	offset       uint64
	updateOffset bool
	suffixes     rawClauses
}

func (this *UpdateBuilder) Prefix(sql string, args ...interface{}) *UpdateBuilder {
	this.prefixes = append(this.prefixes, SQL(sql, args...))
	return this
}

func (this *UpdateBuilder) Options(options ...string) *UpdateBuilder {
	for _, c := range options {
		this.options = append(this.options, SQL(c))
	}
	return this
}

func (this *UpdateBuilder) Table(table string, args ...string) *UpdateBuilder {
	var ts []string
	ts = append(ts, fmt.Sprintf("`%s`", table))
	ts = append(ts, args...)
	this.tables = append(this.tables, SQL(strings.Join(ts, " ")))
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
	this.joins = append(this.joins, join, fmt.Sprintf("`%s`", table), suffix)
	this.joinsArg = append(this.joinsArg, args...)
	return this
}

func (this *UpdateBuilder) SET(column string, value interface{}) *UpdateBuilder {
	if this.columns == nil {
		this.columns = newSets()
	}
	this.columns.Append(newSet(column, value))
	return this
}

func (this *UpdateBuilder) SetMap(data map[string]interface{}) *UpdateBuilder {
	for k, v := range data {
		this.SET(k, v)
	}
	return this
}

func (this *UpdateBuilder) Where2(sql string, args ...interface{}) *UpdateBuilder {
	this.wheres = append(this.wheres, WhereExpression(sql, args...))
	return this
}

func (this *UpdateBuilder) Where(c Clause) *UpdateBuilder {
	this.wheres2 = c
	return this
}

func (this *UpdateBuilder) OrderBy(sql ...string) *UpdateBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *UpdateBuilder) Limit(limit uint64) *UpdateBuilder {
	this.limit = limit
	this.updateLimit = true
	return this
}

func (this *UpdateBuilder) Offset(offset uint64) *UpdateBuilder {
	this.offset = offset
	this.updateOffset = true
	return this
}

func (this *UpdateBuilder) Suffix(sql string, args ...interface{}) *UpdateBuilder {
	this.suffixes = append(this.suffixes, SQL(sql, args...))
	return this
}

func (this *UpdateBuilder) ToSQL() (sql string, args []interface{}, err error) {
	if len(this.tables) == 0 {
		return "", nil, errors.New("update statements must specify a table")
	}
	if this.columns == nil {
		return "", nil, errors.New("update statements must have at least one Set")
	}

	var sqlBuffer = &bytes.Buffer{}
	if len(this.prefixes) > 0 {
		args, _ = this.prefixes.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("UPDATE ")

	if len(this.options) > 0 {
		args, _ = this.options.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	if len(this.tables) > 0 {
		args, _ = this.tables.AppendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.joins) > 0 {
		sqlBuffer.WriteString(" ")
		sqlBuffer.WriteString(strings.Join(this.joins, " "))
		args = append(args, this.joinsArg...)
	}

	sqlBuffer.WriteString(" SET ")

	args, err = this.columns.AppendToSQL(sqlBuffer, ", ", args)
	if err != nil {
		return "", nil, err
	}

	if len(this.wheres) == 0 && this.wheres2 == nil {
		return "", nil, errors.New("update statements must have WHERE condition")
	}

	if this.wheres2 != nil {
		sqlBuffer.WriteString(" WHERE ")
		args, _ = this.wheres2.AppendToSQL(sqlBuffer, " ", args)
	} else if len(this.wheres) > 0 {
		sqlBuffer.WriteString(" WHERE ")
		args, _ = this.wheres.appendToSQL(sqlBuffer, " ", args)
	}

	if len(this.orderBys) > 0 {
		sqlBuffer.WriteString(" ORDER BY ")
		sqlBuffer.WriteString(strings.Join(this.orderBys, ", "))
	}

	if this.updateLimit {
		sqlBuffer.WriteString(" LIMIT ")
		sqlBuffer.WriteString(strconv.FormatUint(this.limit, 10))
	}

	if this.updateOffset {
		sqlBuffer.WriteString(" OFFSET ")
		sqlBuffer.WriteString(strconv.FormatUint(this.offset, 10))
	}

	if len(this.suffixes) > 0 {
		sqlBuffer.WriteString(" ")
		args, _ = this.suffixes.AppendToSQL(sqlBuffer, " ", args)
	}

	sql = sqlBuffer.String()

	return sql, args, err
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
