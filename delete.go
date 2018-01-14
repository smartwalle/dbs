package dbs

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type DeleteBuilder struct {
	prefixes statements
	options  statements
	alias    []string
	tables   statements
	using    string
	joins    statements
	where    Statement
	orderBys []string
	limit    Statement
	offset   Statement
	suffixes statements
}

func (this *DeleteBuilder) Prefix(sql string, args ...interface{}) *DeleteBuilder {
	this.prefixes = append(this.prefixes, NewStatement(sql, args...))
	return this
}

func (this *DeleteBuilder) Options(options ...string) *DeleteBuilder {
	for _, c := range options {
		this.options = append(this.options, NewStatement(c))
	}
	return this
}

func (this *DeleteBuilder) Alias(alias ...string) *DeleteBuilder {
	this.alias = append(this.alias, alias...)
	return this
}

func (this *DeleteBuilder) Table(table string, args ...string) *DeleteBuilder {
	var ts []string
	ts = append(ts, fmt.Sprintf("`%s`", table))
	ts = append(ts, args...)
	this.tables = append(this.tables, NewStatement(strings.Join(ts, " ")))
	return this
}

func (this *DeleteBuilder) USING(sql string) *DeleteBuilder {
	this.using = sql
	return this
}

//func (this *DeleteBuilder) Join(join, table string) *DeleteBuilder {
//	return this.join(join, table)
//}
//
//func (this *DeleteBuilder) RightJoin(table string) *DeleteBuilder {
//	return this.join("RIGHT JOIN", table)
//}
//
//func (this *DeleteBuilder) LeftJoin(table string) *DeleteBuilder {
//	return this.join("LEFT JOIN", table)
//}
//
//func (this *DeleteBuilder) join(join, table string) *DeleteBuilder {
//	this.joins = append(this.joins, join, fmt.Sprintf("`%s`", table))
//	return this
//}

func (this *DeleteBuilder) Join(join, table, suffix string, args ...interface{}) *DeleteBuilder {
	return this.join(join, table, suffix, args...)
}

func (this *DeleteBuilder) RightJoin(table, suffix string, args ...interface{}) *DeleteBuilder {
	return this.join("RIGHT JOIN", table, suffix, args...)
}

func (this *DeleteBuilder) LeftJoin(table, suffix string, args ...interface{}) *DeleteBuilder {
	return this.join("LEFT JOIN", table, suffix, args...)
}

func (this *DeleteBuilder) join(join, table, suffix string, args ...interface{}) *DeleteBuilder {
	var sql = []string{join, fmt.Sprintf("`%s`", table), suffix}
	this.joins = append(this.joins, NewStatement(strings.Join(sql, " "), args...))
	return this
}

func (this *DeleteBuilder) Where(sql Statement) *DeleteBuilder {
	this.where = sql
	return this
}

func (this *DeleteBuilder) OrderBy(sql ...string) *DeleteBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *DeleteBuilder) Limit(limit uint64) *DeleteBuilder {
	this.limit = NewStatement(fmt.Sprintf(" LIMIT %d", limit))
	return this
}

func (this *DeleteBuilder) Offset(offset uint64) *DeleteBuilder {
	this.offset = NewStatement(fmt.Sprintf(" OFFSET %d", offset))
	return this
}

func (this *DeleteBuilder) Suffix(sql string, args ...interface{}) *DeleteBuilder {
	this.suffixes = append(this.suffixes, NewStatement(sql, args...))
	return this
}

func (this *DeleteBuilder) ToSQL() (string, []interface{}, error) {
	if len(this.tables) == 0 {
		return "", nil, errors.New("delete statements must specify a table")
	}

	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	var err error

	if len(this.prefixes) > 0 {
		this.prefixes.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("DELETE ")

	if len(this.options) > 0 {
		this.options.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	if len(this.alias) > 0 {
		sqlBuffer.WriteString(strings.Join(this.alias, ", "))
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("FROM ")

	if len(this.tables) > 0 {
		this.tables.AppendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.using) > 0 {
		sqlBuffer.WriteString(" USING ")
		sqlBuffer.WriteString(this.using)
	}

	if len(this.joins) > 0 {
		sqlBuffer.WriteString(" ")
		this.joins.AppendToSQL(sqlBuffer, " ", args)
	}

	if this.where == nil || this.where.Valid() == false {
		return "", nil, errors.New("delete statements must have WHERE condition")
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

func (this *DeleteBuilder) Exec(s SQLExecutor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return Exec(s, sql, args...)
}

func NewDeleteBuilder() *DeleteBuilder {
	return &DeleteBuilder{}
}
