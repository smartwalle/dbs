package dbs

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"database/sql"
	"fmt"
)

type SelectBuilder struct {
	prefixes     rawSQLs
	options      rawSQLs
	columns      rawSQLs
	from         rawSQLs
	joins        []string
	joinsArg     []interface{}
	wheres       whereExpressions
	groupBys     []string
	havings      rawSQLs
	orderBys     []string
	limit        uint64
	updateLimit  bool
	offset       uint64
	updateOffset bool
	suffixes     rawSQLs
}

func (this *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	this.prefixes = append(this.prefixes, SQL(sql, args...))
	return this
}

func (this *SelectBuilder) Options(options ...string) *SelectBuilder {
	for _, c := range options {
		this.options = append(this.options, SQL(c))
	}
	return this
}

func (this *SelectBuilder) Selects(columns ...string) *SelectBuilder {
	for _, c := range columns {
		this.columns = append(this.columns, SQL(c))
	}
	return this
}

func (this *SelectBuilder) Select(column string, args ...interface{}) *SelectBuilder {
	this.columns = append(this.columns, SQL(column, args...))
	return this
}

func (this *SelectBuilder) From(table string, args ...string) *SelectBuilder {
	var ts []string
	ts = append(ts, fmt.Sprintf("`%s`", table))
	ts = append(ts, args...)
	this.from = append(this.from, SQL(strings.Join(ts, " ")))
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
	this.joins = append(this.joins, join, fmt.Sprintf("`%s`", table), suffix)
	this.joinsArg = append(this.joinsArg, args...)
	return this
}

func (this *SelectBuilder) Where(sql string, args ...interface{}) *SelectBuilder {
	this.wheres = append(this.wheres, WhereExpression(sql, args...))
	return this
}

func (this *SelectBuilder) GroupBy(groupBys ...string) *SelectBuilder {
	this.groupBys = append(this.groupBys, groupBys...)
	return this
}

func (this *SelectBuilder) Having(sql string, args ...interface{}) *SelectBuilder {
	this.havings = append(this.havings, SQL(sql, args...))
	return this
}

func (this *SelectBuilder) OrderBy(sql ...string, ) *SelectBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	this.limit = limit
	this.updateLimit = true
	return this
}

func (this *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	this.offset = offset
	this.updateOffset = true
	return this
}

func (this *SelectBuilder) Suffix(sql string, args ...interface{}) *SelectBuilder {
	this.suffixes = append(this.suffixes, SQL(sql, args...))
	return this
}

func (this *SelectBuilder) ToSQL() (sql string, args []interface{}, err error) {
	if len(this.columns) == 0 {
		return "", nil, errors.New("SELECT statements must have at least on result column")
	}

	var sqlBuffer = &bytes.Buffer{}
	if len(this.prefixes) > 0 {
		args, _ = this.prefixes.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("SELECT ")

	if len(this.options) > 0 {
		args, _ = this.options.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	if len(this.columns) > 0 {
		args, _ = this.columns.AppendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.from) > 0 {
		sqlBuffer.WriteString(" FROM ")
		args, _ = this.from.AppendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.joins) > 0 {
		sqlBuffer.WriteString(" ")
		sqlBuffer.WriteString(strings.Join(this.joins, " "))
		args = append(args, this.joinsArg...)
	}

	if len(this.wheres) > 0 {
		sqlBuffer.WriteString(" WHERE ")
		args, _ = this.wheres.appendToSQL(sqlBuffer, " ", args)
	}

	if len(this.groupBys) > 0 {
		sqlBuffer.WriteString(" GROUP BY ")
		sqlBuffer.WriteString(strings.Join(this.groupBys, ", "))
	}

	if len(this.havings) > 0 {
		sqlBuffer.WriteString(" HAVING ")
		args, _ = this.havings.AppendToSQL(sqlBuffer, " ", args)
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

func (this *SelectBuilder) CountSQL() (sql string, args []interface{}, err error) {
	if len(this.columns) == 0 {
		return "", nil, errors.New("SELECT statements must have at least on result column")
	}

	var sqlBuffer = &bytes.Buffer{}
	if len(this.prefixes) > 0 {
		args, _ = this.prefixes.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("SELECT ")

	if len(this.options) > 0 {
		args, _ = this.options.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	//if len(this.columns) > 0 {
	//	args, _ = this.columns.AppendToSQL(sqlBuffer, ", ", args)
	//}
	sqlBuffer.WriteString("COUNT(*) AS total")

	if len(this.from) > 0 {
		sqlBuffer.WriteString(" FROM ")
		args, _ = this.from.AppendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.joins) > 0 {
		sqlBuffer.WriteString(" ")
		sqlBuffer.WriteString(strings.Join(this.joins, " "))
		args = append(args, this.joinsArg...)
	}

	if len(this.wheres) > 0 {
		sqlBuffer.WriteString(" WHERE ")
		args, _ = this.wheres.appendToSQL(sqlBuffer, " ", args)
	}

	if len(this.groupBys) > 0 {
		sqlBuffer.WriteString(" GROUP BY ")
		sqlBuffer.WriteString(strings.Join(this.groupBys, ", "))
	}

	if len(this.havings) > 0 {
		sqlBuffer.WriteString(" HAVING ")
		args, _ = this.havings.AppendToSQL(sqlBuffer, " ", args)
	}

	if len(this.orderBys) > 0 {
		sqlBuffer.WriteString(" ORDER BY ")
		sqlBuffer.WriteString(strings.Join(this.orderBys, ", "))
	}

	//if this.updateLimit {
	//	sqlBuffer.WriteString(" LIMIT ")
	//	sqlBuffer.WriteString(strconv.FormatUint(this.limit, 10))
	//}
	//
	//if this.updateOffset {
	//	sqlBuffer.WriteString(" OFFSET ")
	//	sqlBuffer.WriteString(strconv.FormatUint(this.offset, 10))
	//}

	if len(this.suffixes) > 0 {
		sqlBuffer.WriteString(" ")
		args, _ = this.suffixes.AppendToSQL(sqlBuffer, " ", args)
	}

	sql = sqlBuffer.String()

	return sql, args, err
}

func (this *SelectBuilder) Query(s SQLExecutor) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return Query(s, sql, args...)
}

func (this *SelectBuilder) Count(s SQLExecutor) (count int64) {
	sql, args, err := this.CountSQL()
	if err != nil {
		return 0
	}
	rows, err := Query(s, sql, args...)
	if err != nil {
		return 0
	}

	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0
		}
	}
	return count
}

func (this *SelectBuilder) Scan(s SQLExecutor, result interface{}) (err error) {
	rows, err := this.Query(s)
	if err != nil {
		return err
	}
	err = Scan(rows, result)
	return err
}

func NewSelectBuilder() *SelectBuilder {
	return &SelectBuilder{}
}