package dba

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
)

//--------------------------------------------------------------------------------
type expression struct {
	sql  string
	args []interface{}
}

type expressions []expression

func Expression(sql string, args ...interface{}) expression {
	return expression{sql, args}
}

func (this expressions) appendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	for i, e := range this {
		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		_, err := io.WriteString(w, e.sql)
		if err != nil {
			return nil, err
		}
		args = append(args, e.args...)
	}
	return args, nil
}

type whereExpression struct {
	sql  string
	args []interface{}
}

type whereExpressions []whereExpression


func WhereExpression(sql string, args ...interface{}) whereExpression {
	return whereExpression{sql, args}
}

func (this whereExpressions) appendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	for i, e := range this {
		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}
		if i == 0 {
			e.sql = strings.TrimSpace(e.sql)
			if strings.HasPrefix(e.sql, "AND") {
				e.sql = strings.TrimPrefix(e.sql, "AND")
			}
			if strings.HasPrefix(e.sql, "OR") {
				e.sql = strings.TrimPrefix(e.sql, "OR")
			}
		}
		_, err := io.WriteString(w, e.sql)
		if err != nil {
			return nil, err
		}
		args = append(args, e.args...)
	}
	return args, nil
}


//--------------------------------------------------------------------------------
type SelectBuilder struct {
	prefixes     expressions
	options      expressions
	columns      expressions
	from         expressions
	joins        []string
	joinsArg     []interface{}
	wheres       whereExpressions
	groupBys     []string
	havings      expressions
	orderBys     expressions
	limit        uint64
	updateLimit  bool
	offset       uint64
	updateOffset bool
	suffixes     expressions
}

func (this *SelectBuilder) Prefix(sql string, args ...interface{}) *SelectBuilder {
	this.prefixes = append(this.prefixes, Expression(sql, args...))
	return this
}

func (this *SelectBuilder) Options(options ...string) *SelectBuilder {
	for _, c := range options {
		this.options = append(this.options, Expression(c))
	}
	return this
}

func (this *SelectBuilder) Selects(columns ...string) *SelectBuilder {
	for _, c := range columns {
		this.columns = append(this.columns, Expression(c))
	}
	return this
}

func (this *SelectBuilder) Select(sql string, args ...interface{}) *SelectBuilder {
	this.columns = append(this.columns, Expression(sql, args...))
	return this
}

func (this *SelectBuilder) From(froms ...string) *SelectBuilder {
	this.from = nil
	for _, f := range froms {
		this.from = append(this.from, Expression(f))
	}
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
	this.joins = append(this.joins, join, table, suffix)
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
	this.havings = append(this.havings, Expression(sql, args...))
	return this
}

func (this *SelectBuilder) OrderBy(sql string, args ...interface{}) *SelectBuilder {
	this.orderBys = append(this.orderBys, Expression(sql, args...))
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
	this.suffixes = append(this.suffixes, Expression(sql, args...))
	return this
}

func (this *SelectBuilder) ToSQL() (sql string, args []interface{}, err error) {
	if len(this.columns) == 0 {
		return "", nil, errors.New("SELECT statements must have at least on result column")
	}

	var sqlBuffer = &bytes.Buffer{}
	if len(this.prefixes) > 0 {
		args, _ = this.prefixes.appendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("SELECT ")

	if len(this.options) > 0 {
		args, _ = this.options.appendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	if len(this.columns) > 0 {
		args, _ = this.columns.appendToSQL(sqlBuffer, ", ", args)
	}

	if len(this.from) > 0 {
		sqlBuffer.WriteString(" FROM ")
		args, _ = this.from.appendToSQL(sqlBuffer, " ", args)
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
		args, _ = this.havings.appendToSQL(sqlBuffer, " ", args)
	}

	if len(this.orderBys) > 0 {
		sqlBuffer.WriteString(" ORDER BY ")
		args, _ = this.orderBys.appendToSQL(sqlBuffer, ", ", args)
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
		args, _ = this.suffixes.appendToSQL(sqlBuffer, " ", args)
	}

	sql = sqlBuffer.String()

	return sql, args, err
}

//--------------------------------------------------------------------------------
func Placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat(", ?", count)[2:]
}

func NewSelectBuilder() *SelectBuilder {
	var s = &SelectBuilder{}
	return s
}