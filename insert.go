package dbs

import (
	"errors"
	"bytes"
	"strings"
	"fmt"
	"database/sql"
)

type InsertBuilder struct {
	prefixes rawClauses
	options  rawClauses
	columns  []string
	table    string
	values   [][]interface{}
	suffixes rawClauses
}

func (this *InsertBuilder) Prefix(sql string, args ...interface{}) *InsertBuilder {
	this.prefixes = append(this.prefixes, SQL(sql, args...))
	return this
}

func (this *InsertBuilder) Options(options ...string) *InsertBuilder {
	for _, c := range options {
		this.options = append(this.options, SQL(c))
	}
	return this
}

func (this *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	this.columns = append(this.columns, columns...)
	return this
}

func (this *InsertBuilder) Column(column string) *InsertBuilder {
	this.columns = append(this.columns, column)
	return this
}

func (this *InsertBuilder) Table(table string) *InsertBuilder {
	this.table = table
	return this
}

func (this *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	this.values = append(this.values, values)
	return this
}

func (this *InsertBuilder) Suffix(sql string, args ...interface{}) *InsertBuilder {
	this.suffixes = append(this.suffixes, SQL(sql, args...))
	return this
}

func (this *InsertBuilder) SET(column string, value interface{}) *InsertBuilder {
	this.columns = append(this.columns, column)
	if len(this.values) == 0 {
		this.values = append(this.values, make([]interface{}, 0, 0))
	}
	var vList = this.values[0]
	vList = append(vList, value)
	this.values[0] = vList
	return this
}

func (this *InsertBuilder) ToSQL() (sql string, args []interface{}, err error) {
	if len(this.table) == 0 {
		err = errors.New("insert statements must specify a table")
		return "", nil, err
	}
	if len(this.values) == 0 {
		err = errors.New("insert statements must have at least one set of values")
		return "", nil, err
	}

	var sqlBuffer = &bytes.Buffer{}

	if len(this.prefixes) > 0 {
		args, _ = this.prefixes.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("INSERT ")

	if len(this.options) > 0 {
		args, _ = this.options.AppendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("INTO `")
	sqlBuffer.WriteString(this.table)
	sqlBuffer.WriteString("` ")

	if len(this.columns) > 0 {
		sqlBuffer.WriteString("(`")
		sqlBuffer.WriteString(strings.Join(this.columns, "`, `"))
		sqlBuffer.WriteString("`)")
	}

	sqlBuffer.WriteString(" VALUES ")

	var valuesPlaceholder = make([]string, len(this.values))
	for index, value := range this.values {
		var valuePlaceholder = make([]string, len(value))
		for i, v := range value {
			switch vt := v.(type) {
			case Clause:
				vSQL, vArgs, err := vt.ToSQL()
				if err != nil {
					return "", nil, err
				}
				valuePlaceholder[i] = vSQL
				args = append(args, vArgs...)
			default:
				valuePlaceholder[i] = "?"
				args = append(args, v)
			}
		}
		valuesPlaceholder[index] = fmt.Sprintf("(%s)", strings.Join(valuePlaceholder, ", "))
	}
	sqlBuffer.WriteString(strings.Join(valuesPlaceholder, ", "))

	if len(this.suffixes) > 0 {
		sqlBuffer.WriteString(" ")
		args, _ = this.suffixes.AppendToSQL(sqlBuffer, " ", args)
	}

	sql = sqlBuffer.String()

	return sql, args, err
}

func (this *InsertBuilder) Exec(s SQLExecutor) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return Exec(s, sql, args...)
}

func Insert(s SQLExecutor, table string, data map[string]interface{}) (sql.Result, error) {
	var in = NewInsertBuilder()
	in.Table(table)

	var values []interface{}
	for k, v := range data {
		in.Column(k)
		values = append(values, v)
	}
	in.Values(values...)
	return in.Exec(s)
}

func NewInsertBuilder() *InsertBuilder {
	return &InsertBuilder{}
}