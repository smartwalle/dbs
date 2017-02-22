package dba

import (
	"errors"
	"bytes"
	"strings"
	"fmt"
)

type InsertBuilder struct {
	prefixes expressions
	options  expressions
	columns  []string
	table    string
	values   [][]interface{}
	suffixes expressions
}

func (this *InsertBuilder) Prefix(sql string, args ...interface{}) *InsertBuilder {
	this.prefixes = append(this.prefixes, Expression(sql, args...))
	return this
}

func (this *InsertBuilder) Options(options ...string) *InsertBuilder {
	for _, c := range options {
		this.options = append(this.options, Expression(c))
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

func (this *InsertBuilder) Insert(table string) *InsertBuilder {
	this.table = table
	return this
}

func (this *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	this.values = append(this.values, values)
	return this
}

func (this *InsertBuilder) Suffix(sql string, args ...interface{}) *InsertBuilder {
	this.suffixes = append(this.suffixes, Expression(sql, args...))
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
		args, _ = this.prefixes.appendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("INSERT ")

	if len(this.options) > 0 {
		args, _ = this.options.appendToSQL(sqlBuffer, " ", args)
		sqlBuffer.WriteString(" ")
	}

	sqlBuffer.WriteString("INTO ")
	sqlBuffer.WriteString(this.table)
	sqlBuffer.WriteString(" ")

	if len(this.columns) > 0 {
		sqlBuffer.WriteString("(")
		sqlBuffer.WriteString(strings.Join(this.columns, ", "))
		sqlBuffer.WriteString(")")
	}

	sqlBuffer.WriteString(" VALUES ")

	var valuesPlaceholder = make([]string, len(this.values))
	for index, value := range this.values {
		var valuePlaceholder = make([]string, len(value))
		for i, v := range value {
			valuePlaceholder[i] = "?"
			args = append(args, v)
		}
		valuesPlaceholder[index] = fmt.Sprintf("(%s)", strings.Join(valuePlaceholder, ","))
	}
	sqlBuffer.WriteString(strings.Join(valuesPlaceholder, ", "))

	if len(this.suffixes) > 0 {
		sqlBuffer.WriteString(" ")
		args, _ = this.suffixes.appendToSQL(sqlBuffer, " ", args)
	}

	sql = sqlBuffer.String()

	return sql, args, err
}

func NewInsertBuilder() *InsertBuilder {
	return &InsertBuilder{}
}