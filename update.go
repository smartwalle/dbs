package dbs

import (
	"bytes"
	"errors"
	"io"
	"strings"
)

const (
	kUpdateBuilder = "UpdateBuilder"
)

type UpdateBuilder struct {
	*builder
	*exec
	prefixes statements
	options  statements
	tables   statements
	joins    statements
	columns  setStmts
	wheres   statements
	orderBys []string
	limit    Statement
	offset   Statement
	suffixes statements
}

func (this *UpdateBuilder) Type() string {
	return kUpdateBuilder
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
	ts = append(ts, this.quote(table))
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
	var sql = []string{join, this.quote(table), suffix}
	this.joins = append(this.joins, NewStatement(strings.Join(sql, " "), args...))
	return this
}

func (this *UpdateBuilder) SET(column string, value interface{}) *UpdateBuilder {
	this.columns = append(this.columns, newSet(this.quote(column), value))
	return this
}

// SETS 批量设置需要更新的字段及其值
// var name = "my name"
// SETS("name", name, "age", 10)
func (this *UpdateBuilder) SETS(kvs ...interface{}) *UpdateBuilder {
	var column string
	for i, value := range kvs {
		if i%2 == 0 {
			column = value.(string)
			continue
		}
		this.SET(column, value)
	}
	return this
}

func (this *UpdateBuilder) SetMap(data map[string]interface{}) *UpdateBuilder {
	for k, v := range data {
		this.SET(k, v)
	}
	return this
}

func (this *UpdateBuilder) Where(sql interface{}, args ...interface{}) *UpdateBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.wheres = append(this.wheres, stmt)
	}
	return this
}

func (this *UpdateBuilder) OrderBy(sql ...string) *UpdateBuilder {
	this.orderBys = append(this.orderBys, sql...)
	return this
}

func (this *UpdateBuilder) Limit(limit int64) *UpdateBuilder {
	this.limit = NewStatement(" LIMIT ?", limit)
	return this
}

func (this *UpdateBuilder) Offset(offset int64) *UpdateBuilder {
	this.offset = NewStatement(" OFFSET ?", offset)
	return this
}

func (this *UpdateBuilder) Suffix(sql interface{}, args ...interface{}) *UpdateBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.suffixes = append(this.suffixes, stmt)
	}
	return this
}

func (this *UpdateBuilder) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	if err := this.AppendToSQL(sqlBuffer, "", args); err != nil {
		return "", nil, err
	}
	sql, err := this.parseVal(sqlBuffer.String())
	if err != nil {
		return "", nil, err
	}
	return sql, args.values, nil
}

func (this *UpdateBuilder) AppendToSQL(w io.Writer, sep string, args *Args) error {
	if len(this.tables) == 0 {
		return errors.New("update statements must specify a table")
	}
	if len(this.columns) == 0 {
		return errors.New("update statements must have at least one Set")
	}

	if len(this.prefixes) > 0 {
		if err := this.prefixes.AppendToSQL(w, " ", args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(w, "UPDATE "); err != nil {
		return err
	}

	if len(this.options) > 0 {
		if err := this.options.AppendToSQL(w, " ", args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
	}

	if len(this.tables) > 0 {
		if err := this.tables.AppendToSQL(w, ", ", args); err != nil {
			return err
		}
	}

	if len(this.joins) > 0 {
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
		if err := this.joins.AppendToSQL(w, " ", args); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(w, " SET "); err != nil {
		return err
	}

	if len(this.columns) > 0 {
		if err := this.columns.AppendToSQL(w, ", ", args); err != nil {
			return err
		}
	}

	if len(this.wheres) > 0 {
		if _, err := io.WriteString(w, " WHERE "); err != nil {
			return err
		}
		if err := this.wheres.AppendToSQL(w, " AND ", args); err != nil {
			return err
		}
	}

	if len(this.orderBys) > 0 {
		if _, err := io.WriteString(w, " ORDER BY "); err != nil {
			return err
		}
		if _, err := io.WriteString(w, strings.Join(this.orderBys, ", ")); err != nil {
			return err
		}
	}

	if this.limit != nil {
		if err := this.limit.AppendToSQL(w, args); err != nil {
			return err
		}
	}

	if this.offset != nil {
		if err := this.offset.AppendToSQL(w, args); err != nil {
			return err
		}
	}

	if len(this.suffixes) > 0 {
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
		if err := this.suffixes.AppendToSQL(w, " ", args); err != nil {
			return err
		}
	}

	return nil
}

// --------------------------------------------------------------------------------
func NewUpdateBuilder() *UpdateBuilder {
	var ub = &UpdateBuilder{}
	ub.builder = newBuilder()
	ub.exec = &exec{b: ub}
	return ub
}

// --------------------------------------------------------------------------------
func Update(table string, args ...string) *UpdateBuilder {
	var ub = NewUpdateBuilder()
	ub.Table(table, args...)
	return ub
}
