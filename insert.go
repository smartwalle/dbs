package dbs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	kInsertBuilder = "InsertBuilder"
)

type InsertBuilder struct {
	builder
	prefixes statements
	options  statements
	columns  []string
	table    string
	values   [][]interface{}
	suffixes statements
	sb       *SelectBuilder
}

func (this *InsertBuilder) Type() string {
	return kInsertBuilder
}

func (this *InsertBuilder) UseFormatter(f Formatter) *InsertBuilder {
	this.builder.UseFormatter(f)
	if this.sb != nil {
		this.sb.UseFormatter(this.f)
	}
	return this
}

func (this *InsertBuilder) Clone() *InsertBuilder {
	var ib = *this
	ib.values = nil
	return &ib
}

func (this *InsertBuilder) Prefix(sql string, args ...interface{}) *InsertBuilder {
	this.prefixes = append(this.prefixes, NewStatement(sql, args...))
	return this
}

func (this *InsertBuilder) Options(options ...string) *InsertBuilder {
	for _, opt := range options {
		this.options = append(this.options, NewStatement(opt))
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

func (this *InsertBuilder) Suffix(sql interface{}, args ...interface{}) *InsertBuilder {
	var stmt = parseStmt(sql, args...)
	if stmt != nil {
		this.suffixes = append(this.suffixes, stmt)
	}
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

func (this *InsertBuilder) Select(sb *SelectBuilder) *InsertBuilder {
	this.sb = sb
	if this.sb != nil {
		this.sb.UseFormatter(this.f)
	}
	return this
}

func (this *InsertBuilder) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	if err := this.Write(sqlBuf); err != nil {
		return "", nil, err
	}

	sql, err := this.format(sqlBuf.String())
	if err != nil {
		return "", nil, err
	}
	return sql, sqlBuf.Values(), nil
}

func (this *InsertBuilder) Write(w Writer) (err error) {
	if len(this.table) == 0 {
		return errors.New("dbs: INSERT statement must specify a table")
	}
	if len(this.values) == 0 && this.sb == nil {
		return errors.New("dbs: INSERT statement must have at least one set of values")
	}

	if len(this.prefixes) > 0 {
		if err = this.prefixes.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = w.WriteString("INSERT "); err != nil {
		return err
	}

	if len(this.options) > 0 {
		if err = this.options.Write(w, " "); err != nil {
			return err
		}
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
	}

	if _, err = fmt.Fprintf(w, "INTO %s ", this.quote(this.table)); err != nil {
		return err
	}

	if len(this.columns) > 0 {
		if _, err = w.WriteString("("); err != nil {
			return err
		}
		var ncs = make([]string, 0, len(this.columns))
		for _, c := range this.columns {
			ncs = append(ncs, this.quote(c))
		}
		if _, err = w.WriteString(strings.Join(ncs, ", ")); err != nil {
			return err
		}
		if _, err = w.WriteString(")"); err != nil {
			return err
		}
	}

	if len(this.values) > 0 {
		if _, err = w.WriteString(" VALUES "); err != nil {
			return err
		}

		var valuesPlaceholder = make([]string, len(this.values))
		for index, value := range this.values {
			var valuePlaceholder = make([]string, len(value))
			for i, v := range value {
				switch vt := v.(type) {
				case Statement:
					vSQL, vArgs, _ := vt.SQL()
					valuePlaceholder[i] = vSQL
					w.WriteArgs(vArgs...)
				default:
					valuePlaceholder[i] = "?"
					w.WriteArgs(v)
				}
			}
			valuesPlaceholder[index] = fmt.Sprintf("(%s)", strings.Join(valuePlaceholder, ", "))
		}
		if _, err = w.WriteString(strings.Join(valuesPlaceholder, ", ")); err != nil {
			return err
		}
	} else if this.sb != nil {
		if _, err = w.WriteString(" ("); err != nil {
			return err
		}
		if err = this.sb.Write(w); err != nil {
			return err
		}
		if _, err = w.WriteString(")"); err != nil {
			return err
		}
	}

	if len(this.suffixes) > 0 {
		if _, err = w.WriteString(" "); err != nil {
			return err
		}
		if err = this.suffixes.Write(w, " "); err != nil {
			return err
		}
	}
	return nil
}

func (this *InsertBuilder) reset() {
	this.values = this.values[:0]
}

func (this *InsertBuilder) Exec(s Session) (sql.Result, error) {
	return execContext(context.Background(), s, this)
}

func (this *InsertBuilder) ExecContext(ctx context.Context, s Session) (result sql.Result, err error) {
	return execContext(ctx, s, this)
}

func NewInsertBuilder() *InsertBuilder {
	var ib = &InsertBuilder{}
	ib.f = gFormatter
	return ib
}

func Insert(columns ...string) *InsertBuilder {
	var ib = NewInsertBuilder()
	ib.Columns(columns...)
	return ib
}
