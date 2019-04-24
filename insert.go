package dbs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	kInsertBuilder = "InsertBuilder"
)

type InsertBuilder struct {
	*builder
	*exec
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

func (this *InsertBuilder) Clone() *InsertBuilder {
	var ib = NewInsertBuilder()
	ib.builder.d = this.builder.d
	ib.prefixes = this.prefixes
	ib.options = this.options
	ib.columns = this.columns
	ib.table = this.table
	ib.suffixes = this.suffixes
	ib.sb = this.sb
	return ib
}

func (this *InsertBuilder) UseDialect(d dialect) {
	this.d = d
	if this.sb != nil {
		this.sb.UseDialect(this.d)
	}
}

func (this *InsertBuilder) Prefix(sql string, args ...interface{}) *InsertBuilder {
	this.prefixes = append(this.prefixes, NewStatement(sql, args...))
	return this
}

func (this *InsertBuilder) Options(options ...string) *InsertBuilder {
	for _, c := range options {
		this.options = append(this.options, NewStatement(c))
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
		this.sb.UseDialect(this.d)
	}
	return this
}

func (this *InsertBuilder) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	if err := this.AppendToSQL(sqlBuffer, args); err != nil {
		return "", nil, err
	}
	sql, err := this.parseVal(sqlBuffer.String())
	if err != nil {
		return "", nil, err
	}
	return sql, args.values, nil
}

func (this *InsertBuilder) AppendToSQL(w io.Writer, args *Args) error {
	if len(this.table) == 0 {
		return errors.New("insert statements must specify a table")
	}
	if len(this.values) == 0 && this.sb == nil {
		return errors.New("insert statements must have at least one set of values")
	}

	if len(this.prefixes) > 0 {
		if err := this.prefixes.AppendToSQL(w, " ", args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, " "); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(w, "INSERT "); err != nil {
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

	if _, err := fmt.Fprintf(w, "INTO %s ", this.quote(this.table)); err != nil {
		return err
	}

	if len(this.columns) > 0 {
		if _, err := io.WriteString(w, "("); err != nil {
			return err
		}
		var ncs = make([]string, 0, len(this.columns))
		for _, c := range this.columns {
			ncs = append(ncs, this.quote(c))
		}
		if _, err := io.WriteString(w, strings.Join(ncs, ", ")); err != nil {
			return err
		}
		if _, err := io.WriteString(w, ")"); err != nil {
			return err
		}
	}

	if len(this.values) > 0 {
		if _, err := io.WriteString(w, " VALUES "); err != nil {
			return err
		}

		var valuesPlaceholder = make([]string, len(this.values))
		for index, value := range this.values {
			var valuePlaceholder = make([]string, len(value))
			for i, v := range value {
				switch vt := v.(type) {
				case Statement:
					vSQL, vArgs, _ := vt.ToSQL()
					valuePlaceholder[i] = vSQL
					args.Append(vArgs...)
				default:
					valuePlaceholder[i] = "?"
					args.Append(v)
				}
			}
			valuesPlaceholder[index] = fmt.Sprintf("(%s)", strings.Join(valuePlaceholder, ", "))
		}
		if _, err := io.WriteString(w, strings.Join(valuesPlaceholder, ", ")); err != nil {
			return err
		}
	} else if this.sb != nil {
		if _, err := io.WriteString(w, " ("); err != nil {
			return err
		}
		if err := this.sb.AppendToSQL(w, args); err != nil {
			return err
		}
		if _, err := io.WriteString(w, ")"); err != nil {
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
func NewInsertBuilder() *InsertBuilder {
	var ib = &InsertBuilder{}
	ib.builder = newBuilder()
	ib.exec = &exec{b: ib}
	return ib
}

// --------------------------------------------------------------------------------
func Insert(columns ...string) *InsertBuilder {
	var ib = NewInsertBuilder()
	ib.Columns(columns...)
	return ib
}
