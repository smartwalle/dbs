package dbs

import (
	"fmt"
	"io"
	"bytes"
)

// --------------------------------------------------------------------------------
type setClause struct {
	column string
	value  interface{}
}

func newSet(column string, value interface{}) Clause {
	return &setClause{column, value}
}

func (this *setClause) ToSQL() (sql string, args []interface{}, err error) {
	var sqlBuffer = &bytes.Buffer{}
	args, err = this.AppendToSQL(sqlBuffer, "", nil)
	return sqlBuffer.String(), args, err
}

func (this *setClause) AppendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	var p = ""
	switch vt := this.value.(type) {
	case Clause:
		cSQL, cArgs, vErr := vt.ToSQL()
		if vErr != nil {
			return nil, vErr
		}
		p = cSQL
		args = append(args, cArgs...)
	default:
		p = "?"
		args = append(args, vt)
	}
	io.WriteString(w, fmt.Sprintf("%s=%s", this.column, p))

	return args, nil
}

func (this *setClause) Append(c ...Clause) {
}

// --------------------------------------------------------------------------------
type setClauses struct {
	clauses []Clause
}

func newSets() Clause {
	return &setClauses{}
}

func (this *setClauses) ToSQL() (sql string, args []interface{}, err error) {
	var sqlBuffer = &bytes.Buffer{}
	args, err = this.AppendToSQL(sqlBuffer, ", ", nil)
	return sqlBuffer.String(), args, err
}

func (this *setClauses) AppendToSQL(w io.Writer, sep string, args []interface{}) ([]interface{}, error) {
	var err error
	for i, c := range this.clauses {
		if i != 0 {
			io.WriteString(w, sep)
		}
		args, err = c.AppendToSQL(w, sep, args)
		if err != nil {
			return nil, err
		}
	}
	return args, nil
}

func (this *setClauses) Append(c ...Clause) {
	this.clauses = append(this.clauses, c...)
}