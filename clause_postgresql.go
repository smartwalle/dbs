package dbs

import "strings"

type onConflictDoUpdateClause struct {
	columns []string
	clauses Clauses
}

func (this *onConflictDoUpdateClause) Write(w Writer) error {
	if len(this.clauses) == 0 || len(this.columns) == 0 {
		return nil
	}

	if _, err := w.WriteString("ON CONFLICT ("); err != nil {
		return err
	}

	if _, err := w.WriteString(strings.Join(this.columns, ", ")); err != nil {
		return err
	}

	if _, err := w.WriteString(") DO UPDATE SET "); err != nil {
		return err
	}

	return this.clauses.Write(w, ", ")
}

func (this *onConflictDoUpdateClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (this *onConflictDoUpdateClause) Append(sql interface{}, args ...interface{}) *onConflictDoUpdateClause {
	this.clauses = append(this.clauses, NewClause(sql, args...))
	return this
}

func (this *onConflictDoUpdateClause) Appends(clauses ...SQLClause) *onConflictDoUpdateClause {
	this.clauses = append(this.clauses, clauses...)
	return this
}

func OnConflictKeyUpdate(columns ...string) *onConflictDoUpdateClause {
	var c = &onConflictDoUpdateClause{}
	c.columns = columns
	return c
}
