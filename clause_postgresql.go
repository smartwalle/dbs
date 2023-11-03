package dbs

import "strings"

type onConflictDoUpdateClause struct {
	columns []string
	clauses Clauses
}

func (conflict *onConflictDoUpdateClause) Write(w Writer) error {
	if len(conflict.clauses) == 0 || len(conflict.columns) == 0 {
		return nil
	}

	if _, err := w.WriteString("ON CONFLICT ("); err != nil {
		return err
	}

	if _, err := w.WriteString(strings.Join(conflict.columns, ", ")); err != nil {
		return err
	}

	if _, err := w.WriteString(") DO UPDATE SET "); err != nil {
		return err
	}

	return conflict.clauses.Write(w, ", ")
}

func (conflict *onConflictDoUpdateClause) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	err := conflict.Write(buf)
	return buf.String(), buf.Values(), err
}

func (conflict *onConflictDoUpdateClause) Append(clause interface{}, args ...interface{}) *onConflictDoUpdateClause {
	conflict.clauses = append(conflict.clauses, NewClause(clause, args...))
	return conflict
}

func (conflict *onConflictDoUpdateClause) Appends(clauses ...SQLClause) *onConflictDoUpdateClause {
	conflict.clauses = append(conflict.clauses, clauses...)
	return conflict
}

func OnConflictKeyUpdate(columns ...string) *onConflictDoUpdateClause {
	return &onConflictDoUpdateClause{columns: columns}
}
