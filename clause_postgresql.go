package dbs

import "strings"

type onConflictDoUpdateClause struct {
	columns []string
	clauses Clauses
}

func (clause *onConflictDoUpdateClause) Write(w Writer) error {
	if len(clause.clauses) == 0 || len(clause.columns) == 0 {
		return nil
	}

	if _, err := w.WriteString("ON CONFLICT ("); err != nil {
		return err
	}

	if _, err := w.WriteString(strings.Join(clause.columns, ", ")); err != nil {
		return err
	}

	if _, err := w.WriteString(") DO UPDATE SET "); err != nil {
		return err
	}

	return clause.clauses.Write(w, ", ")
}

func (clause *onConflictDoUpdateClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (clause *onConflictDoUpdateClause) Append(sql interface{}, args ...interface{}) *onConflictDoUpdateClause {
	clause.clauses = append(clause.clauses, NewClause(sql, args...))
	return clause
}

func (clause *onConflictDoUpdateClause) Appends(clauses ...SQLClause) *onConflictDoUpdateClause {
	clause.clauses = append(clause.clauses, clauses...)
	return clause
}

func OnConflictKeyUpdate(columns ...string) *onConflictDoUpdateClause {
	return &onConflictDoUpdateClause{columns: columns}
}
