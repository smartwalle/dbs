package dbs

import "strings"

type onConflictDoUpdateStmt struct {
	columns []string
	stmts   statements
}

func (this *onConflictDoUpdateStmt) WriteToSQL(w Writer) error {
	if len(this.stmts) == 0 || len(this.columns) == 0 {
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

	return this.stmts.WriteToSQL(w, ", ")
}

func (this *onConflictDoUpdateStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.WriteToSQL(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (this *onConflictDoUpdateStmt) Append(sql interface{}, args ...interface{}) *onConflictDoUpdateStmt {
	this.stmts = append(this.stmts, NewStatement(sql, args...))
	return this
}

func (this *onConflictDoUpdateStmt) Appends(stmts ...Statement) *onConflictDoUpdateStmt {
	this.stmts = append(this.stmts, stmts...)
	return this
}

func OnConflictKeyUpdate(columns ...string) *onConflictDoUpdateStmt {
	var c = &onConflictDoUpdateStmt{}
	c.columns = columns
	return c
}
