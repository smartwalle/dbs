package dbs

import (
	"strings"
)

const (
	kSQLCalcFoundRows = "SQL_CALC_FOUND_ROWS"
	kFoundRows        = "FOUND_ROWS()"
)

const (
	kOnDuplicateKeyUpdate = "ON DUPLICATE KEY UPDATE "
)

// --------------------------------------------------------------------------------
func (this *SelectBuilder) UseSQLCalcFoundRows() *SelectBuilder {
	return this.Options(kSQLCalcFoundRows)
}

func (this *SelectBuilder) FoundRows(args ...string) *SelectBuilder {
	var ts = []string{kFoundRows}

	if len(args) > 0 {
		ts = append(ts, args...)
	}

	var sb = NewSelectBuilder()
	sb.columns = statements{NewStatement(strings.Join(ts, " "))}
	return sb
}

// --------------------------------------------------------------------------------
type onDuplicateKeyUpdateStmt struct {
	stmts statements
}

func (this *onDuplicateKeyUpdateStmt) WriteToSQL(w Writer) error {
	if len(this.stmts) == 0 {
		return nil
	}

	if _, err := w.WriteString(kOnDuplicateKeyUpdate); err != nil {
		return err
	}

	return this.stmts.WriteToSQL(w, ", ")
}

func (this *onDuplicateKeyUpdateStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.WriteToSQL(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (this *onDuplicateKeyUpdateStmt) Append(sql interface{}, args ...interface{}) *onDuplicateKeyUpdateStmt {
	this.stmts = append(this.stmts, NewStatement(sql, args...))
	return this
}

func (this *onDuplicateKeyUpdateStmt) Appends(stmts ...Statement) *onDuplicateKeyUpdateStmt {
	this.stmts = append(this.stmts, stmts...)
	return this
}

func OnDuplicateKeyUpdate() *onDuplicateKeyUpdateStmt {
	var c = &onDuplicateKeyUpdateStmt{}
	return c
}
