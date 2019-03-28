package dbs

import (
	"bytes"
	"io"
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

func (this *onDuplicateKeyUpdateStmt) AppendToSQL(w io.Writer, args *Args) error {
	if len(this.stmts) == 0 {
		return nil
	}

	if _, err := io.WriteString(w, kOnDuplicateKeyUpdate); err != nil {
		return err
	}

	return this.stmts.AppendToSQL(w, ", ", args)
}

func (this *onDuplicateKeyUpdateStmt) ToSQL() (string, []interface{}, error) {
	var sqlBuffer = &bytes.Buffer{}
	var args = newArgs()
	err := this.AppendToSQL(sqlBuffer, args)
	return sqlBuffer.String(), args.values, err
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
