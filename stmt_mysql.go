package dbs

const (
	kOnDuplicateKeyUpdate = "ON DUPLICATE KEY UPDATE "
)

type onDuplicateKeyUpdateStmt struct {
	stmts statements
}

func (this *onDuplicateKeyUpdateStmt) Write(w Writer) error {
	if len(this.stmts) == 0 {
		return nil
	}

	if _, err := w.WriteString(kOnDuplicateKeyUpdate); err != nil {
		return err
	}

	return this.stmts.Write(w, ", ")
}

func (this *onDuplicateKeyUpdateStmt) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
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
