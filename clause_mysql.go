package dbs

const (
	kOnDuplicateKeyUpdate = "ON DUPLICATE KEY UPDATE "
)

type onDuplicateKeyUpdateClause struct {
	clauses Clauses
}

func (this *onDuplicateKeyUpdateClause) Write(w Writer) error {
	if len(this.clauses) == 0 {
		return nil
	}

	if _, err := w.WriteString(kOnDuplicateKeyUpdate); err != nil {
		return err
	}

	return this.clauses.Write(w, ", ")
}

func (this *onDuplicateKeyUpdateClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := this.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (this *onDuplicateKeyUpdateClause) Append(sql interface{}, args ...interface{}) *onDuplicateKeyUpdateClause {
	this.clauses = append(this.clauses, NewClause(sql, args...))
	return this
}

func (this *onDuplicateKeyUpdateClause) Appends(clauses ...SQLClause) *onDuplicateKeyUpdateClause {
	this.clauses = append(this.clauses, clauses...)
	return this
}

func OnDuplicateKeyUpdate() *onDuplicateKeyUpdateClause {
	var c = &onDuplicateKeyUpdateClause{}
	return c
}
