package dbs

const (
	kOnDuplicateKeyUpdate = "ON DUPLICATE KEY UPDATE "
)

type onDuplicateKeyUpdateClause struct {
	clauses Clauses
}

func (clause *onDuplicateKeyUpdateClause) Write(w Writer) error {
	if len(clause.clauses) == 0 {
		return nil
	}

	if _, err := w.WriteString(kOnDuplicateKeyUpdate); err != nil {
		return err
	}

	return clause.clauses.Write(w, ", ")
}

func (clause *onDuplicateKeyUpdateClause) SQL() (string, []interface{}, error) {
	var sqlBuf = getBuffer()
	defer sqlBuf.Release()

	err := clause.Write(sqlBuf)
	return sqlBuf.String(), sqlBuf.Values(), err
}

func (clause *onDuplicateKeyUpdateClause) Append(sql interface{}, args ...interface{}) *onDuplicateKeyUpdateClause {
	clause.clauses = append(clause.clauses, NewClause(sql, args...))
	return clause
}

func (clause *onDuplicateKeyUpdateClause) Appends(clauses ...SQLClause) *onDuplicateKeyUpdateClause {
	clause.clauses = append(clause.clauses, clauses...)
	return clause
}

func OnDuplicateKeyUpdate() *onDuplicateKeyUpdateClause {
	return &onDuplicateKeyUpdateClause{}
}
