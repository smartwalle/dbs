package dbs

const (
	kOnDuplicateKeyUpdate = "ON DUPLICATE KEY UPDATE "
)

type onDuplicateKeyUpdateClause struct {
	clauses Clauses
}

func (duplicate *onDuplicateKeyUpdateClause) Write(w Writer) error {
	if len(duplicate.clauses) == 0 {
		return nil
	}

	if _, err := w.WriteString(kOnDuplicateKeyUpdate); err != nil {
		return err
	}

	return duplicate.clauses.Write(w, ", ")
}

func (duplicate *onDuplicateKeyUpdateClause) SQL() (string, []interface{}, error) {
	var buf = getBuffer()
	defer buf.Release()

	err := duplicate.Write(buf)
	return buf.String(), buf.Values(), err
}

func (duplicate *onDuplicateKeyUpdateClause) Append(clause interface{}, args ...interface{}) *onDuplicateKeyUpdateClause {
	duplicate.clauses = append(duplicate.clauses, NewClause(clause, args...))
	return duplicate
}

func (duplicate *onDuplicateKeyUpdateClause) Appends(clauses ...SQLClause) *onDuplicateKeyUpdateClause {
	duplicate.clauses = append(duplicate.clauses, clauses...)
	return duplicate
}

func OnDuplicateKeyUpdate() *onDuplicateKeyUpdateClause {
	return &onDuplicateKeyUpdateClause{}
}
