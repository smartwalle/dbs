package dba

// --------------------------------------------------------------------------------
type SQLer interface {
	ToSQL() (sql string, args []interface{}, err error)
}

// --------------------------------------------------------------------------------
type rawSQL struct {
	sql interface{}
	args []interface{}
}

func RawSQL(sql interface{}, args ...interface{}) SQLer {
	return &rawSQL{sql, args}
}

func (this *rawSQL) ToSQL() (sql string, args []interface{}, err error) {
	switch t := this.sql.(type) {
	case SQLer:
		sql, args, err = t.ToSQL()
	case string:
		sql = t
		args = this.args
	case nil:
	default:
	}
	return
}