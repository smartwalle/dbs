package dba

import "fmt"

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

// --------------------------------------------------------------------------------
type setStmt struct {
	column string
	value  interface{}
}

func NewSet(column string, value interface{}) SQLer {
	return &setStmt{column, value}
}

func (this *setStmt) ToSQL() (vSQL string, vArgs []interface{}, vErr error) {
	var p = ""
	switch vt := this.value.(type) {
	case SQLer:
		cSQL, cArgs, vErr := vt.ToSQL()
		if vErr != nil {
			return "", nil, vErr
		}
		p = cSQL
		vArgs = append(vArgs, cArgs...)
	default:
		p = "?"
		vArgs = append(vArgs, vt)
	}
	vSQL = fmt.Sprintf("%s=%s", this.column, p)
	return
}