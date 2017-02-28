package dba

import (
	"bytes"
	"database/sql"
)

type Builder struct {
	sql  *bytes.Buffer
	args []interface{}
}

func NewBuilder() *Builder {
	var b = &Builder{}
	b.sql = &bytes.Buffer{}
	return b
}

func (this *Builder) Append(sql string, args ...interface{}) {
	this.sql.WriteString(sql)
	this.sql.WriteString(" ")
	this.args = append(this.args, args...)
}

func (this *Builder) ToSQL() (string, []interface{}, error) {
	var sqlStr = this.sql.String()
	return sqlStr, this.args, nil
}

func (this *Builder) Exec(s StmtPrepare) (sql.Result, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return Exec(s, sql, args...)
}

func (this *Builder) Query(s StmtPrepare) (*sql.Rows, error) {
	sql, args, err := this.ToSQL()
	if err != nil {
		return nil, err
	}
	return Query(s, sql, args...)
}

func (this *Builder) Scan(s StmtPrepare, result interface{}) (err error) {
	rows, err := this.Query(s)
	if err != nil {
		return err
	}
	err = Scan(rows, result)
	return err
}