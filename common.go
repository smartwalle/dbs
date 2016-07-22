package dba

import (
	"database/sql"
)

type StmtPrepare interface {
	Prepare(query string) (*sql.Stmt, error)
}

type Where struct {
	condition string
	args      []interface{}
}

func NewWhere(c string, args ...interface{}) *Where {
	var w = &Where{}
	w.condition = c
	w.args = args
	return w
}

func Exec(s StmtPrepare, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := s.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(args...)
	return result, err
}

func Query(s StmtPrepare, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := s.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	return rows, err
}