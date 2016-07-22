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