package dbs

import (
	"database/sql"
)

type SQLExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func Exec(s SQLExecutor, query string, args ...interface{}) (sql.Result, error) {
	return s.Exec(query, args...)
}

func Query(s SQLExecutor, query string, args ...interface{}) (*sql.Rows, error) {
	return s.Query(query, args...)
}