package dba

import (
	"database/sql"
)

type SQLExecutor interface {
	//Prepare(query string) (*sql.Stmt, error)

	Exec(query string, args ...interface{}) (sql.Result, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func Exec(s SQLExecutor, query string, args ...interface{}) (sql.Result, error) {
	//stmt, err := s.Prepare(query)
	//if err != nil {
	//	return nil, err
	//}
	//defer stmt.Close()

	return s.Exec(query, args...)
}

func Query(s SQLExecutor, query string, args ...interface{}) (*sql.Rows, error) {
	//stmt, err := s.Prepare(query)
	//if err != nil {
	//	return nil, err
	//}
	//defer stmt.Close()

	return s.Query(query, args...)
}