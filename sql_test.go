package dbs

import (
	_ "github.com/go-sql-driver/mysql"
)

var pool *Pool

func getPool() *Pool {
	if pool == nil {
		pool, _ = NewSQL("mysql", "localhost", 30, 10)
	}
	return pool
}

func getSession() *Session {
	var s = getPool().GetSession()
	return s
}
