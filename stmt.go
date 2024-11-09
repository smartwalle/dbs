package dbs

import "database/sql"

type Stmt struct {
	stmt *sql.Stmt
	done chan struct{}
	err  error
}
