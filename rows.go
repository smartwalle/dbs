package dbs

import (
	"database/sql"
)

var mapper = NewMapper(kTag)

func Scan(rows *sql.Rows, dst interface{}) (err error) {
	return mapper.Bind(rows, dst)
}
