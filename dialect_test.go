package dbs

import (
	"fmt"
	"testing"
)

func TestPostgreSQL(t *testing.T) {
	fmt.Println("===== DialectPostgreSQL =====")
	gDialect = DialectPostgreSQL

	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.From("user", "AS u")
	sb.Where("u.id = ? OR u.id = ? OR u.id = ?")

	var s, _, _ = sb.SQL()
	fmt.Println(s)

	gDialect = DialectMySQL
}
