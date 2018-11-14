package dbs

import (
	"fmt"
	"testing"
)

func TestDollar(t *testing.T) {
	fmt.Println("===== PostgreSQL =====")
	Dialect = PostgreSQL

	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.From("user", "AS u")
	sb.Where("u.id = ? OR u.id = ? OR u.id = ?")

	var s, _, _ = sb.ToSQL()
	fmt.Println(s)

	Dialect = MySQL
}
