package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestPostgresQL(t *testing.T) {
	dbs.UseDialect(dbs.Dollar)

	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.From("user", "AS u")
	sb.Where("u.id = ? OR u.id = ? OR u.id = ?", 10, 20, 30)

	check(t, sb, "SELECT u.id FROM user AS u WHERE u.id = $1 OR u.id = $2 OR u.id = $3", []interface{}{10, 20, 30})

	dbs.UseDialect(dbs.Question)
}
