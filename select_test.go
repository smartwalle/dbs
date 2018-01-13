package dbs

import (
	"testing"
	"fmt"
)

func TestSelectBuilder_Select(t *testing.T) {
	var sb = NewSelectBuilder()
	sb.Options("SQL_CALC_FOUND_ROWS")
	sb.Selects("u.id", "u.name", "u.email")
	sb.Selects("w.work_name")
	sb.From("user", "AS u")
	sb.LeftJoin("work", "AS w ON w.id=u.work_id")
	sb.Where(SQL("u.id=?", 1))

	fmt.Println(sb.ToSQL())
}
