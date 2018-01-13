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

	var o1 = OR(SQL("a=?", 20), SQL("b=?", 10))
	var o2 = OR(SQL("c=?", 20), SQL("b=?", 20))
	var o3 = OR(SQL("c=? AND d=?", 10, 10))
	var o4 = OR(IN("e", []int{101, 102, 103}))

	sb.Where(AND(o1, o2, o3, o4))

	fmt.Println(sb.ToSQL())
}
