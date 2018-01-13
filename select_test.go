package dbs

import (
	"testing"
	"fmt"
)

func TestSelectBuilder_Select(t *testing.T) {
	var sb = NewSelectBuilder()
	sb.Selects("u.id", "u.name", "u.email")
	sb.Selects("w.work_name")
	sb.From("user", "AS u")
	sb.LeftJoin("work", "AS w ON w.id=u.work_id")
	sb.Where(SQL("u.id > ?", 1))
	sb.OrderBy("u.id DESC")
	sb.Limit(1)
	sb.Offset(10)
	fmt.Println(sb.ToSQL())


	var sb2 = NewSelectBuilder()
	sb2.Selects("u.id", "u.name")
	sb2.From("user", "AS u")
	sb2.Where(AND())
	fmt.Println(sb2.ToSQL())
}
