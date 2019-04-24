package dbs

import (
	"fmt"
	"testing"
)

var ibUser = Insert("name", "first_name", "last_name").Table("user")

func TestInsertBuilder_Clone(t *testing.T) {
	var ib = ibUser.Clone()
	ib.Values("n1", "f1", "l1")
	fmt.Println(ib.ToSQL())

	ib = ibUser.Clone()
	ib.Values("n2", "f2", "l2")
	fmt.Println(ib.ToSQL())
}

func TestInsertBuilder(t *testing.T) {
	fmt.Println("===== InsertBuilder =====")
	var ib = NewInsertBuilder()
	ib.Table("user")
	ib.SET("name", "yang")
	ib.SET("email", "yang@qq.com")
	ib.SET("amount", SQL("((SELECT amount FROM user_amount WHERE id=? LIMIT 1 AS amount)+?)", 10))
	//ib.Suffix("ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email)")
	ib.Suffix(OnDuplicateKeyUpdate().Append("name=VALUES(name)").Append(SQL("email=VALUES(email)")))
	fmt.Println(ib.ToSQL())
}

func TestInsertBuilder2(t *testing.T) {
	fmt.Println("===== TestInsertBuilder2 =====")
	var ib = NewInsertBuilder()
	ib.Table("b")
	ib.Columns("f3", "f4")

	var sb = NewSelectBuilder()
	sb.Selects("f1")
	sb.Select("?", 10)
	sb.From("a")
	ib.Select(sb)
	fmt.Println(ib.ToSQL())
}
