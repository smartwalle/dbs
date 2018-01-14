package dbs

import (
	"testing"
	"fmt"
)

func TestInsertBuilder_ToSQL(t *testing.T) {
	var ib = NewInsertBuilder()
	ib.Table("user")
	ib.SET("name", "yang")
	ib.SET("email", "yang@qq.com")
	ib.SET("amount", SQL("((SELECT `amount` FROM `class` WHERE id=? LIMIT 1)+?)", 10))
	fmt.Println(ib.ToSQL())
}