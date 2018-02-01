package dbs

import (
	"fmt"
	"testing"
)

func TestInsertBuilder(t *testing.T) {
	fmt.Println("===== InsertBuilder =====")
	var ib = NewInsertBuilder()
	ib.Table("user")
	ib.SET("name", "yang")
	ib.SET("email", "yang@qq.com")
	ib.SET("amount", SQL("((SELECT `amount` FROM `user_amount` WHERE id=? LIMIT 1 AS amount)+?)", 10))
	ib.Suffix("ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email)")
	fmt.Println(ib.ToSQL())
}
