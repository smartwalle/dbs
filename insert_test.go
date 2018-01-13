package dbs

import (
	"testing"
	"fmt"
)

func TestInsertBuilder_ToSQL(t *testing.T) {
	var ib = NewInsertBuilder()
	ib.Table("user")
	ib.Columns("name", "email", "amount")
	ib.Values("Yang", "y@qq.com", 10)
	ib.Values("Feng", "f@qq.com", 20)
	ib.Suffix("ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email), amount=VALUES(amount)")
	fmt.Println(ib.ToSQL())
}
