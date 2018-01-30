package dbs

import (
	"testing"
	"fmt"
)

func TestUpdateBuilder_ToSQL(t *testing.T) {
	var ub = NewUpdateBuilder()
	ub.Table("user")
	ub.SET("username", "test")
	ub.SET("email", "test@qq.com")
	ub.SET("amount", SQL("amount+?", 1))
	ub.Where("id=?", 10)
	ub.Limit(1)
	fmt.Println(ub.ToSQL())
}
