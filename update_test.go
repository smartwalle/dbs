package dbs

import (
	"testing"
	"fmt"
)

func TestUpdateBuilder_ToSQL(t *testing.T) {
	var ub = NewUpdateBuilder()
	ub.SET("name", "Yang")
	ub.SET("email", "email@qq.com")
	ub.SET("amount", SQL("amount+?", 100))
	ub.Table("user")
	ub.Where(SQL("id=?", 10))
	ub.Limit(1)
	fmt.Println(ub.ToSQL())
}