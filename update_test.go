package dbs

import (
	"fmt"
	"testing"
)

func TestUpdateBuilder(t *testing.T) {
	fmt.Println("===== UpdateBuilder =====")
	var ub = NewUpdateBuilder()
	ub.Table("user")
	ub.SET("username")
	ub.SET("email", "test@qq.com")
	ub.SET("amount", SQL("amount+?", 1))
	ub.Where("id=?", 10)
	ub.Limit(1)
	fmt.Println(ub.ToSQL())
}
