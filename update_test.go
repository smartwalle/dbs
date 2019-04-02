package dbs

import (
	"fmt"
	"testing"
)

func TestUpdateBuilder(t *testing.T) {
	fmt.Println("===== UpdateBuilder =====")
	var ub = NewUpdateBuilder()
	ub.Table("user")
	ub.SET("username", "un")
	ub.SET("email", "test@qq.com")
	ub.SET("amount", SQL("amount+?", 1))
	ub.Where("id=?", 10)
	ub.Limit(1)
	fmt.Println(ub.ToSQL())
}

func TestUpdate(t *testing.T) {
	fmt.Println("===== Update =====")
	var v2 = 1000
	fmt.Println(Update("table_name").SETS("c1", "v1", "c2", v2).Where("c2 = ?", 10).ToSQL())
}
