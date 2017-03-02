package dba

import (
	"testing"
	"fmt"
)

func TestUpdateBuilder_ToSQL(t *testing.T) {
	var ub = NewUpdateBuilder()
	ub.Table("user")
	ub.SET("username", "test")
	ub.SET("email", "test@qq.com")
	ub.Where("id=?", 10)
	ub.Limit(1)
	fmt.Println(ub.ToSQL())
}