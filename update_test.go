package dba

import (
	"testing"
	"fmt"
)

func TestUpdateBuilder_ToSQL(t *testing.T) {
	var ub = NewUpdateBuilder()
	ub.Table("t", "AS tt")
	ub.Table("t2", "AS t2")
	ub.SET("a", "a")
	ub.SET("b", "ddd")
	fmt.Println(ub.ToSQL())
}