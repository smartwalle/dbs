package dba

import (
	"testing"
	"fmt"
)

func TestUpdateBuilder_ToSQL(t *testing.T) {
	var ub = NewUpdateBuilder()
	ub.Table("t")
	ub.SET("a", "a")
	ub.SET("b", "ddd")
	fmt.Println(ub.ToSQL())
}