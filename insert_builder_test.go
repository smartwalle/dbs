package dba

import (
	"testing"
	"fmt"
)

func TestInsertBuilder_ToSQL(t *testing.T) {
	var ib = NewInsertBuilder()
	ib.Insert("test").Columns("a", "b", "c").Column("d").Values("11", "12", "13", "14")
	ib.Values("21", "22", "23", "24")

	fmt.Println(ib.ToSQL())
}
