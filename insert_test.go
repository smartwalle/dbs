package dba

import (
	"testing"
	"fmt"
)

func TestInsertBuilder_ToSQL(t *testing.T) {
	var ib = NewInsertBuilder()
	ib.Table("author").Columns("text", "name").Values(1, SQL("((SELECT `name` FROM `class` WHERE id=? LIMIT 1)+10)"))

	fmt.Println(ib.ToSQL())
}
