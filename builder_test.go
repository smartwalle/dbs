package dbs

import (
	"fmt"
	"testing"
)

func TestBuilder(t *testing.T) {
	var b = NewBuilder()
	b.Append("SELECT a.id, a.name")
	b.Format("FROM %s AS a", "add")
	b.Append("WHERE id>?", []interface{}{10}...)
	b.Append("LIMIT ?").Params(20)
	fmt.Println(b.ToSQL())
}
