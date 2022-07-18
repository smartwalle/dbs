package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestBuilder(t *testing.T) {
	var b = dbs.NewBuilder("")
	b.Append("SELECT a.id, a.name")
	b.Format("FROM %s AS a", "add")
	b.Append("WHERE id>?", 10)
	b.Append("LIMIT ?").Params(20)

	check(t, b, "SELECT a.id, a.name FROM add AS a WHERE id>? LIMIT ?", []interface{}{10, 20})
}
