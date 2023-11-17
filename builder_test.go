package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestBuilder(t *testing.T) {
	var table = "test_table"
	var b = dbs.NewBuilder("")
	b.Append("SELECT a.id, a.name")
	b.Appends("FROM", table, "AS a")
	b.Append("WHERE id>?", 10)
	b.Append("LIMIT ?").Params(20)

	check(t, b, "SELECT a.id, a.name FROM test_table AS a WHERE id>? LIMIT ?", []interface{}{10, 20})
}
