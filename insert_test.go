package dbs_test

import (
	"testing"

	"github.com/smartwalle/dbs"
)

func TestInsertBuilder(t *testing.T) {
	var ib = dbs.NewInsertBuilder()
	ib.Table("user")
	ib.Columns("id", "name", "status", "age")
	ib.Values(1, "Sample", 2, 10)
	ib.Values(2, "Sample", 2, 10)
	ib.Values(3, "Sample", 2, 10)
	ib.Values(4, "Sample", 2, 10)
	t.Log(ib.SQL())
}

func BenchmarkInsertBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ib = dbs.NewInsertBuilder()
		ib.Table("user")
		ib.Columns("id", "name", "status", "age")
		ib.Values(1, "Sample", 2, 10)
		ib.Values(2, "Sample", 2, 10)
		ib.Values(3, "Sample", 2, 10)
		ib.Values(4, "Sample", 2, 10)
		_, _, _ = ib.SQL()
	}
}
