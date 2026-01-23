package dbs_test

import (
	"testing"

	"github.com/smartwalle/dbs"
)

func TestUpdateBuilder(t *testing.T) {
	var ub = dbs.NewUpdateBuilder()
	ub.Table("user")
	ub.Set("type", 1)
	ub.Set("name", "Sample")
	ub.Set("status", 2)
	ub.Set("age", 3)
	ub.Where("id = ?", 10)
	t.Log(ub.SQL())
}

func BenchmarkUpdateBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ub = dbs.NewUpdateBuilder()
		ub.Table("user")
		ub.Set("type", 1)
		ub.Set("name", "Sample")
		ub.Set("status", 2)
		ub.Set("age", 3)
		ub.Where("id = ?", 10)
		_, _, _ = ub.SQL()
	}
}
