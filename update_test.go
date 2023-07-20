package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestUpdateBuilder(t *testing.T) {
	var ub = dbs.NewUpdateBuilder()
	ub.Table("user")
	ub.SET("username", "un")
	ub.SET("email", "test@qq.com")
	ub.SET("amount", dbs.SQL("amount+?", 1))
	ub.Where("id=?", 10)
	ub.Limit(1)

	check(t, ub, "UPDATE user SET username=?, email=?, amount=amount+? WHERE id=? LIMIT ?", []interface{}{"un", "test@qq.com", 1, 10, int64(1)})
}

func TestUpdate(t *testing.T) {
	check(
		t,
		dbs.Update("table_name").SETS("c1", "v1", "c2", 1000).Where("c2 = ?", 10),
		"UPDATE table_name SET c1=?, c2=? WHERE c2 = ?",
		[]interface{}{"v1", 1000, 10},
	)
}

func BenchmarkUpdateBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ub = dbs.NewUpdateBuilder()
		ub.Table("user")
		ub.SET("id", 4)
		ub.SET("name", "Sample")
		ub.SET("status", 2)
		ub.SET("age", 20)

		ub.Where("id = ?", 10)
		_, _, _ = ub.SQL()
	}
}
