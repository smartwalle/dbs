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
