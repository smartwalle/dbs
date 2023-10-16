package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestInsertBuilder(t *testing.T) {
	var ib = dbs.NewInsertBuilder()
	ib.Table("user")
	ib.SET("name", "yang")
	ib.SET("email", "yang@qq.com")
	ib.SET("amount", dbs.SQL("((SELECT amount FROM user_amount WHERE id=? LIMIT 1 AS amount)+?)", 10))
	ib.Suffix(dbs.OnDuplicateKeyUpdate().Append("name=VALUES(name)").Append(dbs.SQL("email=VALUES(email)")))

	check(
		t,
		ib,
		"INSERT INTO user (name, email, amount) VALUES (?, ?, ((SELECT amount FROM user_amount WHERE id=? LIMIT 1 AS amount)+?)) ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email)",
		[]interface{}{"yang", "yang@qq.com", 10},
	)
}

func TestInsertBuilder2(t *testing.T) {
	var ib = dbs.NewInsertBuilder()
	ib.Table("b")
	ib.Columns("f3", "f4")

	var sb = dbs.NewSelectBuilder()
	sb.Selects("f1")
	sb.Select("?", 10)
	sb.From("a")
	ib.Select(sb)

	check(t, ib, "INSERT INTO b (f3, f4) (SELECT f1, ? FROM a)", []interface{}{10})
}

func BenchmarkInsertBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ib = dbs.NewInsertBuilder()
		ib.Table("user")
		ib.Columns("id", "name", "status", "age")
		ib.Values(4, "Sample", 2, 10)
		_, _, _ = ib.SQL()
	}
}
