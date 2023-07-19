package dbs_test

import (
	"github.com/smartwalle/dbs"
	"testing"
)

func TestSelectBuilder_Clone(t *testing.T) {
	var sbUser = dbs.Select("u.name", "u.first_name", "u.last_name").From("user", "AS u").Where("a = ?", 10)

	check(t, sbUser.Clone().Where("u.id = ?", 10), "SELECT u.name, u.first_name, u.last_name FROM `user` AS u WHERE a = ? AND u.id = ?", []interface{}{10, 10})
	check(t, sbUser.Clone().Where("u.id = ?", 20), "SELECT u.name, u.first_name, u.last_name FROM `user` AS u WHERE a = ? AND u.id = ?", []interface{}{10, 20})
	check(t, sbUser.Clone().Where("u.email = ?", "qqq@qq.com"), "SELECT u.name, u.first_name, u.last_name FROM `user` AS u WHERE a = ? AND u.email = ?", []interface{}{10, "qqq@qq.com"})
}

func TestSelectBuilder(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(dbs.Alias("u.f2", "f2"))
	sb.Select(dbs.Alias(dbs.NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "amount"))
	sb.Select(dbs.Alias(dbs.Case("u.id").When("10", "20"), "uuid"))
	sb.From("user", "AS u")
	sb.LeftJoin("user_email", "AS ue ON ue.user_id=u.id")

	check(
		t,
		sb,
		"SELECT u.id, u.f2 AS f2, (SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?) AS amount, (CASE u.id WHEN 10 THEN 20 END) AS uuid FROM `user` AS u LEFT JOIN `user_email` AS ue ON ue.user_id=u.id",
		[]interface{}{100},
	)
}

func TestSelectBuilder2(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(dbs.Alias("u.f2", "f2"))
	sb.Select(dbs.Alias(dbs.NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "amount"))
	sb.Select(dbs.Alias(dbs.Case("u.id").When("10", "20"), "uuid"))
	sb.From("user", "AS u")
	sb.LeftJoin("user_email", "AS ue ON ue.user_id=u.id")

	check(
		t,
		sb,
		"SELECT u.id, u.f2 AS f2, (SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?) AS amount, (CASE u.id WHEN 10 THEN 20 END) AS uuid FROM `user` AS u LEFT JOIN `user_email` AS ue ON ue.user_id=u.id",
		[]interface{}{100},
	)
}

func TestSelectBuilder3(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(dbs.Alias("u.name", "name"))
	sb.From("user", "AS u")

	sb.Where("u.name=?", "yang")
	sb.Where(dbs.OR().Append("u.id=?", 10).Append("u.id=?", 20))

	check(t, sb, "SELECT u.id, u.name AS name FROM `user` AS u WHERE u.name=? AND (u.id=? OR u.id=?)", []interface{}{"yang", 10, 20})
}

func TestSelectBuilderAnd(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(dbs.Alias("u.name", "name"))
	sb.From("user", "AS u")
	sb.Where("u.id=?", 100)
	sb.Where("u.status=?", 200)
	sb.Where(dbs.SQL("u.name=?", "test_name"))

	check(t, sb, "SELECT u.id, u.name AS name FROM `user` AS u WHERE u.id=? AND u.status=? AND u.name=?", []interface{}{100, 200, "test_name"})
}

func TestSelectBuilderAnd2(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(dbs.Alias("u.name", "name"))
	sb.From("user", "AS u")

	var a1 = dbs.AND()
	a1.Append("u.id=?", 100)
	a1.Appends(dbs.SQL("u.status=?", 200), dbs.SQL("u.name=?", "test_name"))
	sb.Where(a1)

	check(t, sb, "SELECT u.id, u.name AS name FROM `user` AS u WHERE (u.id=? AND u.status=? AND u.name=?)", []interface{}{100, 200, "test_name"})
}

func TestSelectBuilderOR(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(dbs.Alias("u.name", "name"))
	sb.From("user", "AS u")

	var a1 = dbs.OR()
	a1.Append("u.id=?", 100)
	a1.Appends(dbs.SQL("u.status=?", 200), dbs.SQL("u.name=?", "test_name"))
	sb.Where(a1)

	check(t, sb, "SELECT u.id, u.name AS name FROM `user` AS u WHERE (u.id=? OR u.status=? OR u.name=?)", []interface{}{100, 200, "test_name"})
}

func TestSelectBuilderIN(t *testing.T) {
	var sb = dbs.NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(dbs.Alias("u.name", "name"))
	sb.From("user", "AS u")

	sb.Where("u.id=?", 100)
	sb.Where(dbs.IN("u.status", []int{200, 300, 400}))

	check(t, sb, "SELECT u.id, u.name AS name FROM `user` AS u WHERE u.id=? AND u.status IN (?, ?, ?)", []interface{}{100, 200, 300, 400})
}

func BenchmarkSelectBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var sb = dbs.NewSelectBuilder()
		sb.Selects("id", "user_id", "pay_no", "trade_no", "goods_id", "goods_name", "goods_price", "goods_cnt", "sku_id", "original_spec", "real_spec", "status", "created_at")
		sb.From("user")

		sb.Where("id = ?", "123")
		sb.Where("status = ?", 1)
		_, _, _ = sb.SQL()
	}
}
