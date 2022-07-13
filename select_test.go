package dbs

import (
	"fmt"
	"testing"
)

func TestSelectBuilder_Clone(t *testing.T) {
	var sbUser = Select("u.name", "u.first_name", "u.last_name").From("user", "AS u").Where("a = ?", 10)

	var sb = sbUser.Clone()
	sb.Where("u.id = ?", 10)
	fmt.Println(sb.SQL())

	sb = sbUser.Clone()
	sb.Where("u.id = ?", 20)
	fmt.Println(sb.SQL())

	sb = sbUser.Clone()
	sb.Where("u.email = ?", "qqq@qq.com")
	fmt.Println(sb.SQL())
}

func TestSelectBuilder(t *testing.T) {
	fmt.Println("===== SelectBuilder =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.f2", "f2"))
	sb.Select(Alias(NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "amount"))
	sb.Select(Alias(Case("u.id").When("10", "20"), "uuid"))
	sb.From("user", "AS u")
	sb.LeftJoin("user_email", "AS ue ON ue.user_id=u.id")

	fmt.Println(sb.SQL())
}

func TestSelectBuilder2(t *testing.T) {
	fmt.Println("===== SelectBuilder2 =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.f2", "f2"))
	sb.Select(Alias(NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "amount"))
	sb.Select(Alias(Case("u.id").When("10", "20"), "uuid"))
	sb.From("user", "AS u")
	sb.LeftJoin("user_email", "AS ue ON ue.user_id=u.id")

	fmt.Println(sb.SQL())
}

func TestSelectBuilder3(t *testing.T) {
	fmt.Println("===== SelectBuilder3 =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.name", "name"))
	sb.From("user", "AS u")

	sb.Where("u.name=?", "yang")
	sb.Where(OR().Append("u.id=?", 10).Append("u.id=?", 20))

	fmt.Println(sb.SQL())
}

func TestSelectBuilderAnd(t *testing.T) {
	fmt.Println("===== SelectBuilderAnd =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.name", "name"))
	sb.From("user", "AS u")
	sb.Where("u.id=?", 100)
	sb.Where("u.status=?", 200)
	sb.Where(SQL("u.name=?", "test_name"))

	fmt.Println(sb.SQL())
}

func TestSelectBuilderAnd2(t *testing.T) {
	fmt.Println("===== SelectBuilderAnd2 =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.name", "name"))
	sb.From("user", "AS u")

	var a1 = AND()
	a1.Append("u.id=?", 100)
	a1.Appends(SQL("u.status=?", 200), SQL("u.name=?", "test_name"))
	sb.Where(a1)

	fmt.Println(sb.SQL())
}

func TestSelectBuilderOR(t *testing.T) {
	fmt.Println("===== SelectBuilderOR =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.name", "name"))
	sb.From("user", "AS u")

	var a1 = OR()
	a1.Append("u.id=?", 100)
	a1.Appends(SQL("u.status=?", 200), SQL("u.name=?", "test_name"))
	sb.Where(a1)

	fmt.Println(sb.SQL())
}

func TestSelectBuilderIN(t *testing.T) {
	fmt.Println("===== SelectBuilderIN =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.name", "name"))
	sb.From("user", "AS u")

	sb.Where("u.id=?", 100)
	sb.Where(IN("u.status", []int{200, 300, 400}))

	fmt.Println(sb.SQL())
}

func BenchmarkSelectBuilder(b *testing.B) {
	fmt.Println("===== BenchmarkSelectBuilder =====")
	for i := 0; i < b.N; i++ {
		var sb = NewSelectBuilder()
		sb.Selects("u.id")
		sb.Select(Alias("u.name", "name"))
		sb.From("user", "AS u")

		sb.Where("u.name=?", "yang")
		sb.Where(OR().Append("u.id=?", i).Append("u.id=?", 20))
		sb.SQL()
	}
}
