package dbs

import (
	"fmt"
	"testing"
)

func TestSelectBuilder(t *testing.T) {
	fmt.Println("===== SelectBuilder =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.f2", "f2"))
	sb.Select(Alias(NewStatement("SELECT tt.id, tt.name FROM test_table AS tt WHERE tt.id=?", 100), "amount"))
	sb.Select(Alias(Case("u.id").When("10", "20"), "uuid"))
	sb.From("user", "AS u")
	sb.LeftJoin("user_email", "AS ue ON ue.user_id=u.id")

	fmt.Println(sb.ToSQL())
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

	fmt.Println(sb.ToSQL())
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

	fmt.Println(sb.ToSQL())
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

	fmt.Println(sb.ToSQL())
}

func TestSelectBuilder3(t *testing.T) {
	fmt.Println("===== SelectBuilder3 =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.name", "name"))
	sb.From("user", "AS u")

	sb.Where("u.id=", Alias(SQL("SELECT id FROM user_email WHERE email=?", "test@qq.com"), "user_id"))

	fmt.Println(sb.ToSQL())
}

func TestSelectBuilderIN(t *testing.T) {
	fmt.Println("===== SelectBuilderIN =====")
	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.Select(Alias("u.name", "name"))
	sb.From("user", "AS u")

	sb.Where("u.id=?", 100)
	sb.Where(IN("u.status", []int{200, 300, 400}))

	fmt.Println(sb.ToSQL())
}
