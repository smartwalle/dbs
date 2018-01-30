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
