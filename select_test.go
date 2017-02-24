package dba

import (
	"testing"
	"fmt"
)

func TestSelectBuilder_Select(t *testing.T) {
	var sb = NewSelectBuilder()
	sb.Selects("a", "b").Select("c").From("test").LeftJoin("db_name", "AS dn").Where("p.a=?", 1).OrderBy("haha DES", "test ASC")
	fmt.Println(sb.ToSQL())
}
