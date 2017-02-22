package dba

import (
	"testing"
	"fmt"
)

func TestSelectBuilder_Select(t *testing.T) {
	var sb = NewSelectBuilder()
	sb.Selects("a", "b").Select("c").From("test", "AS t").LeftJoin("db_name", "AS dn").Where("p.a=?", 1).OrderBy("? DES", "haha")
	fmt.Println(sb.ToSQL())
}
