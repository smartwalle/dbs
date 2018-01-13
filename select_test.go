package dbs

import (
	"testing"
	"fmt"
)

func TestSelectBuilder_Select(t *testing.T) {
	var sb = NewSelectBuilder()
	sb.Selects("id", "name")
	sb.Select("email")
	sb.From("user")

	var ws = AND()
	ws.Append(OR(C("id=?", 10), C("email=?", "qq@qq.com")))
	ws.Append(OR(C("id=?", 11), C("email=?", "qq@qq.com")))
	sb.WhereClause(ws)

	fmt.Println(sb.ToSQL())
	fmt.Println(sb.CountSQL())
}
