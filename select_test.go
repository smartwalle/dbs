package dbs

import (
	"testing"
	"fmt"
)

func TestSelectBuilder_Select(t *testing.T) {
	var sb = NewSelectBuilder()
	sb.Selects("id", "name").Select("email").From("user")
	fmt.Println(sb.ToSQL())
	fmt.Println(sb.CountSQL())
}
