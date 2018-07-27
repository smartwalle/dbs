package dbs

import (
	"fmt"
	"testing"
)

func TestCountBuilder(t *testing.T) {
	fmt.Println("===== CountBuilder =====")

	var sb = NewSelectBuilder()
	sb.Selects("u.id")
	sb.From("user", "AS u")
	sb.Where("u.id > 10")

	var cb = NewCountBuilder(sb)
	fmt.Println(cb.ToSQL())
}
